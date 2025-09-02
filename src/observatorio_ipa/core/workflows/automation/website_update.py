import jwt
import json
import requests
import sqlite3
import logging
from datetime import datetime, timezone
from pathlib import Path
from git import Repo, GitCommandError
from github import Github, PullRequest
from google.cloud import storage

from observatorio_ipa.utils import db
from observatorio_ipa.core.config import Settings, AutoWebsiteSettings, LOGGER_NAME

logger = logging.getLogger(LOGGER_NAME)


def get_jwt(app_id: str, private_key_path: str) -> str:
    """
    Generate a JWT for GitHub App authentication.

    Args:
        app_id (str): GitHub App's client ID.
        private_key_path (str): Path to the GitHub App's private key file.

    Returns:
        str: The generated JWT.

    """
    with open(private_key_path, "rb") as key_file:
        # signing_key = serialization.load_pem_private_key(
        #     key_file.read(),
        #     password=None,
        # )
        signing_key = key_file.read()
    now = int(datetime.now(timezone.utc).timestamp())
    payload = {
        "iat": now - 60,  # issued at time
        "exp": now + (10 * 60),  # JWT expiration time
        "iss": app_id,  # Github App's client ID
    }

    return jwt.encode(payload, signing_key, algorithm="RS256")


def get_installation_token(app_id: str, private_key_path: str, repo_url: str) -> str:
    """
    Get a GitHub App installation access token for the given repository.

    Args:
        app_id (str): GitHub App's client ID.
        private_key_path (str): Path to the GitHub App's private key file.
        repo_url (str): HTTPS URL of the repository.

    Returns:
        str: The installation access token.
    """
    jwt_token = get_jwt(app_id, private_key_path)
    headers = {
        "Authorization": f"Bearer {jwt_token}",
        "Accept": "application/vnd.github+json",
    }
    # Get installation ID for the repo
    repo_full_name = repo_url.rstrip(".git").split("github.com/")[-1]
    url = f"https://api.github.com/repos/{repo_full_name}/installation"
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    installation_id = response.json()["id"]

    # Create installation access token
    url = f"https://api.github.com/app/installations/{installation_id}/access_tokens"
    response = requests.post(url, headers=headers)
    response.raise_for_status()
    return response.json()["token"]


def _make_url_with_token(repo_url: str, github_token: str) -> str:
    """
    Create a URL with the GitHub token for authentication.

    Args:
        repo_url (str): The repository URL.
        github_token (str): The GitHub token.

    Returns:
        str: The URL with the token.
    """
    return repo_url.replace("https://", f"https://x-access-token:{github_token}@")


def _ensure_git_repo(
    local_repo_path: str | Path, repo_url: str, github_token: str, branch: str
) -> Repo:
    """
    Clone the repository if not present locally, else pull latest changes.

    Uses the specified branch. Creates it if the branch does not exist.

    Args:
        local_repo_path (str): Local path for the git repository.
        repo_url (str): HTTPS URL of the remote repository.
        github_token (str): GitHub personal access token.
        branch (str): Branch to checkout or create.

    Returns:
        Repo: GitPython Repo object for the local repository.
    """
    url_with_token = _make_url_with_token(repo_url, github_token)
    repo_name = Path(repo_url).stem
    local_repo_path_ = Path(local_repo_path).expanduser().resolve()
    full_local_repo_path = local_repo_path_ / repo_name

    if not full_local_repo_path.exists():
        print(f"Cloning repository to {full_local_repo_path.as_posix()}...")
        repo = Repo.clone_from(url_with_token, full_local_repo_path.as_posix())
        origin = repo.remotes.origin
        origin.fetch()
        # Check if branch exists on remote
        remote_branches = [ref.name.split("/")[-1] for ref in origin.refs]
        if branch in remote_branches:
            print(
                f"Branch '{branch}' exists on remote. Checking out and updating from main..."
            )
            repo.git.checkout(branch)
            repo.git.pull("origin", branch)
            repo.git.pull("origin", "main")
        else:
            print(f"Branch '{branch}' does not exist. Creating from main...")
            repo.git.checkout("origin/main")
            repo.git.checkout("-b", branch)
        # Ensure branch matches main
        repo.git.merge("origin/main")
    else:
        print(
            f"Repository exists at {local_repo_path_.as_posix()}, pulling latest changes..."
        )
        repo = Repo(full_local_repo_path)
        origin = repo.remotes.origin
        origin.fetch()
        # Always checkout or create the branch from main if needed
        try:
            repo.git.checkout(branch)
        except GitCommandError:
            print(f"Branch '{branch}' does not exist locally. Creating from main...")
            repo.git.checkout("origin/main")
            repo.git.checkout("-b", branch)
        # Pull latest changes from remote branch (if exists) and from main
        remote_branches = [ref.name.split("/")[-1] for ref in origin.refs]
        if branch in remote_branches:
            repo.git.pull("origin", branch)
        repo.git.pull("origin", "main")
        # Ensure branch matches main
        repo.git.merge("origin/main")
    return repo


def _download_gcs_file(
    bucket: storage.Bucket, gcs_path: str | Path, local_path: str | Path
) -> None:
    """
    Download a file from Google Cloud Storage to a local path.

    Args:
        bucket (storage.Bucket): GCS bucket object.
        gcs_path (str): Path to the file in GCS (relative to bucket root).
        local_path (str): Local filesystem path to save the file.
    """

    blob = bucket.blob(Path(gcs_path).as_posix())
    local_path = Path(local_path).expanduser().resolve()
    local_path.parent.mkdir(parents=True, exist_ok=True)
    blob.download_to_filename(str(local_path))
    print(f"Downloaded {gcs_path} to {local_path}")


def _replace_files_from_gcs(
    files_to_replace: list[str] | list[Path],
    gcs_base_assets_path: str | Path,
    bucket_name: str,
    service_account_key: str | Path,
    local_repo_path: str | Path,
    repo_base_assets_path: str | Path,
) -> None:
    """
    Download and overwrite files in the local git repository from GCS.

    Args:
        files_to_replace (list[str] | list[Path]): List of relative file paths to replace.
        gcs_base_path (str | Path): Base path in GCS bucket.
        bucket_name (str): Name of the GCS bucket.
        service_account_key (str | Path): Path to the GCP service account JSON key.
        local_repo_path (str | Path): Local path to the git repository.
        repo_base_path (str | Path): Base path in the git repo to copy files into.
    """
    gcs_base_assets_path = Path(gcs_base_assets_path)
    local_repo_path = Path(local_repo_path)
    repo_base_assets_path = Path(repo_base_assets_path)

    # Create Cloud Storage Client and Bucket
    with open(service_account_key) as f:
        service_account_info = json.load(f)
    storage_client = storage.Client.from_service_account_info(service_account_info)
    bucket = storage_client.bucket(bucket_name)

    for rel_path in files_to_replace:
        try:
            gcs_file_path = Path(rel_path)
            stripped_gcs_file_path = str(gcs_file_path.as_posix()).replace(
                gcs_base_assets_path.as_posix() + "/", ""
            )
            local_file_path = (
                local_repo_path / repo_base_assets_path / stripped_gcs_file_path
            )
            _download_gcs_file(bucket, gcs_file_path, local_file_path)
        except Exception as e:
            print(f"Error downloading {gcs_file_path}: {e}")
    print("All files replaced from GCS.")


def _commit_and_push(
    repo: Repo, branch: str, repo_url: str, github_token: str, job_id: str
) -> str | None:
    """
    Commit and push changes to the remote branch.

    Args:
        repo (Repo): GitPython Repo object.
        branch (str): Branch to push to.
        repo_url (str): HTTPS URL of the remote repository including token.
        github_token (str): GitHub personal access token.
        job_id (str): ID of the job triggering the commit.

    Returns:
        str | None: The commit hexsha if changes were committed, else None.

    """
    repo.git.add(A=True)
    commit_msg = f"""Update stats files from GCS ({datetime.now().strftime('%Y-%m-%d %H:%M')})\n\n
        Job ID: {job_id}"""
    if repo.is_dirty():
        commit = repo.index.commit(commit_msg)
        print("Committed changes.")
        url_with_token = _make_url_with_token(repo_url, github_token)
        repo.git.push(url_with_token, branch)
        print(f"Pushed changes to branch {branch}.")
        # Return the commit id
        return commit.hexsha
    else:
        print("No changes to commit.")
        return None


def _create_pull_request(
    job_id: str,
    repo_url: str,
    github_token: str,
    branch: str,
    base_branch: str = "main",
) -> PullRequest.PullRequest:
    """
    Create a pull request from the branch to the base branch using PyGithub.

    Args:
        job_id (str): ID of the job triggering the PR.
        repo_url (str): HTTPS URL of the remote repository.
        github_token (str): GitHub personal access token.
        branch (str): Source branch for the PR.
        base_branch (str): Target branch for the PR (default: "main").

    Returns:
        PullRequest.PullRequest: The created pull request object.

    """

    repo_full_name = repo_url.rstrip(".git").split("github.com/")[-1]
    github_repo = Github(github_token)
    repo = github_repo.get_repo(repo_full_name)
    title = f"Automated stats update {datetime.now().strftime('%Y-%m-%d %H:%M')}"
    body = f"Automated update (replacement) of stats files from GCS. Job ID: {job_id}"
    try:
        pr = repo.create_pull(title=title, body=body, head=branch, base=base_branch)
        print(f"Pull request created: {pr.html_url}")
        return pr
    except Exception as e:
        if "A pull request already exists" in str(e):
            print("A pull request already exists.")
        else:
            print(f"Failed to create pull request: {e}")
        raise e


def website_update(
    files_to_replace: list[str] | list[Path],
    website_settings: AutoWebsiteSettings,
    storage_bucket_name: str,
    google_credentials_file: str | Path,
    job_id: str,
) -> PullRequest.PullRequest | None:
    """Updates a git repository with files from Google Cloud Storage and creates a pull request.

    This function is intended to auto update the dependency files (assets) for a website stored in GitHub.
    This function requires a Settings object of type core.config.AutoWebsiteSettings.

    Args:
        files_to_replace (list[str] | list[Path]): List of file paths to replace.
        website_settings (AutoWebsiteSettings): Settings object for the website.
        storage_bucket_name (str): Name of the Google Cloud Storage bucket.
        google_credentials_file (str | Path): Path to the Google Cloud service account key file.
        job_id (str): ID of the job triggering the update.

    Returns:
        PullRequest.PullRequest | None: The created pull request or None if no changes were made.

    """
    # Create connection to GitHub
    logger.debug("Generating GitHub App installation token")
    github_settings = website_settings.github
    github_token = get_installation_token(
        app_id=github_settings.app_id,
        private_key_path=github_settings.private_key_path,
        repo_url=github_settings.repo_url,
    )

    logger.debug("Starting GCS to GitHub file replacement process")
    # Clone or use Git Repo
    repo = _ensure_git_repo(
        local_repo_path=website_settings.local_repo_path,
        repo_url=github_settings.repo_url,
        github_token=github_token,
        branch=website_settings.work_branch,
    )

    # Replace Stats files in local Repo
    logger.debug("Replacing files from GCS")
    _replace_files_from_gcs(
        files_to_replace=files_to_replace,
        gcs_base_assets_path=website_settings.gcs_base_assets_path,
        bucket_name=storage_bucket_name,
        service_account_key=google_credentials_file,
        local_repo_path=Path(repo.working_dir),
        repo_base_assets_path=website_settings.repo_base_assets_path,
    )

    # Commit and push changes to Working Branch (Should not be main)
    logger.debug("Committing and pushing changes to GitHub")
    commit_hexsha = _commit_and_push(
        repo=repo,
        branch=website_settings.work_branch,
        repo_url=website_settings.github.repo_url,
        github_token=github_token,
        job_id=job_id,
    )
    if not commit_hexsha:
        logger.info("No changes to push, skipping pull request creation.")
        print("No changes to push, skipping pull request creation.")
        return None

    # Create pull request. Website update will not complete until someone approves PR
    logger.debug("Creating pull request")
    pr = _create_pull_request(
        job_id=job_id,
        repo_url=github_settings.repo_url,
        github_token=github_token,
        branch=website_settings.work_branch,
        base_branch=website_settings.main_branch,
    )
    logger.info(f"Pull request created: {pr.html_url}")
    print(f"Pull request created: {pr.html_url}")
    return pr


def auto_website_update(
    conn: sqlite3.Connection, job_id: str, settings: Settings
) -> None:
    """
    Main process to replace files in a git repository with files from Google Cloud Storage and create a pull request.

    This function requires a Settings object of type core.config.Settings.

    Args:
        conn: sqlite3.Connection: SQLite database connection.
        job_id (str): Job ID to filter files in the database.
        settings (Settings): Configuration settings object.
    """

    logger.debug("Starting Website Update process...")
    # Check if Job is ready for Website Update
    job = conn.execute("""SELECT * FROM jobs WHERE id = ? """, (job_id,)).fetchone()
    if not job:
        logger.warning("Job not found.")
        print("Job not found.")
        return

    if job["job_status"] != "RUNNING":
        logger.debug("Job already Finished. Exiting.")
        print("Job already Finished. Exiting.")
        return

    if job["stats_export_status"] not in ("COMPLETED", "FAILED"):
        logger.debug("Stats export not completed. Exiting.")
        print("Stats export not completed. Exiting.")
        return

    # verify no running stats exports
    running_stats_exports = conn.execute(
        """SELECT id 
            FROM exports 
            WHERE job_id=? AND type='table' AND state='RUNNING'""",
        (job["id"],),
    ).fetchall()
    if running_stats_exports:
        # Still running, continue to next Job status update
        logger.debug("Stats exports still running. Exiting.")
        print("Stats exports still running. Exiting.")
        return

    # Create Website Update Task if first run
    # Get or Create Website Update Task
    db_website = conn.execute(
        "SELECT * FROM website_updates WHERE job_id = ? ",
        (job_id,),
    ).fetchone()

    if not db_website:
        iso_now = db.datetime_to_iso(db.utc_now())
        conn.execute(
            """
            INSERT INTO website_updates (job_id, status, created_at, updated_at)
            VALUES (?, ?, ?, ?)
            """,
            (job_id, "PENDING", iso_now, iso_now),
        )

        db_website = conn.execute(
            "SELECT * FROM website_updates WHERE job_id = ? ",
            (job_id,),
        ).fetchone()

    # Get list of files to replace from DB
    db_stats_tasks = conn.execute(
        """
        SELECT * 
        FROM exports 
        WHERE job_id = ? 
            AND type='table'
            AND state = 'COMPLETED'
        """,
        (job_id,),
    ).fetchall()

    files_to_replace = [Path(row["path"], row["name"]) for row in db_stats_tasks]
    if not files_to_replace:
        # No Files to replace, finish Website Update Task
        conn.execute(
            """UPDATE website_updates 
            SET status = 'COMPLETED', updated_at = ? 
            WHERE job_id = ? """,
            (db.datetime_to_iso(db.utc_now()), job_id),
        )
        logger.info("No files to replace. Exiting.")
        print("No files to replace. Exiting.")
        return

    try:
        pull_request = website_update(
            files_to_replace=files_to_replace,
            website_settings=settings.app.automation.website,
            storage_bucket_name=settings.app.stats_export.storage_bucket,  # type: ignore
            google_credentials_file=settings.app.google.credentials_file,
            job_id=job_id,
        )

    except Exception as e:
        logger.error(f"Website update process Failed: {e}")
        print(f"Website update process Failed: {e}")
        conn.execute(
            """
            UPDATE website_updates
            SET attempts = attempts + 1, last_error = ?, 
            updated_at = ? WHERE job_id = ? """,
            (str(e), db.datetime_to_iso(db.utc_now()), job_id),
        )
        return

    # Update Website Update Task as Completed
    conn.execute(
        """
        UPDATE website_updates
        SET status = 'COMPLETED', 
            pull_request_id = ?, 
            pull_request_url = ? ,
        updated_at = ? WHERE job_id = ? """,
        (
            str(pull_request.id) if pull_request else None,
            pull_request.html_url if pull_request else None,
            db.datetime_to_iso(db.utc_now()),
            job_id,
        ),
    )

    return


if __name__ == "__main__":
    # Example usage (replace with your actual values or call main from another script)
    pass
