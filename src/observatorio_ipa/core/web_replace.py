import os
import sys
import shutil
from datetime import datetime
from pathlib import Path
from git import Repo, GitCommandError
from github import Github
from google.cloud import storage


def download_gcs_file(bucket_name, gcs_path, local_path, service_account_key):
    """
    Download a file from Google Cloud Storage to a local path.

    Args:
        bucket_name (str): Name of the GCS bucket.
        gcs_path (str): Path to the file in GCS (relative to bucket root).
        local_path (str): Local filesystem path to save the file.
        service_account_key (str): Path to the GCP service account JSON key.
    """
    storage_client = storage.Client.from_service_account_json(service_account_key)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(gcs_path)
    local_path = Path(local_path)
    local_path.parent.mkdir(parents=True, exist_ok=True)
    blob.download_to_filename(str(local_path))
    print(f"Downloaded {gcs_path} to {local_path}")


def ensure_git_repo(local_repo_path: str | Path, repo_url: str, branch: str) -> Repo:
    """
    Clone the repository if not present locally, else pull latest changes.

    Args:
        local_repo_path (str): Local path for the git repository.
        repo_url (str): HTTPS URL of the remote repository.
        github_token (str): GitHub personal access token.
        branch (str): Branch to checkout or create.

    Returns:
        Repo: GitPython Repo object for the local repository.
    """
    local_repo_path_ = Path(local_repo_path)
    if not local_repo_path_.exists():
        print(f"Cloning repository to {local_repo_path_.as_posix()}...")
        repo = Repo.clone_from(repo_url, local_repo_path)
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
        repo = Repo(local_repo_path_)
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


def replace_files_from_gcs(
    files_to_replace: list[str],
    gcs_base_path: str,
    bucket_name: str,
    service_account_key: str,
    local_repo_path: str,
    repo_base_path: str,
) -> None:
    """
    Download and overwrite files in the local git repository from GCS.

    Args:
        files_to_replace (list[str]): List of relative file paths to replace.
        gcs_base_path (str): Base path in GCS bucket.
        bucket_name (str): Name of the GCS bucket.
        service_account_key (str): Path to the GCP service account JSON key.
        local_repo_path (str): Local path to the git repository.
        repo_base_path (str): Base path in the git repo to copy files into.
    """
    for rel_path in files_to_replace:
        rel_path_clean = rel_path.lstrip("./")
        gcs_path = str(Path(gcs_base_path) / rel_path_clean)
        local_path = Path(local_repo_path) / repo_base_path / rel_path_clean
        download_gcs_file(bucket_name, gcs_path, local_path, service_account_key)
    print("All files replaced from GCS.")


def commit_and_push(repo: Repo, branch: str, repo_url: str):
    """
    Commit and push changes to the remote branch.

    Args:
        repo (Repo): GitPython Repo object.
        branch (str): Branch to push to.
        github_token (str): GitHub personal access token.
        repo_url (str): HTTPS URL of the remote repository.
    """
    repo.git.add(A=True)
    commit_msg = (
        f"Update stats files from GCS ({datetime.now().strftime('%Y-%m-%d %H:%M')})"
    )
    if repo.is_dirty():
        repo.index.commit(commit_msg)
        print("Committed changes.")
        repo.git.push(repo_url, branch)
        print(f"Pushed changes to branch {branch}.")
    else:
        print("No changes to commit.")


def create_pull_request(
    repo_url: str, github_token: str, branch: str, base_branch: str = "main"
) -> None:
    """
    Create a pull request from the branch to the base branch using PyGithub.

    Args:
        repo_url (str): HTTPS URL of the remote repository.
        github_token (str): GitHub personal access token.
        branch (str): Source branch for the PR.
        base_branch (str): Target branch for the PR (default: "main").
    """

    repo_full_name = repo_url.rstrip(".git").split("github.com/")[-1]
    g = Github(github_token)
    repo = g.get_repo(repo_full_name)
    title = f"Automated stats update {datetime.now().strftime('%Y-%m-%d %H:%M')}"
    body = "Automated update of stats files from GCS."
    try:
        pr = repo.create_pull(title=title, body=body, head=branch, base=base_branch)
        print(f"Pull request created: {pr.html_url}")
    except Exception as e:
        if "A pull request already exists" in str(e):
            print("A pull request already exists.")
        else:
            print(f"Failed to create pull request: {e}")


def main(
    files_to_replace: list[str],
    gcs_base_path: str,
    bucket_name: str,
    service_account_key: str,
    repo_url: str,
    github_token: str,
    local_repo_path: str,
    repo_base_path: str,
    branch: str = "stats_update",
    base_branch: str = "main",
) -> None:
    """
    Main process to replace files in a git repository with files from Google Cloud Storage and create a pull request.

    Args:
        files_to_replace (list[str]): List of relative file paths to replace.
        gcs_base_path (str): Base path in GCS bucket.
        bucket_name (str): Name of the GCS bucket.
        service_account_key (str): Path to the GCP service account JSON key.
        repo_url (str): HTTPS URL of the remote repository.
        github_token (str): GitHub personal access token.
        local_repo_path (str): Local path to the git repository.
        repo_base_path (str): Base path in the git repo to copy files into.
        branch (str): Branch to push changes to (default: "stats_update").
        base_branch (str): Target branch for the PR (default: "main").
    """
    print("--- Starting GCS to GitHub file replacement process ---")
    url_with_token = repo_url.replace("https://", f"https://{github_token}@")
    repo = ensure_git_repo(local_repo_path, url_with_token, branch)
    replace_files_from_gcs(
        files_to_replace,
        gcs_base_path,
        bucket_name,
        service_account_key,
        local_repo_path,
        repo_base_path,
    )
    commit_and_push(repo, branch, url_with_token)
    create_pull_request(repo_url, github_token, branch, base_branch)
    print("--- Process completed ---")


if __name__ == "__main__":
    # Example usage (replace with your actual values or call main from another script)
    files_to_replace = [
        "./stats/ee_month/month_file_1.csv",
        "./stats/ee_year/year_file_1.csv",
    ]
    gcs_base_path = "OSN/"
    bucket_name = "your-bucket-name"
    service_account_key = "path/to/service_account.json"
    repo_url = "https://github.com/username/repo.git"
    github_token = "your_github_token"
    local_repo_path = "./local_repo"
    repo_base_path = ""  # e.g., "src/" if you want to copy into a subfolder
    main(
        files_to_replace,
        gcs_base_path,
        bucket_name,
        service_account_key,
        repo_url,
        github_token,
        local_repo_path,
        repo_base_path,
    )
