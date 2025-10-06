import logging
from datetime import date
from pathlib import Path
from google.cloud import storage

from observatorio_ipa.core.config import LOGGER_NAME
from observatorio_ipa.services.gee.exports import ExportTaskList


logger = logging.getLogger(LOGGER_NAME)


def create_storage_client(project, credentials):
    """Creates a Google Cloud Storage client."""
    return storage.Client(project=project, credentials=credentials)


def upload_blob(storage_client, bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to the bucket."""
    # The ID of your GCS bucket
    # bucket_name = "your-bucket-name"
    # The path to your file to upload
    # source_file_name = "local/path/to/file"
    # The ID of your GCS object
    # destination_blob_name = "storage-object-name"

    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    # Optional: set a generation-match precondition to avoid potential race conditions
    # and data corruptions. The request to upload is aborted if the object's
    # generation number does not match your precondition. For a destination
    # object that does not yet exist, set the if_generation_match precondition to 0.
    # If the destination object already exists in your bucket, set instead a
    # generation-match precondition using its generation number.
    generation_match_precondition = 0

    blob.upload_from_filename(
        source_file_name, if_generation_match=generation_match_precondition
    )

    print(f"File {source_file_name} uploaded to {destination_blob_name}.")


def _get_files(
    files_path: str | Path, storage_conn: storage.Client, storage_bucket: str
) -> list[Path]:
    files_path = Path(files_path)
    prefix = files_path.as_posix() + "/"
    len_prefix = len(prefix)
    bucket = storage_conn.bucket(storage_bucket)
    blobs = bucket.list_blobs(prefix=prefix, delimiter="/")

    # Exclude Folders. Folders should end with "/" on account of delimiter="/"
    list_of_files = []
    for blob in blobs:
        relative_path = blob.name[len_prefix:]
        if relative_path and "/" not in relative_path:
            list_of_files.append(Path(relative_path))
    return list_of_files


def date_stamp_file(file: str | Path) -> Path:
    file = Path(file)
    lu = date.today().strftime("%Y%m%d")
    return file.parent / f"{file.stem}_LU{lu}{file.suffix}"


def _rename_files(
    files: list[str] | list[Path],
    files_path: str | Path,
    storage_conn: storage.Client,
    storage_bucket: str,
) -> list[dict]:
    files_ = [Path(f) for f in files]
    renamed_files = [
        {"name": f, "new_name": date_stamp_file(f), "succeeded": False} for f in files_
    ]

    bucket = storage_conn.bucket(storage_bucket)
    for item in renamed_files:
        try:
            full_item_name = Path(files_path, item["name"]).as_posix()
            full_item_new_name = Path(files_path, item["new_name"]).as_posix()
            blob = bucket.blob(full_item_name)
            new_blob = bucket.rename_blob(blob, full_item_new_name)
            item["succeeded"] = True
            print(f"Renamed {item['name']} to {new_blob.name}")
        except Exception as e:
            print(e)
            pass

    return renamed_files


def _move_files(
    files: list[str] | list[Path],
    src_path: str | Path,
    dest_path: str | Path,
    storage_conn: storage.Client,
    storage_bucket: str,
    overwrite: bool = False,
):
    files_ = [{"name": Path(f), "succeeded": False} for f in files]
    src_path_ = Path(src_path)
    dest_path_ = Path(dest_path)
    bucket = storage_conn.bucket(storage_bucket)
    for file in files_:
        src_file_name = Path(src_path_, file["name"]).as_posix()
        dest_file_name = Path(dest_path_, file["name"]).as_posix()

        src_blob = bucket.blob(src_file_name)
        dest_blob = bucket.blob(dest_file_name)
        if dest_blob.exists():
            if overwrite:
                dest_blob.delete()
            else:
                raise FileExistsError(
                    f"File {dest_file_name} already exists in bucket {storage_bucket}."
                )

        try:
            dest_blob = bucket.copy_blob(src_blob, bucket, dest_file_name)
            src_blob.delete()
            file["succeeded"] = True
            print(f"Moved {file['name']} to {dest_path_.as_posix()}/")
        except Exception as e:
            logger.warning(e)
            pass
    return files_


def _move_and_rename_files(
    files: list[str] | list[Path],
    src_path: str | Path,
    dest_path: str | Path,
    storage_conn: storage.Client,
    storage_bucket: str,
    dest_overwrite: bool = False,
):
    files_ = [{"name": Path(f), "succeeded": False} for f in files]
    src_path_ = Path(src_path)
    dest_path_ = Path(dest_path)
    bucket = storage_conn.bucket(storage_bucket)
    for file in files_:
        file["new_name"] = date_stamp_file(file["name"])
        src_file_name = Path(src_path_, file["name"]).as_posix()
        dest_file_name = Path(dest_path_, file["new_name"]).as_posix()

        src_blob = bucket.blob(src_file_name)
        dest_blob = bucket.blob(dest_file_name)
        # if src file doesn't exist - skip as successful
        if not src_blob.exists():
            file["succeeded"] = True
            continue

        if dest_blob.exists():
            if dest_overwrite:
                dest_blob.delete()
        try:
            dest_blob = bucket.copy_blob(src_blob, bucket, dest_file_name)
            src_blob.delete()
            file["succeeded"] = True
            print(f"Moved and renamed {src_file_name} to {dest_file_name}")
        except Exception as e:
            print(f"Filed to move {src_file_name}: {str(e)}")
            logger.warning(e)
            continue

    return files_


def _task_move_and_rename(
    task_list: ExportTaskList,
    src_path: str | Path,
    dest_path: str | Path,
    storage_conn: storage.Client,
    storage_bucket: str,
    dest_overwrite: bool = False,
) -> ExportTaskList:

    files_to_move = [item.name for item in task_list]
    files_moved = _move_and_rename_files(
        files=files_to_move,
        src_path=src_path,
        dest_path=dest_path,
        storage_conn=storage_conn,
        storage_bucket=storage_bucket,
        dest_overwrite=dest_overwrite,
    )

    # Update tasks that failed to move
    failed_moving = [item["name"] for item in files_moved if not item["succeeded"]]
    for task in task_list:
        if task.name in failed_moving:
            task.task_status = "FAILED"
            task.error = "Failed to move file to archive."
            task.task = None

    return task_list
