import pytz
import json
import logging
from datetime import datetime
from pathlib import Path

from google.cloud import storage
from observatorio_ipa.core.config import LOGGER_NAME
from observatorio_ipa.core.workflows.images.monthly_export import _fix_name_prefix

logger = logging.getLogger(LOGGER_NAME)


def create_manifest(
    images: list[str], collection_path: str | Path, meta: dict = {}
) -> str:
    collection_path = Path(collection_path)
    images.sort()

    manifest = {
        "date_created": datetime.now(tz=pytz.UTC).isoformat(),
        "metadata": meta,
        "source": {
            "image_collection": collection_path.as_posix(),
            "first_image": images[0] if images else None,
            "last_image": images[-1] if images else None,
            "images": images,
        },
    }

    manifest_json = json.dumps(manifest, indent=2)

    # manifest_path = f"{image_collection_path}/monthly_manifest.json"
    # with open(manifest_path, "w") as f:
    #     json.dump(manifest, f)
    return manifest_json


def save_manifest_to_file(
    manifest_json: str,
    manifest_path: str | Path,
    manifest_name: str,
    overwrite: bool = False,
) -> None:
    full_manifest_path = Path(manifest_path, manifest_name)
    if full_manifest_path.exists():
        if overwrite:
            full_manifest_path.unlink(missing_ok=True)
        else:
            raise FileExistsError(f"File {full_manifest_path} already exists.")
    with open(full_manifest_path, "w") as f:
        f.write(manifest_json)


def save_manifest_to_storage(
    manifest_json: str,
    manifest_path: str | Path,
    manifest_name: str,
    storage_conn: storage.Client,
    storage_bucket: str,
    overwrite: bool = False,
) -> None:
    full_path = Path(manifest_path, manifest_name)
    bucket = storage_conn.bucket(storage_bucket)
    blob = bucket.blob(full_path.as_posix())

    if blob.exists():
        if overwrite:
            blob.delete()
        else:
            raise FileExistsError(
                f"Blob {full_path.as_posix()} already exists in bucket {storage_bucket}."
            )
    blob.upload_from_string(manifest_json)


def save_manifest(
    target: str,
    manifest_path: str | Path,
    manifest_name: str,
    manifest_json: str,
    storage_conn: storage.Client | None,
    storage_bucket: str | None,
    overwrite: bool = False,
) -> None:

    match target:
        case "storage":
            if not storage_conn or not storage_bucket:
                raise ValueError(
                    "Storage client and bucket name must be provided for storage target."
                )
            return save_manifest_to_storage(
                manifest_json,
                manifest_path,
                manifest_name,
                storage_conn,
                storage_bucket,
                overwrite,
            )

        case "file":
            return save_manifest_to_file(
                manifest_json, manifest_path, manifest_name, overwrite
            )
        case _:
            raise ValueError(f"Unknown stats_export_target: {target}:{manifest_path}")


def read_manifest_from_file(manifest_path: str | Path, manifest_name: str) -> dict:
    full_manifest_path = Path(manifest_path, manifest_name)
    try:
        with open(full_manifest_path, "r") as f:
            manifest = json.load(f)
        return manifest
    except Exception as e:
        logger.warning(f"Couldn't read manifest from file: {e}")
        return {}


def read_manifest_from_storage(
    storage_conn: storage.Client,
    storage_bucket: str,
    manifest_path: str | Path,
    name: str,
    overwrite: bool = False,
) -> dict:
    full_path = Path(manifest_path, name)
    try:
        bucket = storage_conn.bucket(storage_bucket)
        blob = bucket.blob(full_path.as_posix())
        manifest_json = blob.download_as_text()
        return json.loads(manifest_json)
    except Exception as e:
        logger.warning(f"Couldn't read manifest from storage: {e}")
        return {}


def get_manifest(
    source: str,
    manifest_path: str | Path,
    manifest_name: str,
    storage_conn: storage.Client | None,
    storage_bucket: str | None,
) -> dict:
    manifest_path = Path(manifest_path)
    match source:
        case "storage":
            if not storage_conn or not storage_bucket:
                raise ValueError(
                    "Storage client and bucket name must be provided for storage source."
                )
            return read_manifest_from_storage(
                storage_conn, storage_bucket, manifest_path, manifest_name
            )

        case "file":
            return read_manifest_from_file(manifest_path, manifest_name)
        case _:
            raise ValueError(f"Unknown stats_export_target: {source}:{manifest_path}")


def compare_manifest_to_collection(
    manifest_src: str,
    manifest_path: str | Path,
    manifest_name: str,
    collection_images: list[str],
    collection_path: str | Path,
    image_prefix: str,
    storage_conn: storage.Client | None = None,
    storage_bucket: str | None = None,
) -> bool:
    """Compares collection and image list from manifest to another collection and image list

    Returns True if information in manifest is the same as new collection

    args:
        manifest_src (str): Source of the manifest. One of 'file', 'storage'
        manifest_path (str | Path): Path to the manifest file or folder in storage
        manifest_name (str): Name of the manifest file
        collection_path (str | Path): Path to the collection folder
        image_prefix (str): Prefix for the image files
        storage_conn (storage.Client | None): Storage client if using storage source
        storage_bucket (str | None): Storage bucket name if using storage source
    returns:
        bool: True if manifest matches collection, False otherwise
    """

    manifest_path = Path(manifest_path)
    # collection_path = Path(collection_path)

    # Get List of Images from Collection
    image_prefix = _fix_name_prefix(image_prefix)

    # collection_images = _get_imgs_in_monthly_ic(
    #     monthly_collection_path=collection_path,
    #     name_prefix=image_prefix,
    # )

    # Get list of images in the manifest
    try:
        manifest = get_manifest(
            manifest_src, manifest_path, manifest_name, storage_conn, storage_bucket
        )
    except Exception as e:
        logger.warning(f"Couldn't read manifest: {e}")
        logger.warning(
            f"Assuming manifest is missing on purpose. If this is unexpected, check manifest inputs provided in settings TOML file"
        )
        return False

    manifest_collection_src = manifest.get("source", {})
    manifest_collection_path = Path(manifest_collection_src.get("image_collection", ""))
    manifest_images: list = manifest_collection_src.get("images", [])

    if collection_path != manifest_collection_path:
        logger.info(
            f"Collection path {collection_path} does not match manifest collection path {manifest_collection_path}."
        )
        return False

    if len(collection_images) != len(manifest_images):
        logger.info(
            f"Collection has {len(collection_images)} images, but manifest has {len(manifest_images)} images."
        )
        return False

    if collection_images.sort() != manifest_images.sort():
        logger.info(f"Images in the collection and manifest don't match.")
        return False
    return True
