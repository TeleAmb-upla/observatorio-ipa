import os
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from google.oauth2 import service_account
from google_auth_oauthlib.flow import InstalledAppFlow

SCOPES = ["https://www.googleapis.com/auth/drive"]
# Set your folder IDs here
SOURCE_FOLDER_ID = "your-source-folder-id"
DEST_FOLDER_ID = "your-destination-folder-id"


def authenticate_with_oauth():
    """
    Authenticate to Google Drive using OAuth 2.0 user login flow.
    Returns:
        googleapiclient.discovery.Resource: Authenticated Google Drive service object.
    """
    flow = InstalledAppFlow.from_client_secrets_file("credentials.json", SCOPES)
    creds = flow.run_local_server(port=0)
    return build("drive", "v3", credentials=creds)


def authenticate_with_service_account():
    """
    Authenticate to Google Drive using a service account.
    Returns:
        googleapiclient.discovery.Resource: Authenticated Google Drive service object.
    """
    creds = service_account.Credentials.from_service_account_file(
        "service_account.json", scopes=SCOPES
    )
    return build("drive", "v3", credentials=creds)


def list_files(service, folder_id):
    """
    List all files in a specified Google Drive folder.
    Args:
        service (googleapiclient.discovery.Resource): Authenticated Google Drive service object.
        folder_id (str): The ID of the folder to list files from.
    Returns:
        list: List of file metadata dictionaries (id and name).
    """
    results = (
        service.files()
        .list(q=f"'{folder_id}' in parents and trashed=false", fields="files(id, name)")
        .execute()
    )
    return results.get("files", [])


def move_and_rename_file(service, file_id, new_name, dest_folder_id):
    """
    Move a file to a new folder and rename it in Google Drive.
    Args:
        service (googleapiclient.discovery.Resource): Authenticated Google Drive service object.
        file_id (str): The ID of the file to move and rename.
        new_name (str): The new name for the file.
        dest_folder_id (str): The ID of the destination folder.
    """
    # Move file
    file = service.files().get(fileId=file_id, fields="parents").execute()
    prev_parents = ",".join(file.get("parents"))
    service.files().update(
        fileId=file_id,
        addParents=dest_folder_id,
        removeParents=prev_parents,
        fields="id, parents",
    ).execute()
    # Rename file
    service.files().update(fileId=file_id, body={"name": new_name}).execute()


if __name__ == "__main__":
    # Choose authentication method:
    # service = authenticate_with_oauth()
    service = authenticate_with_service_account()

    files = list_files(service, SOURCE_FOLDER_ID)
    print(f"Found {len(files)} files in source folder.")
    for idx, file in enumerate(files, 1):
        new_name = f"renamed_{file['name']}"
        print(f"Moving and renaming: {file['name']} -> {new_name}")
        move_and_rename_file(service, file["id"], new_name, DEST_FOLDER_ID)
    print("Done.")
