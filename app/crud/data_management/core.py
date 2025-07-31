import requests
import base64
import urllib.parse
from app.models.hubs import HubsList
from app.models.projects import ProjectsList
from app.models.folders import FoldersList
from app.models.contents import FolderContentsList

APS_BASE_URL = "https://developer.api.autodesk.com"

def get_hubs(token) -> HubsList:
    """
    Retrieves a list of hubs the user has access to.
    Corresponds to: GET /project/v1/hubs
    """
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.get(f"{APS_BASE_URL}/project/v1/hubs", headers=headers)
    response.raise_for_status()
    # print(response.text)
    hubs_data = HubsList.model_validate_json(response.text)
    return hubs_data

def get_projects(hub_id, token) -> ProjectsList:
    """
    Retrieves a list of projects within a specific hub.
    Corresponds to: GET /project/v1/hubs/{hub_id}/projects
    """
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.get(f"{APS_BASE_URL}/project/v1/hubs/{hub_id}/projects", headers=headers)
    response.raise_for_status()
    # print(response.text)
    return ProjectsList.model_validate_json(response.text)

def get_top_folders(hub_id, project_id, token) -> FoldersList:
    """
    Retrieves the top-level folders of a project.
    Corresponds to: GET /project/v1/hubs/{hub_id}/projects/{project_id}/topFolders
    """
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.get(f"{APS_BASE_URL}/project/v1/hubs/{hub_id}/projects/{project_id}/topFolders", headers=headers)
    response.raise_for_status()
    # print("[DEBUG]", f"{response.text=}")
    return FoldersList.model_validate_json(response.text)


def get_folder_contents(project_id, folder_id, token) -> FolderContentsList:
    """
    Retrieves the contents (files and subfolders) of a specific folder.
    Corresponds to: GET /data/v1/projects/{project_id}/folders/{folder_id}/contents
    """
    headers = {"Authorization": f"Bearer {token}"}
    encoded_folder_id = urllib.parse.quote(folder_id) # URL-encode the ID
    url = f"https://developer.api.autodesk.com/data/v1/projects/{project_id}/folders/{encoded_folder_id}/contents"
    response = requests.get(url, headers=headers)
    # print("***")
    # print(response.text)
    response.raise_for_status()
    return FolderContentsList.model_validate_json(response.text)

def get_item_versions(project_id, item_id, token):
    """
    Retrieves all versions of a specific item (file).
    response.raise_for_status()

    Corresponds to: GET /data/v1/projects/{project_id}/items/{item_id}/versions
    """
    headers = {"Authorization": f"Bearer {token}"}
    encoded_item_id = urllib.parse.quote(item_id) # URL-encode the ID
    url = f"https://developer.api.autodesk.com/data/v1/projects/{project_id}/items/{encoded_item_id}/versions"
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.json().get("data", [])

def get_model_views_and_metadata(urn, token):
    """
    Retrieves the model views (and their metadata) for a given file URN.
    The URN must be for a file that has been translated to SVF or SVF2.
    """
    encoded_urn = base64.urlsafe_b64encode(urn.encode()).decode().rstrip("=")
    headers = {"Authorization": f"Bearer {token}"}
    
    manifest_url = f"{APS_BASE_URL}/modelderivative/v2/designdata/{encoded_urn}/manifest"
    manifest_response = requests.get(manifest_url, headers=headers)
    
    # If the manifest doesn't exist (404), it means the file was never translated.
    if manifest_response.status_code == 404:
        print(f"    - Info: No derivative manifest found. File has not been translated.")
        return None
    
    # For any other error, raise it.
    manifest_response.raise_for_status()
    manifest = manifest_response.json()

    if manifest.get('status') != 'success':
        print(f"    - Translation status for {urn}: {manifest.get('status')} ({manifest.get('progress', '')})")
        return None

    metadata_url = f"{APS_BASE_URL}/modelderivative/v2/designdata/{encoded_urn}/metadata"
    metadata_response = requests.get(metadata_url, headers=headers)
    
    if metadata_response.status_code == 200:
        return metadata_response.json().get("data", {}).get("metadata", [])
    else:
        print(f"    - Could not retrieve metadata for {urn}. Status: {metadata_response.status_code}")
        return None

def download_file_content(storage_urn, token):
    """
    Downloads the actual content of a file from OSS given its storage URN.
    """
    headers = {"Authorization": f"Bearer {token}"}

    # 1. Parse the URN to get the bucket key and object key
    # Example URN: "urn:adsk.objects:os.object:wip.dm.prod/abcdef.ifc"
    urn_parts = storage_urn.split(':')
    object_id = urn_parts[-1]
    bucket_key, object_key = object_id.split('/')
    
    encoded_bucket_key = urllib.parse.quote(bucket_key)
    encoded_object_key = urllib.parse.quote(object_key)

    # 2. Get a temporary, signed S3 URL to download the file directly
    # This is the most efficient method as it bypasses APS servers for the download.
    s3_url_endpoint = f"{APS_BASE_URL}/oss/v2/buckets/{encoded_bucket_key}/objects/{encoded_object_key}/signeds3download"
    
    s3_response = requests.get(s3_url_endpoint, headers=headers)
    s3_response.raise_for_status()
    s3_data = s3_response.json()
    
    # The response will contain a URL to download from. If the object was uploaded in chunks
    # it might contain multiple URLs. For most ACC files, it will be one.
    download_url = s3_data.get('url')
    if not download_url:
        raise ValueError("Could not retrieve the S3 download URL from APS.")

    # 3. Use the S3 URL to get the file content
    # Note: No auth headers are needed for the S3 URL itself.
    file_response = requests.get(download_url)
    file_response.raise_for_status()
    
    # The content is in binary format
    return file_response.content