import os
import time
import base64
import requests #type: ignore
import viktor as vkt #type: ignore

from functools import lru_cache
from dotenv import load_dotenv
from typing import Annotated

load_dotenv()

CLIENT_ID = os.environ.get("CLIENT_ID")
CLIENT_SECRET = os.environ.get("CLIENT_SECRET")


APS_BASE_URL = "https://developer.api.autodesk.com"
OSS_BASE_URL = f"{APS_BASE_URL}/oss/v2"
MD_BASE_URL = f"{APS_BASE_URL}/modelderivative/v2"
DA_BASE_URL = f"{APS_BASE_URL}/da/us-east/v3"
AUTH_URL = f"{APS_BASE_URL}/authentication/v2/token"
SCOPES = "data:read data:write data:create bucket:create bucket:read code:all"

@lru_cache(maxsize=1)
def get_token(client_id: str, client_secret: str) -> str:
    vkt.UserMessage.info("Requesting new 2-legged token...")

    response = requests.post(
        AUTH_URL,
        data={
            "client_id": client_id,
            "client_secret": client_secret,
            "grant_type": "client_credentials",
            "scope": SCOPES,
        },
        timeout=15,
    )
    response.raise_for_status()
    token = response.json()["access_token"]
    vkt.UserMessage.info("Token obtained successfully.")
    return token


def create_bucket_if_not_exists(token: str, bucket_key: str) -> None:
    vkt.UserMessage.info("Checking/Creating bucket")
    response = requests.post(
        f"{OSS_BASE_URL}/buckets",
        json={"bucketKey": bucket_key, "policyKey": "transient"},
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        timeout=15,
    )
    if response.status_code not in [200, 409]:
        response.raise_for_status()
    vkt.UserMessage.info("Bucket checked/created successfully.")


def upload_to_OSS(token: str, object_name: str, file_content: bytes, bucket_key: str) -> Annotated[str, "Object ID"]:
    """Presign object for upload using binary content. Allows file upload without needing full APS credentials.
    Upload file. Uses the presigned URL to perform the upload.
    """
    vkt.UserMessage.info(f"Uploading binary data as '{object_name}' to OSS bucket...")
    
    # Presign URL.
    s3_upload_endpoint = f"{OSS_BASE_URL}/buckets/{bucket_key}/objects/{object_name}/signeds3upload"
    response = requests.get(s3_upload_endpoint, headers={"Authorization": f"Bearer {token}"}, timeout=15)
    response.raise_for_status()
    signed_url_data = response.json()
    
    # Upload file to signed url
    s3_response = requests.put(signed_url_data["urls"][0], data=file_content, timeout=120)
    s3_response.raise_for_status()
    finalize_response = requests.post(
        s3_upload_endpoint,
        json={"uploadKey": signed_url_data["uploadKey"], "size": len(file_content)},
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        timeout=30,
    )
    finalize_response.raise_for_status()
    vkt.UserMessage.info(f"Binary data '{object_name}' uploaded successfully.")
    return finalize_response.json()["objectId"]


def safe_base64_encode(text: str) -> str:
    return base64.urlsafe_b64encode(text.encode()).decode().strip("=")


def start_svf_translation_job(token: str, object_urn: str):
    """This translates the object into something readable, e.g., DWG to JSON.
    Using the file URN (Uniform Resource Name), the file is identified
    and the Model Derivative (MD) API translates it.
    """
    vkt.UserMessage.info("MD: Starting derivative translation job")
    job_payload = {"input": {"urn": object_urn}, "output": {"formats": [{"type": "svf", "views": ["2d"]}]}}
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json", "x-ads-force": "true"}
    response = requests.post(f"{MD_BASE_URL}/designdata/job", headers=headers, json=job_payload, timeout=30)
    response.raise_for_status()
    vkt.UserMessage.info("MD: Translation job submitted.")


def get_svf_translation_status(token: str, object_urn: str) -> tuple[Annotated[str,"Translation Status"], Annotated[str, "Transalation Progress"]]:
    """Checks the Model Derivative job status ONCE and returns it.
    SVF -> Simple Viewer Format (translated)
    """
    response = requests.get(
        f"{MD_BASE_URL}/designdata/{object_urn}/manifest", headers={"Authorization": f"Bearer {token}"}, timeout=30
    )
    if response.status_code == 202:
        return "inprogress", "Manifest not ready"
    response.raise_for_status()
    manifest = response.json()
    return manifest.get("status"), manifest.get("progress", "N/A")


def process_cad_file(object_name: str, file_content: bytes, token: str, client_id: str) -> Annotated[str, "Uniform Resource Name"]:
    """Process the CAD file to suit the requirements of the APS viewer.
    1. Create or check if a bucket exists. A unique bucket is created or checked if it exists.
    2. Upload the file to an OSS bucket (A bucket is created using the client ID).
    3. Translate the file into SVF.
    4. Return the URN once the model is translated.
    """
    # Check or create a bucket for VIKTOR
    BUCKET_KEY = f"viktor-bucket-{client_id.lower()}"
    create_bucket_if_not_exists(token=token, bucket_key=BUCKET_KEY)
    # Upload file to the bucket
    oss_object_id = upload_to_OSS(token=token, object_name=object_name, file_content=file_content, bucket_key=BUCKET_KEY)
    urn = safe_base64_encode(oss_object_id)
    # Translate file to be able to be used by the APS viewer
    start_svf_translation_job(token, urn)

    md_finished = False

    while not md_finished:
        try:
            md_status, md_progress = get_svf_translation_status(token, urn)
            vkt.UserMessage.info(f"  > MD Status: {md_status} ({md_progress})")

            if md_status in ["success", "failed"]:
                md_finished = True
                if md_status == "failed":
                    raise vkt.UserError("Model Derivative translation failed!")
        except requests.exceptions.RequestException as e:
            vkt.UserMessage.info(f"  > Error checking MD status: {e}. Retrying...")
        
        time.sleep(10)
    return urn