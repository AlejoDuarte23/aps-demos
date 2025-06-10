import os
import time
import base64
import json
from pathlib import Path
import requests

from dotenv import load_dotenv

load_dotenv()


CLIENT_ID = os.environ.get("CLIENT_ID")
CLIENT_SECRET = os.environ.get("CLIENT_SECRET")


BUCKET_KEY = f"dwg-multi-process-demo-{CLIENT_ID.lower()}"
INPUT_DWG_FILE = Path("visualization_-_conference_room.dwg")
OUTPUT_PDF_FILE = INPUT_DWG_FILE.with_suffix(".pdf")

APS_BASE_URL = "https://developer.api.autodesk.com"
OSS_BASE_URL = f"{APS_BASE_URL}/oss/v2"
MD_BASE_URL = f"{APS_BASE_URL}/modelderivative/v2"
DA_BASE_URL = f"{APS_BASE_URL}/da/us-east/v3"  # Defined for clarity

SCOPES = "data:read data:write data:create bucket:create bucket:read code:all"
_token = None


def get_token() -> str:
    global _token
    if _token:
        return _token
    print("Requesting new 2-legged token...")
    response = requests.post(
        f"{APS_BASE_URL}/authentication/v2/token",
        data={
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
            "grant_type": "client_credentials",
            "scope": SCOPES,
        },
        timeout=15,
    )
    response.raise_for_status()
    _token = response.json()["access_token"]
    print("Token obtained successfully.")
    return _token


def create_bucket_if_not_exists(token: str) -> None:
    print(f"Checking/Creating bucket: {BUCKET_KEY}...")
    response = requests.post(
        f"{OSS_BASE_URL}/buckets",
        json={"bucketKey": BUCKET_KEY, "policyKey": "transient"},
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        timeout=15,
    )
    if response.status_code not in [200, 409]:
        response.raise_for_status()
    print("Bucket checked/created successfully.")


def upload_file_via_s3(token: str, object_name: str, file_path: Path) -> str:
    print(f"Uploading '{file_path.name}' to OSS bucket...")
    s3_upload_endpoint = f"{OSS_BASE_URL}/buckets/{BUCKET_KEY}/objects/{object_name}/signeds3upload"
    response = requests.get(s3_upload_endpoint, headers={"Authorization": f"Bearer {token}"}, timeout=15)
    response.raise_for_status()
    signed_url_data = response.json()
    with file_path.open("rb") as fp:
        file_content = fp.read()
    s3_response = requests.put(signed_url_data["urls"][0], data=file_content, timeout=120)
    s3_response.raise_for_status()
    finalize_response = requests.post(
        s3_upload_endpoint,
        json={"uploadKey": signed_url_data["uploadKey"], "size": len(file_content)},
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        timeout=30,
    )
    finalize_response.raise_for_status()
    print(f"File '{file_path.name}' uploaded successfully.")
    return finalize_response.json()["objectId"]


def create_placeholder_object_via_s3(token: str, object_name: str) -> None:
    print(f"Creating empty placeholder for output object: {object_name}...")
    s3_upload_endpoint = f"{OSS_BASE_URL}/buckets/{BUCKET_KEY}/objects/{object_name}/signeds3upload"
    get_url_response = requests.get(s3_upload_endpoint, headers={"Authorization": f"Bearer {token}"}, timeout=15)
    if get_url_response.status_code == 409:
        print("Placeholder object already exists.")
        return
    get_url_response.raise_for_status()
    signed_url_data = get_url_response.json()
    s3_response = requests.put(signed_url_data["urls"][0], data=b"", timeout=30)
    s3_response.raise_for_status()
    finalize_response = requests.post(
        s3_upload_endpoint,
        json={"uploadKey": signed_url_data["uploadKey"], "size": 0},
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        timeout=30,
    )
    finalize_response.raise_for_status()
    print("Empty placeholder object created successfully.")


def get_signed_url(token: str, object_name: str, access: str) -> str:
    print(f"Generating signed URL for '{object_name}' with '{access}' access...")
    response = requests.post(
        f"{OSS_BASE_URL}/buckets/{BUCKET_KEY}/objects/{object_name}/signed",
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        json={"access": access},
        timeout=15,
    )
    response.raise_for_status()
    print(f"Signed {access.upper()} URL generated.")
    return response.json()["signedUrl"]


# FIX: Modified to return the uploadKey, which is needed for finalization.
def get_signed_s3_upload_url(token: str, object_name: str) -> tuple[str, str]:
    """Generates a pre-signed URL and uploadKey suitable for uploading a file to OSS."""
    print(f"Generating signed S3 UPLOAD URL for '{object_name}'...")
    s3_upload_endpoint = f"{OSS_BASE_URL}/buckets/{BUCKET_KEY}/objects/{object_name}/signeds3upload"
    response = requests.get(s3_upload_endpoint, headers={"Authorization": f"Bearer {token}"}, timeout=15)
    response.raise_for_status()
    signed_url_data = response.json()
    upload_url = signed_url_data["urls"][0]
    upload_key = signed_url_data["uploadKey"]
    print("Signed UPLOAD URL and uploadKey generated.")
    return upload_url, upload_key


def safe_base64_encode(text: str) -> str:
    return base64.urlsafe_b64encode(text.encode()).decode().strip("=")


# FIX: New function to finalize the S3 upload after the DA job completes.
def finalize_da_upload(token: str, object_name: str, upload_key: str, file_size: int) -> None:
    """Finalizes a direct S3 upload initiated by Design Automation."""
    print(f"Finalizing S3 upload for '{object_name}'...")
    s3_upload_endpoint = f"{OSS_BASE_URL}/buckets/{BUCKET_KEY}/objects/{object_name}/signeds3upload"
    finalize_response = requests.post(
        s3_upload_endpoint,
        json={"uploadKey": upload_key, "size": file_size},
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        timeout=30,
    )
    finalize_response.raise_for_status()
    print("S3 upload finalized successfully.")


def start_svf_translation_job(token: str, object_urn: str):
    print("\nMD: Starting derivative translation job for 2D views...")
    job_payload = {"input": {"urn": object_urn}, "output": {"formats": [{"type": "svf", "views": ["2d"]}]}}
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json", "x-ads-force": "true"}
    response = requests.post(f"{MD_BASE_URL}/designdata/job", headers=headers, json=job_payload, timeout=30)
    response.raise_for_status()
    print("MD: Translation job submitted.")


def start_da_work_item(token: str, input_url: str, output_url: str) -> str:
    """Creates and submits a Design Automation WorkItem to convert DWG to PDF."""
    print("\nDA: Submitting WorkItem to convert DWG to PDF...")

    payload = {
        "activityId": "AutoCAD.PlotToPDF+prod",
        "arguments": {"HostDwg": {"url": input_url, "verb": "get"}, "Result": {"url": output_url, "verb": "put"}},
    }

    work_item_endpoint = f"{DA_BASE_URL}/workitems"

    response = requests.post(
        work_item_endpoint,
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        json=payload,
        timeout=30,
    )
    response.raise_for_status()
    work_item_id = response.json()["id"]
    print(f"DA: WorkItem submitted. ID: {work_item_id}")
    return work_item_id


def get_svf_translation_status(token: str, object_urn: str) -> tuple[str, str]:
    """Checks the Model Derivative job status ONCE and returns it."""
    response = requests.get(
        f"{MD_BASE_URL}/designdata/{object_urn}/manifest", headers={"Authorization": f"Bearer {token}"}, timeout=30
    )
    if response.status_code == 202:
        return "inprogress", "Manifest not ready"
    response.raise_for_status()
    manifest = response.json()
    return manifest.get("status"), manifest.get("progress", "N/A")


# FIX: Modified to return the full JSON response on success to extract stats.
def get_da_work_item_status(token: str, work_item_id: str) -> dict:
    """Checks the Design Automation WorkItem status ONCE and returns the full response data."""
    check_status_endpoint = f"{DA_BASE_URL}/workitems/{work_item_id}"
    response = requests.get(check_status_endpoint, headers={"Authorization": f"Bearer {token}"}, timeout=20)
    response.raise_for_status()
    return response.json()


def download_result_from_bucket(token: str, object_name: str, destination_path: Path):
    download_url = get_signed_url(token, object_name, access="read")
    print(f"\nDownloading result to '{destination_path}'...")
    response = requests.get(download_url, timeout=120)
    response.raise_for_status()
    destination_path.write_bytes(response.content)
    print("PDF successfully downloaded!")


def main() -> None:
    if not INPUT_DWG_FILE.exists():
        raise FileNotFoundError(f"Input file not found at '{INPUT_DWG_FILE}'.")

    try:
        token = get_token()
        create_bucket_if_not_exists(token)

        dwg_object_id = upload_file_via_s3(token, INPUT_DWG_FILE.name, INPUT_DWG_FILE)
        dwg_urn = safe_base64_encode(dwg_object_id)
        create_placeholder_object_via_s3(token, OUTPUT_PDF_FILE.name)

        start_svf_translation_job(token, dwg_urn)

        input_dwg_url = get_signed_url(token, INPUT_DWG_FILE.name, access="read")
        # FIX: Capture both the URL and the uploadKey.
        output_pdf_url, da_upload_key = get_signed_s3_upload_url(token, OUTPUT_PDF_FILE.name)
        da_work_item_id = start_da_work_item(token, input_dwg_url, output_pdf_url)

        print("\n--- Waiting for Both Cloud Jobs to Finish (MD and DA) ---")
        md_finished, da_finished = False, False
        da_final_status = None
        while not md_finished or not da_finished:
            if not md_finished:
                try:
                    md_status, md_progress = get_svf_translation_status(token, dwg_urn)
                    print(f"  > MD Status: {md_status} ({md_progress})")
                    if md_status in ["success", "failed"]:
                        md_finished = True
                        if md_status == "failed":
                            raise RuntimeError("Model Derivative translation failed!")
                except requests.exceptions.RequestException as e:
                    print(f"  > Error checking MD status: {e}. Retrying...")

            if not da_finished:
                try:
                    status_data = get_da_work_item_status(token, da_work_item_id)
                    da_status = status_data["status"]
                    print(f"  > DA Status: {da_status}")

                    if da_status in ["success", "failed", "cancelled", "failedUpload"]:
                        da_finished = True
                        da_final_status = status_data  # Save the final status object
                        da_report_url = status_data.get("reportUrl")
                        if da_report_url:
                            print(
                                "--- DA REPORT ---\n"
                                + requests.get(da_report_url, timeout=30).text
                                + "\n--- END REPORT ---"
                            )
                        if da_status != "success":
                            raise RuntimeError(f"Design Automation work item failed with status: {da_status}")
                except requests.exceptions.RequestException as e:
                    print(f"  > Error checking DA status: {e}. Retrying...")

            if not md_finished or not da_finished:
                time.sleep(10)

        # FIX: Finalize the upload after DA job success.
        if da_final_status and da_final_status["status"] == "success":
            file_size = da_final_status["stats"]["bytesUploaded"]
            finalize_da_upload(token, OUTPUT_PDF_FILE.name, da_upload_key, file_size)
        else:
            # If we get here, the DA job failed, so we raise an error.
            raise RuntimeError(
                f"Cannot finalize upload because DA job did not succeed. Final status: {da_final_status['status']}"
            )

        download_result_from_bucket(token, OUTPUT_PDF_FILE.name, OUTPUT_PDF_FILE)

        print("\n\n✅✅✅ WORKFLOW COMPLETE ✅✅✅")
        print(f"  -> 2D Viewer derivatives are ready for URN: {dwg_urn}")
        print(f"  -> PDF result has been downloaded to: {OUTPUT_PDF_FILE.resolve()}")

    except requests.exceptions.RequestException as e:
        print(f"\nERROR: An API request failed: {e}")
        if e.response is not None:
            try:
                print(f"--> Response [{e.response.status_code}]: {e.response.json()}")
            except json.JSONDecodeError:
                print(f"--> Response [{e.response.status_code}]: {e.response.text}")
    except (RuntimeError, KeyError, ValueError, FileNotFoundError) as e:
        print(f"\nERROR: An error occurred: {e}")


if __name__ == "__main__":
    main()
