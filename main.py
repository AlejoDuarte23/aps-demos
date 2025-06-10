import os
import time
import base64
import json
import requests
from pathlib import Path
from functools import lru_cache
from dotenv import load_dotenv
from typing import Annotated

load_dotenv()


CLIENT_ID = os.environ.get("CLIENT_ID")
CLIENT_SECRET = os.environ.get("CLIENT_SECRET")

BUCKET_KEY = f"dwg-multi-process-demo-{CLIENT_ID.lower()}"
INPUT_DWG_FILE = Path("visualization_-_conference_room.dwg")
OUTPUT_PDF_FILE = INPUT_DWG_FILE.with_suffix(".pdf")

APS_BASE_URL = "https://developer.api.autodesk.com"
OSS_BASE_URL = f"{APS_BASE_URL}/oss/v2"
MD_BASE_URL = f"{APS_BASE_URL}/modelderivative/v2"
DA_BASE_URL = f"{APS_BASE_URL}/da/us-east/v3"
AUTH_URL = f"{APS_BASE_URL}/authentication/v2/token"
SCOPES = "data:read data:write data:create bucket:create bucket:read code:all"


@lru_cache(maxsize=1)
def get_token() -> str:
    print("Requesting new 2-legged token...")
    response = requests.post(
        AUTH_URL,
        data={
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
            "grant_type": "client_credentials",
            "scope": SCOPES,
        },
        timeout=15,
    )
    response.raise_for_status()
    token = response.json()["access_token"]
    print("Token obtained successfully.")
    return token


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


def upload_file_via_s3(token: str, object_name: str, file_path: Path) -> Annotated[str, "Object ID"]:
    """Presign object for upload -> Allows file upload without needing full Forge credentials.
    Upload file -> Uses the presigned URL to perform the upload.
    """
    print(f"Uploading '{file_path.name}' to OSS bucket...")
    # Presign Url.
    s3_upload_endpoint = f"{OSS_BASE_URL}/buckets/{BUCKET_KEY}/objects/{object_name}/signeds3upload"
    response = requests.get(s3_upload_endpoint, headers={"Authorization": f"Bearer {token}"}, timeout=15)
    response.raise_for_status()
    signed_url_data = response.json()
    # Upload file to signed url
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
    """This is used to crete place holder (0 bytes -> data=b"") of the PDF or the final file we
    want to create"""
    # Presign Url
    print(f"Creating empty placeholder for output object: {object_name}...")
    s3_upload_endpoint = f"{OSS_BASE_URL}/buckets/{BUCKET_KEY}/objects/{object_name}/signeds3upload"
    get_url_response = requests.get(s3_upload_endpoint, headers={"Authorization": f"Bearer {token}"}, timeout=15)
    if get_url_response.status_code == 409:
        print("Placeholder object already exists.")
        return
    get_url_response.raise_for_status()
    signed_url_data = get_url_response.json()
    # Create 0 bytes object
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
    """Signed url alow us to perform comnine operations with the objects
    in the buckets based on the "access" type ("read", "write", "delete").
    """
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


def get_signed_s3_upload_url(
    token: str, object_name: str
) -> tuple[Annotated[str, "Upload Url"], Annotated[str, "Upload Key"]]:
    """Generates a pre-signed URL and uploadKey suitable for uploading a file to OSS.
    this is usefull for:
        - Uploading large files: BIM models, CAD drawings) efficiently.
        - Integrating with client-side libraries or tools that can perform S3-compatible multipart uploads.
        - When you need more robust upload capabilities than a simple PUT request."""

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


def finalize_da_upload(token: str, object_name: str, upload_key: str, file_size: int) -> None:
    """Finalizes a direct S3 upload initiated by Design Automation. Uses the get_signed_s3_upload_url output.
    this acts as a "commit" signal. Even if all the bytes of your file have been uploaded, the object won't be fully assembled and accessible in your bucket until this finalization call is made.

    if da_final_status and da_final_status["status"] == "success":
    file_size = da_final_status["stats"]["bytesUploaded"]
    finalize_da_upload(token, OUTPUT_PDF_FILE.name, da_upload_key, file_size)
    """
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
    """This translate the object in to json basically -> like dwg to json
    so using the file urn -> Uniform Resource Name the file is identified 
    and the MD -> model derivative API translate it"""
    print("\nMD: Starting derivative translation job for 2D views...")
    job_payload = {"input": {"urn": object_urn}, "output": {"formats": [{"type": "svf", "views": ["2d"]}]}}
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json", "x-ads-force": "true"}
    response = requests.post(f"{MD_BASE_URL}/designdata/job", headers=headers, json=job_payload, timeout=30)
    response.raise_for_status()
    print("MD: Translation job submitted.")


def start_da_work_item(token: str, input_url: str, output_url: str) -> str:
    """Creates and submits a Design Automation workitem to convert DWG to PDF.
    Inputs the DWG's signed urls with read permisions
    and the PDF Place holder's signed url to be modified (verb -> put)
    """
    print("\nDA: Submitting WorkItem to convert DWG to PDF...")

    # Found this activity Id in the vs ccode extension of APS
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


def get_svf_translation_status(token: str, object_urn: str) -> tuple[Annotated[str,"Translation Status"], Annotated[str, "Transalation Progress"]]:
    """Checks the Model Derivative job status ONCE and returns it.
    svf -> simple viewer fromat (translated)"""
    response = requests.get(
        f"{MD_BASE_URL}/designdata/{object_urn}/manifest", headers={"Authorization": f"Bearer {token}"}, timeout=30
    )
    if response.status_code == 202:
        return "inprogress", "Manifest not ready"
    response.raise_for_status()
    manifest = response.json()
    return manifest.get("status"), manifest.get("progress", "N/A")


def get_da_work_item_status(token: str, work_item_id: str) -> dict:
    """Checks the Design Automation WorkItem status ONCE and returns the full response data."""
    check_status_endpoint = f"{DA_BASE_URL}/workitems/{work_item_id}"
    response = requests.get(check_status_endpoint, headers={"Authorization": f"Bearer {token}"}, timeout=20)
    response.raise_for_status()
    return response.json()


def download_result_from_bucket(token: str, object_name: str, destination_path: Path):
    """
    After all the work is done we return the final pdf
    """
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

        output_pdf_url, da_upload_key = get_signed_s3_upload_url(token, OUTPUT_PDF_FILE.name)
        # Hard coded activity -> "activityId": "AutoCAD.PlotToPDF+prod",
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

def generate_pdf_with_da(input_dwg_path: Path) -> None:
    """
    Runs a complete Design Automation workflow to convert a local DWG file to a PDF.

    This function will:
    1. Upload the DWG to an OSS bucket.
    2. Submit a Design Automation job to convert the DWG to a PDF in the cloud.
    3. Wait for the job to complete.
    4. Download the resulting PDF to the local filesystem.
    """
    if not input_dwg_path.exists():
        raise FileNotFoundError(f"Input file not found at '{input_dwg_path}'.")

    output_pdf_path = input_dwg_path.with_suffix(".pdf")

    print("--- STARTING DESIGN AUTOMATION PDF GENERATION WORKFLOW ---")
    try:
        token = get_token()
        create_bucket_if_not_exists(token)

        # Note: For simplicity, this uploads the file every time.
        # In a real app, you might check if it already exists.
        upload_file_via_s3(token, input_dwg_path.name, input_dwg_path)
        create_placeholder_object_via_s3(token, output_pdf_path.name)

        input_dwg_url = get_signed_url(token, input_dwg_path.name, access="read")
        output_pdf_url, da_upload_key = get_signed_s3_upload_url(token, output_pdf_path.name)
        da_work_item_id = start_da_work_item(token, input_dwg_url, output_pdf_url)

        print("\n--- Waiting for Design Automation Job to Finish ---")
        da_final_status = None
        while True:
            status_data = get_da_work_item_status(token, da_work_item_id)
            da_status = status_data["status"]
            print(f"  > DA Status: {da_status}")

            if da_status in ["success", "failed", "cancelled", "failedUpload"]:
                da_final_status = status_data
                da_report_url = status_data.get("reportUrl")
                if da_report_url:
                    print(
                        "--- DA REPORT ---\n"
                        + requests.get(da_report_url, timeout=30).text
                        + "\n--- END REPORT ---"
                    )
                if da_status != "success":
                    raise RuntimeError(f"Design Automation work item failed with status: {da_status}")
                break  # Exit the loop
            time.sleep(10)

        file_size = da_final_status["stats"]["bytesUploaded"]
        finalize_da_upload(token, output_pdf_path.name, da_upload_key, file_size)

        download_result_from_bucket(token, output_pdf_path.name, output_pdf_path)
        print("\n✅✅✅ PDF Generation Complete ✅✅✅")
        print(f"  -> PDF result has been downloaded to: {output_pdf_path.resolve()}")

    except requests.exceptions.RequestException as e:
        print(f"\nERROR: An API request failed: {e}")
        if e.response is not None:
            print(f"--> Response [{e.response.status_code}]: {e.response.text}")
    except (RuntimeError, KeyError, ValueError, FileNotFoundError) as e:
        print(f"\nERROR: An error occurred: {e}")


def translate_dwg_for_viewer(input_dwg_path: Path) -> None:
    """
    Runs a complete Model Derivative workflow to prepare a DWG for web viewing.

    This function will:
    1. Upload the DWG file to an OSS bucket.
    2. Submit a Model Derivative job to translate the file into SVF format.
    3. Wait for the translation to complete.
    4. Print the final URN needed to load the model in the Forge Viewer.
    """
    if not input_dwg_path.exists():
        raise FileNotFoundError(f"Input file not found at '{input_dwg_path}'.")

    print("\n--- STARTING MODEL DERIVATIVE (VIEWER) TRANSLATION WORKFLOW ---")
    try:
        token = get_token()
        create_bucket_if_not_exists(token)

        # Note: For simplicity, this uploads the file every time.
        # In a real app, you might check if it already exists.
        dwg_object_id = upload_file_via_s3(token, input_dwg_path.name, input_dwg_path)
        dwg_urn = safe_base64_encode(dwg_object_id)

        start_svf_translation_job(token, dwg_urn)

        print("\n--- Waiting for Model Derivative Job to Finish ---")
        while True:
            md_status, md_progress = get_svf_translation_status(token, dwg_urn)
            print(f"  > MD Status: {md_status} ({md_progress})")
            if md_status in ["success", "failed"]:
                if md_status == "failed":
                    raise RuntimeError("Model Derivative translation failed!")
                break # Exit the loop
            time.sleep(10)

        print("\n✅✅✅ Viewer Translation Complete ✅✅✅")
        print(f"  -> Use the following URN to load your model in the viewer: {dwg_urn}")

    except requests.exceptions.RequestException as e:
        print(f"\nERROR: An API request failed: {e}")
        if e.response is not None:
            print(f"--> Response [{e.response.status_code}]: {e.response.text}")
    except (RuntimeError, KeyError, ValueError, FileNotFoundError) as e:
        print(f"\nERROR: An error occurred: {e}")



if __name__ == "__main__":
    # main()
    generate_pdf_with_da(input_dwg_path=INPUT_DWG_FILE)
