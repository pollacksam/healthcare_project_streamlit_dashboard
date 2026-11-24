import sys
import json
import io
import boto3

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

S3_BUCKET = "healthcare-project-sampollack"
CREDENTIALS_FILE_KEY = "credentials/googledrivecredentials.json"
BRONZE_FOLDER = "bronze/"
GOOGLE_DRIVE_FOLDER_ID = "1EtyQg14b1P6EJ1JQ3UqeGTDwMxba17IQ"

s3 = boto3.client("s3")

print("Reading Google credentials from S3...")

response = s3.get_object(
Bucket=S3_BUCKET,
Key=CREDENTIALS_FILE_KEY
)

credentials_data = json.loads(
response["Body"].read().decode("utf-8")
)

credentials = service_account.Credentials.from_service_account_info(
credentials_data,
scopes=["https://www.googleapis.com/auth/drive.readonly"]
)

drive = build(
"drive",
"v3",
credentials=credentials,
cache_discovery=False
)

print("Connected to Google Drive successfully")

print("Searching for CSV files...")

files = []
page_token = None

while True:
    response = drive.files().list(
    q=f"'{GOOGLE_DRIVE_FOLDER_ID}' in parents and trashed = false",
    fields="nextPageToken, files(id, name)",
    pageSize=1000,
    pageToken=page_token,
    supportsAllDrives=True,
    includeItemsFromAllDrives=True
    ).execute()

    files.extend(
        file for file in response.get("files", [])
        if file["name"].lower().endswith(".csv")
    )
    
    page_token = response.get("nextPageToken")

    if not page_token:
        break

print(f"Found {len(files)} CSV files")

if not files:
    raise Exception("No CSV files found in Google Drive folder")
success_count = 0
failed_files = []

for file in files:

    file_name = file["name"]
    file_id = file["id"]
    
    print(f"Processing: {file_name}")
    
    try:
        # Download file from Google Drive
        file_buffer = io.BytesIO()
        request = drive.files().get_media(
            fileId=file_id,
            supportsAllDrives=True
        )
    
        downloader = MediaIoBaseDownload(file_buffer, request)
    
        done = False
    
        while not done:
            _, done = downloader.next_chunk()
    
        file_buffer.seek(0)
    
        # Upload file to S3 Bronze
        s3_key = f"{BRONZE_FOLDER}{file_name}"
    
        s3.upload_fileobj(
            file_buffer,
            S3_BUCKET,
            s3_key
        )
    
        print(f"Uploaded: s3://{S3_BUCKET}/{s3_key}")
    
        success_count += 1
    
    except Exception as e:
        print(f"Error uploading {file_name}: {e}")
        failed_files.append(file_name)

print("\n========== INGESTION SUMMARY ==========")
print(f"Total CSV Files: {len(files)}")
print(f"Successfully Uploaded: {success_count}")
print(f"Failed: {len(failed_files)}")
print("=======================================")

if failed_files:
    raise Exception(f"Failed files: {failed_files}")

print("Google Drive to S3 Bronze ingestion completed successfully!")