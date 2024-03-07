#!/usr/bin/env python3

# Path: py_add_fits_files_to_dio.py

# This script uses pyDataverse to upload files to Dataverse
# ----------------------------------------------

# This script iterates through each (originally only for fits) file in the specified directory, extracts the star number from
# the file name, and uses this information to construct a JSON payload. It then executes a curl command
# to upload each file to a specified Dataverse server using the provided API token and Persistent ID.
# The output of each curl command, along with relevant data, is logged to 'log.txt' for record-keeping
# and debugging purposes.

# Troubleshooting
# ---------------
# If you get the following error:
# SystemError: (libev) error creating signal/async pipe: Too many open files
# Then run the following command:
# ulimit -n 4096

# Fixes some issues with grequests and urllib3
import gevent.monkey
gevent.monkey.patch_all(thread=False, select=False)
import grequests
import requests
import urllib3

# Import required modules
import argparse
import json
import subprocess
import os
import re
import sys
import time
import urllib.parse
import urllib.request
from pathlib import Path
import pyDataverse.api
from dvuploader import DVUploader, File
from mimetype_description import guess_mime_type, get_mime_type_description
import hashlib
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import traceback
import re
import logging

parser = argparse.ArgumentParser()
parser.add_argument("-f", "--folder", help="The directory containing the FITS files.", required=True)
parser.add_argument("-t", "--token", help="API token for authentication.", required=True)
parser.add_argument("-p", "--persistent_id", help="Persistent ID for the dataset.", required=True)
parser.add_argument("-u", "--server_url", help="URL of the Dataverse server.", required=True)
parser.add_argument("-b", "--files_per_batch", help="Number of files to upload per batch.", required=False)
parser.add_argument("-l", "--directory_label", help="The directory label for the file.", required=False)
parser.add_argument("-d", "--description", help="The description for the file. {file_name_without_extension}", required=False)
parser.add_argument("-w", "--wipe", help="Wipe the file hashes json file.", action='store_true', required=False)
parser.add_argument("-n", "--hide", help="Hide the display progress.", action='store_false', required=False)

args = parser.parse_args()
if args.files_per_batch is None:
    FILES_PER_BATCH = 20
else:
    FILES_PER_BATCH = int(args.files_per_batch)

# directory_label = args.directory_label
if args.directory_label is None:
    FILE_DESCRIPTION_LABEL = ''
else:
    FILE_DESCRIPTION_LABEL = args.directory_label

if args.token == '':
    print("\n\n ❌ API token is empty.\n")
    sys.exit(1)

UPLOAD_DIRECTORY=args.folder
DATAVERSE_API_TOKEN=args.token
DATASET_PERSISTENT_ID=args.persistent_id
DATASET_ID=''
SERVER_URL=args.server_url
HIDE_DISPLAY=args.hide
WIPE_CACHE=args.wipe
ONLINE_FILE_DATA=[]
COMPILED_FILE_LIST_WITH_MIMETYPES = []
MODIFIED_DOI_STR = ''
NOT_ALL_FILES_ONLINE = True

# Configure logging
logging.basicConfig(filename='wait_for_200.log', level=logging.WARNING, format='%(asctime)s - %(levelname)s - %(message)s')

# Process SERVER_URL to ensure it has the correct protocol
if re.match("^http://", SERVER_URL):
    # Replace "http://" with "https://"
    SERVER_URL = re.sub("^http://", "https://", SERVER_URL)
elif not re.match("^https://", SERVER_URL):
    # Add "https://" if no protocol is specified
    SERVER_URL = "https://{}".format(SERVER_URL)

class File:
    """
    A class to represent a file.
    """
    def __init__(self, directoryLabel, filepath, description, mimeType):
        self.directoryLabel = directoryLabel
        self.filepath = filepath
        self.description = description
        self.mimeType = mimeType
    def __repr__(self):
        return f"File(directoryLabel='{self.directoryLabel}', filepath='{self.filepath}', description='{self.description}', mimeType='{self.mimeType}')"

class TimeoutHTTPAdapter(HTTPAdapter):
    def __init__(self, *args, **kwargs):
        self.timeout = kwargs.pop("timeout", None)
        super().__init__(*args, **kwargs)

    def send(self, request, **kwargs):
        timeout = kwargs.get("timeout")
        if timeout is None and self.timeout is not None:
            kwargs["timeout"] = self.timeout
        return super().send(request, **kwargs)

def requests_retry_session(
    retries=3,
    backoff_factor=0.3,
    status_forcelist=(500, 502, 504),
    session=None,
    timeout=None,
):
    session = session or requests.Session()
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
        allowed_methods=["HEAD", "GET", "PUT", "DELETE", "OPTIONS", "TRACE"]
    )
    adapter = TimeoutHTTPAdapter(max_retries=retry, timeout=timeout)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session

def fetch_data(url, type="GET"):
    """
    Fetch data from a given URL and return the JSON response.
    """
    headers = {
        "X-Dataverse-key": DATAVERSE_API_TOKEN
    }
    try:
        if type == "DELETE":
            response = requests_retry_session().delete(url, headers=headers)
        else:
            response = requests_retry_session().get(url, headers=headers)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        logging.error(f"Failed fetch_data() from {url}: {e}")
        return None

def sanitize_folder_path(folder_path):
    """
    Sanitize the folder path.
    """
    folder_path = folder_path.rstrip('/').lstrip('./').lstrip('/')
    sanitized_name = re.sub(r'[^\w\-\.]', '_', folder_path)
    return sanitized_name

def get_dataset_info():
    """
    Tests connection and fetches the dataset information.
    """
    api = pyDataverse.api.NativeApi(SERVER_URL, api_token=DATAVERSE_API_TOKEN)
    response = api.get_dataset(DATASET_PERSISTENT_ID)
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"Error retrieving dataset: {response.json()['message']}")
        logging.error(f"Error get_dataset_info() retrieving dataset: {response.json()['message']}")

def native_api_upload_file_using_request(files):
    """
    Uploads a list of files to a Dataverse dataset using the Native API.

    :param files: A list of dictionaries, each containing file metadata and path.
    """
    print("\nUploading files using the Native API...")
    print('-' * 40)
    # Base URL for dataset file upload using dataset ID
    url_dataset_id = f"{SERVER_URL}/api/datasets/{DATASET_ID}/add?key={DATAVERSE_API_TOKEN}"
    # Base URL for dataset file upload using persistent ID
    url_persistent_id = f"{SERVER_URL}/api/datasets/:persistentId/add?persistentId={DATASET_PERSISTENT_ID}&key={DATAVERSE_API_TOKEN}"
    for file_info in files:
        # Extract file metadata
        directory_label = file_info.get('directoryLabel', '')
        filepath = file_info.get('filepath')
        mime_type = file_info.get('mimeType', 'text/plain')
        description = file_info.get('description', '')
        hash = file_info.get('hash', '')
        if HIDE_DISPLAY:
            print('-' * 40)
            print(f"Uploading file: {filepath}")
            print(f"Directory label: {directory_label}")
            print(f"MIME type: {mime_type}")
            print(f"Description: {description}")
            print(f"Hash: {hash}")
            print("")
        # Prepare file for upload
        with open(filepath, 'rb') as f:
            file_content = f.read()
        # Check to see if file_content is empty
        if not file_content:
            print(f"File {filepath} is empty. Skipping...")
            logging.info(f"File {filepath} is empty. Skipping...")
            continue
        files_to_upload = {'file': (filepath.split('/')[-1], file_content, mime_type)}

        # Optional description and file tags
        params = {
            'description': description,
            'categories': [],
            'tabIngest': 'false',
            'restrict': 'false'
        }
        params_as_json_string = json.dumps(params)
        payload = {'jsonData': params_as_json_string}

        # Choose URL based on whether you're using dataset ID or persistent ID
        upload_url = url_dataset_id if DATASET_ID else url_persistent_id
        if HIDE_DISPLAY:
            print(f"Making request: {upload_url}")
        r = requests.post(upload_url, data=payload, files=files_to_upload)

        # Add a retry if the status code is not 200
        while r.status_code != 200:
            print(f"Something went wrong. Retrying... {r.status_code}")
            wait_for_200(upload_url, file_number_it_last_completed=0, timeout=600, interval=10)
            r = requests.post(upload_url, data=payload, files=files_to_upload)
        try:
            response_json = r.json()
            print(response_json)
        except json.JSONDecodeError:
            print("Response is not in JSON format.")
            logging.info(f"Response is not in JSON format: {r.text}")
        except Exception as e:
            print(f"An error occurred: {e}")
            logging.error(f"Error native_api_upload_file_using_request(): An error occurred: {e}")

def s3_direct_upload_file_using_curl(files):
    """
    Upload files to a Dataverse dataset using curl for S3 direct upload.

    Args:
    - files (list of dicts): List containing file metadata and paths.
    """
    for file_info in files:
        # Extract file details
        directory_label = file_info.get('directoryLabel')
        filepath = file_info.get('filepath')
        mime_type = file_info.get('mimeType')
        description = file_info.get('description')
        size = os.path.getsize(filepath)
        # Execute the curl command
        try:
            curl_command_for_file_url = f"curl -H 'X-Dataverse-key:{DATAVERSE_API_TOKEN}' '{SERVER_URL}/api/datasets/:persistentId/uploadurls?persistentId={DATASET_PERSISTENT_ID}&size={size}'"
            upload_to_tmp_output = subprocess.check_output(curl_command_for_file_url, shell=True, text=True)
            data = json.loads(upload_to_tmp_output)
            # Extract the "storageIdentifier", "partSize", and "url" values
            storage_identifier = data['data']['storageIdentifier']
            part_size = data['data']['partSize']
            url = data['data']['url']
            # Execute the curl command and capture the output
            upload_into_s3_url = subprocess.check_output(
                f"curl -i -H 'x-amz-tagging:dv-state=temp' -X PUT -T {filepath} '{url}'",
                shell=True,
                text=True
            )
            # Initialize variables for the values we want to extract
            x_amz_request_id = None
            e_tag = None
            # Split the upload_into_s3_url by new lines and iterate through it to find the desired headers
            for line in upload_into_s3_url.split('\n'):
                if line.startswith('x-amz-request-id:'):
                    x_amz_request_id = line.split(':', 1)[1].strip()
                elif line.startswith('ETag:'):
                    e_tag = line.split(':', 1)[1].strip()
            # Now x_amz_request_id and e_tag variables hold the extracted values
            file_hash = f"{e_tag}"
            # Construct the JSON payload
            payload = {
                'description': f"{description}",
                'directoryLabel': f"{directory_label}",
                'mimeType': f"{mime_type}",
                'contentType': f"{mime_type}",
                'storageIdentifier': f"{storage_identifier}",
                'restrict': 'false',
                'fileName': os.path.basename(filepath),
                'fileSystemName': os.path.basename(filepath),
                'checksum': {'@type': 'MD5', '@value': f"{file_hash}"},
                'categories': [],
                'restrict': False
            }
            payload_str = json.dumps(payload)
            register_files_command = f"curl -X POST -H 'X-Dataverse-key: {DATAVERSE_API_TOKEN}' '{SERVER_URL}/api/datasets/:persistentId/add?persistentId={DATASET_PERSISTENT_ID}' -F 'jsonData={payload_str}'"
            register_files = subprocess.check_output(register_files_command, shell=True, text=True)
            if not HIDE_DISPLAY:
                print("curl_command_for_file_url")
                print(curl_command_for_file_url)
                print(f"storageIdentifier: {storage_identifier}")
                print(f"partSize: {part_size}")
                print(f"url: {url}")
                print(f"curl -i -H 'x-amz-tagging:dv-state=temp' -X PUT -T {filepath} '{url}'")
                print("upload_into_s3_url")
                print(upload_into_s3_url)
                print(f"x-amz-request-id: {x_amz_request_id}")
                print(f"ETag/File Hash: {e_tag}")
                print("payload")
                print(payload_str)
                print(register_files_command)
                print(register_files)
        # If exception is a timeout error, try again.
        except subprocess.TimeoutExpired:
            print(f"Failed to upload file: {filepath} because of timeout. Error: {e.output}")
            logging.info(f"Failed to upload file: {filepath} because of timeout. Error: {e.output}")
        except subprocess.CalledProcessError as e:
            print(f"Failed to upload file: {filepath}. Error: {e.output}")
            logging.info(f"Failed to upload file: {filepath}. Error: {e.output}")
            exit(1)

def upload_file_using_pyDataverse(files):
    """
    Upload files to a Dataverse dataset.

    Args:
    - files (list of dicts): List containing file metadata and paths.
    """
    # Initialize the Dataverse API
    api = pyDataverse.api.NativeApi(SERVER_URL, DATAVERSE_API_TOKEN)
    data_access_api = pyDataverse.api.DataAccessApi(SERVER_URL, DATAVERSE_API_TOKEN)

    for file_info in files:
        # Extract file details
        directory_label = file_info.get('directoryLabel')
        filepath = file_info.get('filepath')
        mime_type = file_info.get('mimeType')
        description = file_info.get('description')
        # Note: 'hash' is not used directly in the upload, but could be part of a validation step

        # Prepare the metadata for the file
        file_metadata = {
            'description': description,
            'directoryLabel': directory_label,
            'categories': [],
            'restrict': False
        }

        # Convert metadata to JSON format as required by the API
        file_metadata_json = json.dumps(file_metadata)

        # Upload the file
        resp = api.upload_datafile(DATASET_PERSISTENT_ID, filepath, file_metadata_json, mime_type)

        if resp.status_code == 200:
            print(f"File uploaded successfully: {filepath}")
        else:
            print(f"Failed to upload file: {filepath}. Response: {resp.text}")
            logging.info(f"Failed to upload file: {filepath}. Response: {resp.text}")

def populate_online_file_data(json_file_path):
    global ONLINE_FILE_DATA
    try:
        with open(json_file_path, 'r') as file:
            ONLINE_FILE_DATA = json.load(file)
    except FileNotFoundError:
        print(f"File {json_file_path} not found. Ensure the path is correct.")
        logging.error(f"Error populate_online_file_data() File {json_file_path} not found. Ensure the path is correct.")
    except json.JSONDecodeError:
        print(f"Error decoding JSON from {json_file_path}. Ensure the file contains valid JSON.")
        logging.error(f"Error populate_online_file_data() Error decoding JSON from {json_file_path}. Ensure the file contains valid JSON.")

def hash_file(file_path, hash_algo="md5"):
    hash_func = getattr(hashlib, hash_algo)()
    with open(file_path, 'rb') as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_func.update(chunk)
    return hash_func.hexdigest()

def is_file_online(file_hash):
    return file_hash in ONLINE_FILE_DATA

def does_file_exist_and_content_isnt_empty(file_path):
    """
    Check if the file is empty or contains only brackets or doesn't exist.
    """
    print(f"Checking if {file_path} is empty...")
    # if not os.path.isfile(local_files_filename)
    try:
        with open(file_path, 'r') as file:
            content = file.read().strip()
            if content in ["", "[]", "{}"]:
                print("File is empty or contains only brackets.")
                return False
            else:
                print(f" ✓ File is not empty.\n\n")
                return True
    except FileNotFoundError:
        print("File not found.")
        return False

def get_files_with_hashes_list():
    """
    Get a list of files with hashes from DOI.
    """
    file_hashes_exist = does_file_exist_and_content_isnt_empty(LOCAL_JSON_FILE_WITH_LOCAL_FS_HASHES)
    print(f"Checking if any of the hashes exist online: {file_hashes_exist} ...")
    try:
        if not file_hashes_exist:
            print(f"File {LOCAL_JSON_FILE_WITH_LOCAL_FS_HASHES} does not exist or is empty.")
            contents = os.listdir(NORMALIZED_FOLDER_PATH)
            if os.path.isfile(LOCAL_FILE_LIST_STORED):
                file_paths_unsorted = []
                with open(LOCAL_FILE_LIST_STORED) as f:
                    file_paths_unsorted = f.readlines()
                file_paths_unsorted = [x.strip() for x in file_paths_unsorted]
                print(f"Found {len(file_paths_unsorted)} files in {NORMALIZED_FOLDER_PATH}")
            else:
                file_paths_unsorted = [
                    os.path.join(NORMALIZED_FOLDER_PATH, filename) for filename in contents
                    if not filename.startswith(".") and os.path.isfile(os.path.join(NORMALIZED_FOLDER_PATH, filename))
                ]
                with open(LOCAL_FILE_LIST_STORED, 'w') as f:
                    for file_path in file_paths_unsorted:
                        f.write("%s\n" % file_path)
        else:
            print(f"Reading file paths from {LOCAL_FILE_LIST_STORED}...")
            file_paths_unsorted = []
            with open(LOCAL_FILE_LIST_STORED) as f:
                file_paths_unsorted = f.readlines()
            file_paths_unsorted = [x.strip() for x in file_paths_unsorted]
    except Exception as e:
        print(f"An error occurred: {e}")
    file_paths = sorted(file_paths_unsorted, reverse=True)
    print(f"Found {len(file_paths)} files in {NORMALIZED_FOLDER_PATH}")
    if file_paths == []:
        print(f"No files in {NORMALIZED_FOLDER_PATH}")
        sys.exit(1)
    # If the file_hashes.json file is missing or is empty then hash the files and write the hashes to the file_hashes.json file.
    results = {}
    if not file_hashes_exist:
        print("Calculating hashes...")
        for file_path in file_paths:
            file_path, file_hash = hash_file(file_path)
            if HIDE_DISPLAY:
                print(f" Hashing file {file_path}... ", end="\r")
            while file_hash is None:
                print(f" File {file_path} is empty. Trying again... ", end="\r")
                hash_file(file_path)
                file_path, file_hash = hash_file(file_path)
            results[file_path] = file_hash
        print("")
        print(f"Writing hashes to {LOCAL_JSON_FILE_WITH_LOCAL_FS_HASHES}...")
        with open(LOCAL_JSON_FILE_WITH_LOCAL_FS_HASHES, 'w') as f:
            json.dump(results, f, indent=4)
    else:
        print(f"Reading hashes from {LOCAL_JSON_FILE_WITH_LOCAL_FS_HASHES}...")
        with open(LOCAL_JSON_FILE_WITH_LOCAL_FS_HASHES) as local_json_file_with_local_fs_hashes_file:
            local_json_file_data = json.load(local_json_file_with_local_fs_hashes_file)
            # Check that local_json_file_data has items and is not empty.
            if isinstance(local_json_file_data, dict):
                existing_results = list(local_json_file_data.items())
                for file_path, file_hash in existing_results:
                    results[file_path] = file_hash
            else:
                existing_results = local_json_file_data
            for file_path, file_hash in existing_results:
                results[file_path] = file_hash
        print("")
    print(f"Found hashing for all {len(results)} files.")
    return results

def set_files_and_mimetype_to_exported_file(results):
    print("\nSetting file definitions with mimetypes & metadata together...")
    print('-' * 40)
    print("This might take a while...")
    if os.path.exists(LOCAL_FILE_DICT_STORED):
        print("Loading files from stored file...")
        with open(LOCAL_FILE_DICT_STORED, 'r') as infile:
            files = json.load(infile)
    else:
        files = []

        if isinstance(results, dict):
            results = list(results.items())

        for file_path, file_hash in results:
            if HIDE_DISPLAY:
                print(f" Setting file {file_path}... ", end="\r")
            if not file_hash or not file_path:
                continue
            filename = os.path.basename(file_path)
            mimeType = guess_mime_type(os.path.join(UPLOAD_DIRECTORY, file_path))
            if mimeType == "None" or type(mimeType) == type(None) or mimeType == "":
                # Start with setting the default to a binary file and change as needed.
                mimeType = "application/octet-stream"
            if filename.endswith(".shp"):
                mimeType = "application/octet-stream"
            if filename.endswith(".shp.xml"):
                mimeType = "application/fgdc+xml"
            if filename.endswith(".dbf"):
                mimeType = "application/x-dbf"
            if filename.endswith(".shx"):
                mimeType = "application/octet-stream"
            if filename.endswith(".prj"):
                mimeType = "text/plain"
            if filename.endswith(".cpg"):
                mimeType = "text/plain"
            if filename.endswith(".sbn"):
                mimeType = "application/octet-stream"
            if filename.endswith(".sbx"):
                mimeType = "application/octet-stream"
            if filename.endswith(".fbn"):
                mimeType = "application/octet-stream"
            if filename.endswith(".fbx"):
                mimeType = "application/octet-stream"
            if filename.endswith(".ain"):
                mimeType = "application/octet-stream"
            if filename.endswith(".aih"):
                mimeType = "application/octet-stream"
            if filename.endswith(".ixs"):
                mimeType = "application/octet-stream"
            if filename.endswith(".mxs"):
                mimeType = "application/octet-stream"
            if filename.endswith(".atx"):
                mimeType = "application/xml"
            if filename.endswith(".qix"):
                mimeType = "x-gis/x-shapefile"
            if mimeType == "application/fits":
                mimeType = "image/fits"
            description = f"Posterior distributions of the stellar parameters for the star with ID from the Gaia DR3 catalog {os.path.splitext(filename)[0]}."
            file_dict = {
                "directoryLabel": FILE_DESCRIPTION_LABEL,
                "filepath": file_path,
                "mimeType": mimeType,
                "description": description,
                "hash": file_hash
            }
            files.append(file_dict)

        # Save the generated files array to LOCAL_FILE_DICT_STORED
        with open(LOCAL_FILE_DICT_STORED, 'w') as outfile:
            json.dump(files, outfile)
        print("Files definitions saved.")
    print("set_files_and_mimetype_to_exported_file complete")
    print('-' * 40)
    print("")
    return files

def upload_file_with_dvuploader(upload_files, loop_number=0):
    """
    Upload files with dvuploader.
    """
    print("Uploading files...")
    try:
        dvuploader = DVUploader(files=upload_files)
        dvuploader_status = dvuploader.upload(
            api_token=DATAVERSE_API_TOKEN,
            dataverse_url=SERVER_URL,
            persistent_id=DATASET_PERSISTENT_ID,
        )
    except Exception as e:
        print(f"An error occurred with uploading: {e}")
        print('Upload_file Step: trying again in 10 seconds...')
        time.sleep(10)
        logging.info(f"upload_file_with_dvuploader: An error occurred with uploading retry Number{loop_number}: {e}")
        upload_file_with_dvuploader(upload_files, loop_number=loop_number+1)
    return True

def get_count_of_the_doi_files_online():
    return len(get_list_of_the_doi_files_online())

def check_dataset_is_unlocked():
    """
    Checks for any locks on the dataset and attempts to unlock if locked.
    """
    lock_url = f"{SERVER_URL}/api/datasets/{DATASET_ID}/locks"
    print(f"{SERVER_URL}/api/datasets/{DATASET_ID}/locks")
    print('-' * 40)
    while True:
        dataset_locks = fetch_data(lock_url)
        # Check if dataset_locks is None or if 'data' key is not present
        if dataset_locks is None or 'data' not in dataset_locks:
            print('Failed to fetch dataset locks or no data available.')
            break
        if dataset_locks['data'] == []:
            print('Dataset is not locked...')
            break
        else:
            print('dataset_locks: ', dataset_locks)
            # unlock_response = fetch_data(lock_url, type="DELETE")
            # print('unlock_response: ', unlock_response)
            print('Dataset is locked. Waiting 2 seconds...')
            time.sleep(2)
            print('Trying again...')

def wait_for_200(url, file_number_it_last_completed, timeout=60, interval=5, max_attempts=None):
    """
    Check a URL repeatedly until a 200 status code is returned.

    Parameters:
    - url: The URL to check.
    - timeout: The maximum time to wait for a 200 response, in seconds.
    - interval: The time to wait between checks, in seconds.
    - max_attempts: The maximum number of attempts to check the URL (None for unlimited).
    """
    start_time = time.time()
    attempts = 0

    while True:
        date_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        try:
            response = requests.get(url)
            if response.status_code == 200:
                logging.info(f"Success: Received 200 status code from {url}")
                print(f"{date_time} Success: Received 200 status code from {url}")
                return True
            elif response.status_code == 403 or "User :guest is not permitted to perform requested action." in response.text:
                # Check for specific error message indicating an invalid API token
                logging.error(f"{date_time} Error: The API token is either empty or isn't valid.")
                print(f"{date_time} Error: The API token is either empty or isn't valid.")
                return False
            else:
                message = f" {date_time} Warning: Received {response.status_code} status code from {url}. Retrying..."
                print(message, end="\r")
                logging.warning(message)
        except requests.RequestException as e:
            message = f" {date_time} An error occurred in wait_for_200(): Request failed: {e}, logging and retrying..."
            print(message, end="\r")
            logging.error(message)
        attempts += 1
        if max_attempts is not None and attempts >= max_attempts:
            message = f" {date_time} An error occurred in wait_for_200(): Reached the maximum number of attempts ({max_attempts}) without success."
            print(message)
            logging.error(message)
            return False

        elapsed_time = time.time() - start_time
        if elapsed_time + interval > timeout:
            message = f"An error occurred in wait_for_200(): Timeout reached ({timeout} seconds) without success."
            print(message)
            logging.error(message)
            return False
        time.sleep(interval)

def prepare_files_for_upload():
    # Extract MD5 hashes from the online files JSON
    online_hashes = {file['md5'] for file in ONLINE_FILE_DATA}

    # List to store paths of files not found online
    files_not_online = []
    for file_info in COMPILED_FILE_LIST_WITH_MIMETYPES:
        file_path = file_info['filepath']
        file_hash = file_info['hash']
        if file_hash not in online_hashes:
            files_not_online.append(file_info)
    print("")
    if not files_not_online:
        print("All files are already online.")
    else:
        print(f"Found {len(files_not_online)} files not online.")
    return files_not_online

def main(loop_number=0, start_time=None, time_per_batch=None, staring_file_number=0):
    global MODIFIED_DOI_STR # Global variable to track modified DOI string.

    try:
        # Initialize start time and time_per_batch if not provided.
        if start_time is None:
            start_time = time.time() # Capture the start time of the operation.
        if time_per_batch is None:
            time_per_batch = [] # Track time taken for each batch to upload.

        # Prepare the list of files to be uploaded.
        compiled_files_to_upload = prepare_files_for_upload()
        total_files = len(compiled_files_to_upload) # Total number of files prepared for upload.
        restart_number = staring_file_number # Starting index for file upload in case of a restart.

        # Print the total number of files to upload.
        print(f"Total files to upload: {total_files}")

        # Ensure the dataset is not locked before starting the upload process.
        check_dataset_is_unlocked()

        # Exit if there are no files to upload.
        if compiled_files_to_upload == []:
            print("All files are already online.")
            return

        # Iterate over files in batches for upload.
        for i in range(restart_number, len(compiled_files_to_upload), FILES_PER_BATCH):
            batch_start_time = time.time()
            if i + FILES_PER_BATCH > len(compiled_files_to_upload):
                files = compiled_files_to_upload[i:]
            else:
                files = compiled_files_to_upload[i:i+FILES_PER_BATCH]
            print(f"Uploading files {i} to {i+FILES_PER_BATCH}... {len(compiled_files_to_upload) - i - FILES_PER_BATCH}")

            # Ensure the Dataverse server is ready before uploading.
            wait_for_200(f'{SERVER_URL}/dataverse/root', file_number_it_last_completed=i, timeout=600, interval=10)

            # Retrieve the initial count of DOI files online for comparison after upload.
            original_count = get_count_of_the_doi_files_online()

            # Verify the dataset is unlocked before proceeding otherwise wait for it to be unlocked.
            check_dataset_is_unlocked()

            # Choose the desired upload method. Uncomment the method you wish to use.
            # upload_file_using_pyDataverse(files)
            # s3_direct_upload_file_using_curl(files)
            # native_api_upload_file_using_request(files)
            upload_file_with_dvuploader(files, 0)

            batch_end_time = time.time()
            time_per_batch.append(batch_end_time - batch_start_time)
            average_time_per_batch = sum(time_per_batch) / len(time_per_batch)
            batches_left = (total_files - i) / FILES_PER_BATCH
            estimated_time_left = batches_left * average_time_per_batch
            hours, remainder = divmod(estimated_time_left, 3600)
            minutes, _ = divmod(remainder, 60)
            print(f"Uploading files {i} to {i+FILES_PER_BATCH}... {total_files - i - FILES_PER_BATCH} files left to upload. Estimated time remaining: {int(hours)} hours and {int(minutes)} minutes.")

            restart_number = i  # Update restart_number in case of a need to restart.
            # How Many files were uploaded
            new_count = get_count_of_the_doi_files_online()
            # If the new count is the same as the original count then the files were not uploaded.
            if new_count == original_count:
                print(f"Files {i} to {i+FILES_PER_BATCH} were not uploaded. Trying again in 5 seconds...")
                time.sleep(5)
                main(loop_number=loop_number+1, start_time=start_time, time_per_batch=time_per_batch, staring_file_number=restart_number)
    except json.JSONDecodeError as json_err:
        error_context="An unexpected error occurred in Main(): Error parsing JSON data. Check the logs for more details."
        print(f"{error_context} {json_err}")
        logging.error(f"An unexpected error occurred in Main(): {error_context}: {json_err}")
    except Exception as e:
        error_traceback = traceback.format_exc()
        logging.error(f"An unexpected error occurred in Main(): {e}\n{error_traceback}")
        print("An unexpected error occurred. Check the logs for more details.")
        traceback.print_exc()
        time.sleep(5)
        if loop_number > 5:
            time.sleep(5)
            print('Loop number is greater than 5. Exiting program.')
            sys.exit(1)
        main(loop_number=loop_number+1, start_time=start_time, time_per_batch=time_per_batch, staring_file_number=restart_number)

def wipe_report():
    """
    Wipe the file_hashes.json file.
    """
    if os.path.isfile(LOCAL_JSON_FILE_WITH_LOCAL_FS_HASHES):
        with open(LOCAL_JSON_FILE_WITH_LOCAL_FS_HASHES, 'w') as outfile:
            json.dump([], outfile)
    if os.path.isfile(MODIFIED_DOI_STR):
        with open(MODIFIED_DOI_STR, 'w') as second_outfile:
            json.dump([], second_outfile)
    if os.path.isfile(LOCAL_FILE_LIST_STORED):
        os.remove(LOCAL_FILE_LIST_STORED)
    if os.path.isfile(LOCAL_FILE_DICT_STORED):
        os.remove(LOCAL_FILE_DICT_STORED)

def get_list_of_the_doi_files_online():
    """
    Get a list of files with hashes that are already online.
    """
    global ONLINE_FILE_DATA
    headers = {
        "X-Dataverse-key": DATAVERSE_API_TOKEN
    }
    print(f"\nRe-fetching updated list of online files for this {DATASET_PERSISTENT_ID}...")
    first_url_call = f"{SERVER_URL}/api/datasets/:persistentId/?persistentId={DATASET_PERSISTENT_ID}"
    data = fetch_data(first_url_call)

    if data is None or 'status' in data and data['status'] == 'ERROR' and 'message' in data and data['message'] == 'Bad api key ':
        print('Bad api key. Exiting program.')
        sys.exit(1)

    url_to_get_online_file_list = f"{SERVER_URL}/api/datasets/{DATASET_ID}/versions/:draft/files"
    # Request the list of files for this DOI
    full_data = fetch_data(url_to_get_online_file_list)

    while full_data is None or 'data' not in full_data:
        print(f'Failed to fetch the list of files for {DATASET_PERSISTENT_ID}. Trying again in 5 seconds...')
        wait_for_200(f"{SERVER_URL}/dataverse/root", file_number_it_last_completed=0, timeout=300, interval=10)
        full_data = fetch_data(url_to_get_online_file_list)
        time.sleep(5)

    files_online_for_this_doi = []
    for file in full_data['data']:
        files_online_for_this_doi.append(file['dataFile'])
    print(f"Found {len(files_online_for_this_doi)} files for this DOI online.")
    print("")
    print("Writing the list of files to file_hashes.json...")
    with open(MODIFIED_DOI_STR, 'w') as outfile:
        json.dump(files_online_for_this_doi, outfile)
        ONLINE_FILE_DATA = files_online_for_this_doi
    return files_online_for_this_doi

def check_all_local_hashes_that_are_online():
    global ONLINE_FILE_DATA
    print("Checking if all files are online...")
    # If the LOCAL_JSON_FILE_WITH_LOCAL_FS_HASHES is empty then run get_files_with_hashes_list() to create the file_hashes.json file
    if not does_file_exist_and_content_isnt_empty(LOCAL_JSON_FILE_WITH_LOCAL_FS_HASHES):
        check_list_data = get_files_with_hashes_list()
    else:
        with open(LOCAL_JSON_FILE_WITH_LOCAL_FS_HASHES, 'r') as local_json_file_with_local_fs_hashes_file:
            # Create check_list_data from the file_hashes.json file to grab with check_list_data.items().
            check_list_data = json.load(local_json_file_with_local_fs_hashes_file)
    missing_files = prepare_files_for_upload()
    if missing_files != []:
        print(f"Found {len(missing_files)} files locally.")
        return missing_files
    return False

def has_read_access(directory):
    """
    Check if the directory has read access.
    """
    return os.access(directory, os.R_OK)

def is_directory_empty(directory):
    """
    Check if a directory is empty or not.
    """
    try:
        with os.scandir(directory) as it:
            next(it)
            return False
    except StopIteration:
        return True

def cleanup_storage():
    # https://guides.dataverse.org/en/latest/api/native-api.html#cleanup-storage-of-a-dataset
    dryrun_url = f"{SERVER_URL}/api/datasets/:persistentId/cleanStorage?persistentId={DATASET_PERSISTENT_ID}&dryrun=true"
    headers = {"X-Dataverse-key": DATAVERSE_API_TOKEN}

    # Initial dry run to get the list of files
    response = requests.get(dryrun_url, headers=headers)
    if response.status_code == 200:
        response_data = response.json()
        if 'data' in response_data and 'message' in response_data['data']:
            message = response_data['data']['message']
            # Split the message to extract the "Found" and "Deleted" parts
            found_items = [item.strip() for item in message.split('\nDeleted:')[0].replace('Found: ', '').split(',') if item.strip()]
            deleted_items = [item.strip() for item in message.split('\nDeleted:')[1].split(',') if item.strip()] if '\nDeleted:' in message else []
            found_count = len(found_items)
            deleted_count = len(deleted_items)

            print(f"Files registered: {found_count}")
            print(f"Files not registered and to be cleaned up: {deleted_count}")

            # Bypass the prompt if there are no files to delete
            if deleted_count == 0:
                print("No files to clean up. Exiting.")
                return

            # If there are files to clean up, prompt the user for confirmation
            user_input = input(f"Proceed with cleanup of {deleted_count} files? [y/N]: ").strip().lower()
            if user_input == 'y':
                cleanup_url = f"{SERVER_URL}/api/datasets/:persistentId/cleanStorage?persistentId={DATASET_PERSISTENT_ID}&dryrun=false"
                cleanup_response = requests.get(cleanup_url, headers=headers)
                print(f"Cleaning up {deleted_count} files...")
                if cleanup_response.status_code == 200:
                    print("Cleanup successful.")
                    print(cleanup_response.json())
                else:
                    print("Cleanup failed.")
            else:
                print("Cleanup bypassed.")
        else:
            print("Unexpected response format.")
    else:
        print("Failed to retrieve the list of files for cleanup.")

if __name__ == "__main__":
    NORMALIZED_FOLDER_PATH = os.path.normpath(UPLOAD_DIRECTORY)
    SANITIZED_FILENAME = sanitize_folder_path(os.path.abspath(UPLOAD_DIRECTORY))
    LOCAL_JSON_FILE_WITH_LOCAL_FS_HASHES = os.getcwd() + '/' + SANITIZED_FILENAME + '.json'
    LOCAL_FILE_LIST_STORED = os.getcwd() + '/' + SANITIZED_FILENAME + '_file_list.txt'
    LOCAL_FILE_DICT_STORED = os.getcwd() + '/' + SANITIZED_FILENAME + '_file_dict.txt'
    original_doi_str = DATASET_PERSISTENT_ID
    MODIFIED_DOI_STR = ''.join(['_' if not c.isalnum() else c for c in original_doi_str]) + '.json'

    if HIDE_DISPLAY:
        print("\n👀 - The option to hide hashing progress is not enabled. Hashing progress will be displayed on the screen.\n")
    else:
        print("\n🚫 - The option to hide hashing progress is enabled. Hashing progress will not be displayed on the screen.\n")

    print(f"🔍 - Verifying the existence of the folder: {UPLOAD_DIRECTORY}...")
    if has_read_access(UPLOAD_DIRECTORY):
        print(f" ✓ The user has read access to the folder: {UPLOAD_DIRECTORY}\n")
    else:
        print(f" ❌ - The user does not have read access to the folder: {UPLOAD_DIRECTORY}\n\n")
        sys.exit(1)

    print(f"📁 Checking if the folder: {UPLOAD_DIRECTORY} is empty...")
    if is_directory_empty(UPLOAD_DIRECTORY):
        print(f" ❌ - The folder: {UPLOAD_DIRECTORY} is empty\n\n")
        sys.exit(1)
    else:
        print(f" ✓ The folder: {UPLOAD_DIRECTORY} is not empty\n")

    if WIPE_CACHE and not os.path.isfile(LOCAL_JSON_FILE_WITH_LOCAL_FS_HASHES):
        print(f"🧹 - Wiping the {LOCAL_JSON_FILE_WITH_LOCAL_FS_HASHES} file ...\n")
        wipe_report()
        print("")
    else:
        print(f"📖 - Reading hashes from {LOCAL_JSON_FILE_WITH_LOCAL_FS_HASHES} ...\n")

    if FILES_PER_BATCH != 20:
        print("⚠️ - Bigger batch sizes does not mean faster upload times. It is recommended to keep the batch size at 20. This is intended for fine tuning.\n")

    print(f"🔍 - Checking to see if {SERVER_URL}/dataverse/root is up...")
    wait_for_200(f"{SERVER_URL}/dataverse/root", file_number_it_last_completed=0, timeout=300, interval=10)
    print(" ✓ 200 status received.\n\n🔍 - Get the dataset id...")
    DATASET_INFO = get_dataset_info()
    DATASET_ID = DATASET_INFO["data"]["id"]
    print(f" 🆔 - Dataset ID: {DATASET_ID}\n\n")
    get_list_of_the_doi_files_online()
    populate_online_file_data(MODIFIED_DOI_STR)
    local_fs_files_array = get_files_with_hashes_list()
    COMPILED_FILE_LIST_WITH_MIMETYPES = set_files_and_mimetype_to_exported_file(local_fs_files_array)
    cleanup_storage()

    while NOT_ALL_FILES_ONLINE is not False:
        print("🔄 - Checking if all files are online and running the file batch size of {}...".format(FILES_PER_BATCH))
        print("🚀 - Identified that not all files were uploaded. Starting the upload process...\n")
        main()
        time.sleep(5)
        if check_all_local_hashes_that_are_online() is False:
            NOT_ALL_FILES_ONLINE = True

    print("\n\nDone.\n\n")