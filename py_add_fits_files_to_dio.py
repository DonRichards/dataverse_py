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

UPLOAD_DIRECTORY=args.folder
DATAVERSE_API_TOKEN=args.token
DATASET_PERSISTENT_ID=args.persistent_id
SERVER_URL=args.server_url
HIDE_DISPLAY=args.hide
WIPE_CACHE=args.wipe
ONLINE_FILE_DATA=[]

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

def hash_file(file_path, hash_algo="md5"):
    """
    Hash a single file and return the hash.
    """
    hash_func = getattr(hashlib, hash_algo)()
    try:
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_func.update(chunk)
    except IOError:
        return file_path, None
    return file_path, hash_func.hexdigest()

def does_file_exist_and_content_isnt_empty(file_path):
    """
    Check if the file is empty or contains only brackets or doesn't exist.
    """
    print(f"Checking if {file_path} is empty...")
    try:
        with open(file_path, 'r') as file:
            content = file.read().strip()
            # if the file is empty or contains only brackets then return false else return true
            if content == "" or content == "[]" or content == "{}":
                return False
            print(f"File {file_path} is not empty.")
            return True
    except FileNotFoundError:
        print("File not found.")
        return False

def get_files_with_hashes_list():
    """
    Get a list of files with hashes from DOI.
    """
    file_hashes_exist = does_file_exist_and_content_isnt_empty(LOCAL_JSON_FILE_WITH_LOCAL_FS_HASHES)
    online_file_count = len(get_list_of_the_doi_files_online())
    print(f"Checking if hashes exist: {file_hashes_exist} ...")
    print(f"Number of files found online {online_file_count}")
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
    if not file_hashes_exist:
        print("Calculating hashes...")
        results = {}
        for file_path in file_paths:
            file_path, file_hash = hash_file(file_path)
            if HIDE_DISPLAY:
                print(f" Hashing file {file_path}... ", end="\r")
            while file_hash is None:
                print(f" File {file_path} is empty. Trying again... ", end="\r")
                hash_file(file_path)
                file_path, file_hash = hash_file(file_path)
            # If file_hash exist in the online list of hashes or if the number of online hashes are zero then skip.
            if online_file_count > 0:
                if check_if_hash_is_online(file_hash):
                    if HIDE_DISPLAY:
                        print(f" No file_hashes_exist: File with hash {file_hash} is already online. Skipping...", end="\r")
                    continue
                else:
                    if HIDE_DISPLAY:
                        print(f" No file_hashes_exist: File with hash {file_hash} is not online. Adding file to the list...", end="\r")
            results[file_path] = file_hash
        print("")
        print(f"Writing hashes to {LOCAL_JSON_FILE_WITH_LOCAL_FS_HASHES}...")
        with open(LOCAL_JSON_FILE_WITH_LOCAL_FS_HASHES, 'w') as f:
            json.dump(results, f, indent=4)
    else:
        print(f"Reading hashes from {LOCAL_JSON_FILE_WITH_LOCAL_FS_HASHES}...")
        with open(LOCAL_JSON_FILE_WITH_LOCAL_FS_HASHES) as json_file:
            data = json.load(json_file)
            existing_results = list(data.items())
            items_left = len(existing_results)
            results = {}
            for file_path, file_hash in existing_results:
                if online_file_count > 0:
                    if check_if_hash_is_online(file_hash):
                        if HIDE_DISPLAY:
                            print(f" file_hashes_exist: File with hash {file_hash} is already online. Skipping...", end="\r")
                        continue
                    else:
                        if HIDE_DISPLAY:
                            print(f" No file_hashes_exist: File with hash {file_hash} is not online. Adding file to the list...", end="\r")
                results[file_path] = file_hash
        print("")
    print(f"Found hashing {len(results)} files not uploaded to DOI yet.")
    return results

def set_files_and_mimetype_to_exported_file(results):
    print("Setting file definitions with mimetypes & metadata together...")
    print("This will likely take a while.")
    files = []

    # Odd bug on first load.
    if isinstance(results, dict):
        results = list(results.items())

    for file_path, file_hash in results:
        if HIDE_DISPLAY:
            print(f" Setting file {file_path}... ", end="\r")
        if file_hash is None or file_hash == "":
            print(f" Hash for file {file_path} is empty... ", end="\r")
            continue
        if file_path is None or file_path == "":
            print(f" File path for file {file_path} is empty... ", end="\r")
            continue
        mimeType = guess_mime_type(os.path.join(UPLOAD_DIRECTORY, file_path))
        if mimeType == "application/fits":
            mimeType = "image/fits"

        filename = os.path.basename(file_path)
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
        file_name_without_extension = os.path.splitext(os.path.basename(file_path))[0]
        directory_label = ""
        description = f"Posterior distributions of the stellar parameters for the star with ID from the Gaia DR3 catalog {file_name_without_extension}."
        file_dict = {
            "directoryLabel": directory_label,
            "filepath": file_path,
            "mimeType": mimeType,
            "description": description,
            "hash": file_hash
        }
        files.append(file_dict)
    print("")
    return files

def upload_file(upload_files, loop_number=0):
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
        if loop_number > 5:
            print('Loop number is greater than 5. Exiting program.')
        upload_file(upload_files, loop_number=loop_number+1)
    return True

def check_if_hash_is_online(file_hash):
    """
    Checks if the file hash is already online.
    """
    global ONLINE_FILE_DATA
    for file in ONLINE_FILE_DATA:
        if file_hash in file.values():
            return True
    return False

def check_and_unlock_dataset():
    """
    Checks for any locks on the dataset and attempts to unlock if locked.
    """
    headers = {
        "X-Dataverse-key": DATAVERSE_API_TOKEN
    }
    lock_url = f"{SERVER_URL}/api/datasets/{DATASET_ID}/locks"
    while True:
        try:
            lock_list_response = requests_retry_session().get(lock_url, headers=headers, timeout=15)
        except requests.exceptions.HTTPError as e:
            print(f"HTTP error: {e}")
        except requests.exceptions.ConnectionError as e:
            print(f"Connection error: {e}")
        except requests.exceptions.Timeout as e:
            print(f"Timeout error: {e}")
        except requests.exceptions.RequestException as e:
            print(f"Error: {e}")

        dataset_locks = lock_list_response.json()
        
        if dataset_locks['data'] == []:
            print('Dataset is not locked...')
            break
        else:
            print('dataset_locks: ', dataset_locks)
            unlock_response = requests.delete(lock_url, headers=headers, timeout=15)
            print('unlock_response: ', unlock_response)
            print('Dataset is locked. Waiting 5 seconds...')
            time.sleep(5)
            print('Trying again...')

def main(compiled_file_list, loop_number=0, start_time=None, time_per_batch=None, staring_file_number=0):
    try:
        if start_time is None:
            start_time = time.time()
        if time_per_batch is None:
            time_per_batch = []
        total_files = len(compiled_file_list)
        restart_number = staring_file_number
        check_and_unlock_dataset()
        for i in range(restart_number, len(compiled_file_list), FILES_PER_BATCH):
            batch_start_time = time.time()
            if i + FILES_PER_BATCH > len(compiled_file_list):
                files = compiled_file_list[i:]
            else:
                files = compiled_file_list[i:i+FILES_PER_BATCH]
            print(f"Uploading files {i} to {i+FILES_PER_BATCH}... {len(compiled_file_list) - i - FILES_PER_BATCH}")
            headers = {
                "X-Dataverse-key": DATAVERSE_API_TOKEN
            }
            first_url_call = f"{SERVER_URL}/api/datasets/:persistentId/?persistentId={DATASET_PERSISTENT_ID}"
            response = requests_retry_session().get(first_url_call, headers=headers)
            data = response.json()
            upload_file(files, 0)
            batch_end_time = time.time()
            time_per_batch.append(batch_end_time - batch_start_time)
            average_time_per_batch = sum(time_per_batch) / len(time_per_batch)
            batches_left = (total_files - i) / FILES_PER_BATCH
            estimated_time_left = batches_left * average_time_per_batch
            hours, remainder = divmod(estimated_time_left, 3600)
            minutes, _ = divmod(remainder, 60)
            print(f"Uploading files {i} to {i+FILES_PER_BATCH}... {total_files - i - FILES_PER_BATCH} files left to upload. Estimated time remaining: {int(hours)} hours and {int(minutes)} minutes.")
            restart_number = i

    except Exception as e:
        print(f"An error occurred in Main(): {e}")
        traceback.print_exc()
        if loop_number > 5:
            print('Loop number is greater than 5. Exiting program.')
            sys.exit(1)
        main(compiled_file_list, loop_number=loop_number+1)

def wipe_report():
    """
    Wipe the file_hashes.json file.
    """
    if os.path.isfile(LOCAL_JSON_FILE_WITH_LOCAL_FS_HASHES):
        with open(LOCAL_JSON_FILE_WITH_LOCAL_FS_HASHES, 'w') as outfile:
            json.dump([], outfile)
    if os.path.isfile(modified_doi_str):
        with open(modified_doi_str, 'w') as second_outfile:
            json.dump([], second_outfile)
    if os.path.isfile(LOCAL_FILE_LIST_STORED):
        os.remove(LOCAL_FILE_LIST_STORED)

def get_list_of_the_doi_files_online():
    """
    Get a list of files with hashes that are already online.
    """
    headers = {
        "X-Dataverse-key": DATAVERSE_API_TOKEN
    }
    print("Getting the list of files online for this DOI...")
    first_url_call = f"{SERVER_URL}/api/datasets/:persistentId/?persistentId={DATASET_PERSISTENT_ID}"
    response = requests_retry_session().get(first_url_call, headers=headers)
    data = response.json()

    if 'status' in data and data['status'] == 'ERROR' and data['message'] == 'Bad api key ':
        print('Bad api key. Exiting program.')
        sys.exit(1)

    check_and_unlock_dataset()
    url = f"{SERVER_URL}/api/datasets/{DATASET_ID}/versions/:draft/files"
    # Request the list of files for this DOI
    second_response = requests_retry_session().get(url, headers=headers)
    full_data = second_response.json()
    files_online_for_this_doi = []
    for file in full_data['data']:
        files_online_for_this_doi.append(file['dataFile'])
    print(f"Found {len(files_online_for_this_doi)} files for this DOI online.")
    print("")
    print("Writing the list of files to file_hashes.json...")
    with open(modified_doi_str, 'w') as outfile:
        json.dump(files_online_for_this_doi, outfile)
    return files_online_for_this_doi

def get_all_local_hashes_that_are_not_online():
    global ONLINE_FILE_DATA
    print("Checking if all files are online...")
    check_list_list_of_hashes_online = get_list_of_the_doi_files_online()
    # turn the list of hashes from file into a list of hashes
    missing_files = []
    # If the LOCAL_JSON_FILE_WITH_LOCAL_FS_HASHES is empty then run get_files_with_hashes_list() to create the file_hashes.json file
    if not os.path.isfile(LOCAL_JSON_FILE_WITH_LOCAL_FS_HASHES) or does_file_exist_and_content_isnt_empty(LOCAL_JSON_FILE_WITH_LOCAL_FS_HASHES):
        get_files_with_hashes_list()
    with open(LOCAL_JSON_FILE_WITH_LOCAL_FS_HASHES) as json_file:
        check_list_data = json.load(json_file)
        for file_path, file_hash in check_list_data.items():
            if file_hash not in check_list_list_of_hashes_online:
                missing_files.append(file_path)
                if HIDE_DISPLAY:
                    print(f"File with hash {file_hash} is not online.", end="\r")
    if missing_files != []:
        print(f"Found {len(missing_files)} files locally missing from the DOI.")
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

if __name__ == "__main__":
    NORMALIZED_FOLDER_PATH = os.path.normpath(UPLOAD_DIRECTORY)
    SANITIZED_FILENAME = sanitize_folder_path(os.path.abspath(UPLOAD_DIRECTORY))
    LOCAL_JSON_FILE_WITH_LOCAL_FS_HASHES = os.getcwd() + '/' + SANITIZED_FILENAME + '.json'
    LOCAL_FILE_LIST_STORED = os.getcwd() + '/' + SANITIZED_FILENAME + '_file_list.txt'
    original_doi_str = DATASET_PERSISTENT_ID
    modified_doi_str = ''.join(['_' if not c.isalnum() else c for c in original_doi_str]) + '.json'

    if HIDE_DISPLAY:
        print("\n👀 The option to hide hashing progress is not enabled. Hashing progress will be displayed on the screen.\n")
    else:
        print("\n🚫 The option to hide hashing progress is enabled. Hashing progress will not be displayed on the screen.\n")

    print(f"🔍 Verifying the existence of the folder: {UPLOAD_DIRECTORY}...")
    if has_read_access(UPLOAD_DIRECTORY):
        print(f" ✅ The user has read access to the folder: {UPLOAD_DIRECTORY}\n")
    else:
        print(f" ❌ The user does not have read access to the folder: {UPLOAD_DIRECTORY}\n\n")
        sys.exit(1)

    print(f"📁 Checking if the folder: {UPLOAD_DIRECTORY} is empty...")
    if is_directory_empty(UPLOAD_DIRECTORY):
        print(f" ❌ The folder: {UPLOAD_DIRECTORY} is empty\n\n")
        sys.exit(1)
    else:
        print(f" ✅ The folder: {UPLOAD_DIRECTORY} is not empty\n")

    if WIPE_CACHE and not os.path.isfile(LOCAL_JSON_FILE_WITH_LOCAL_FS_HASHES):
        print(f"🧹 Wiping the {LOCAL_JSON_FILE_WITH_LOCAL_FS_HASHES} file ...\n")
        wipe_report()
        print("")
    else:
        print(f"📖 Reading hashes from {LOCAL_JSON_FILE_WITH_LOCAL_FS_HASHES} ...\n")

    if FILES_PER_BATCH != 20:
        print("⚠️ Bigger batch sizes does not mean faster upload times. It is recommended to keep the batch size at 20. This is intended for fine tuning.\n")

    with open(modified_doi_str) as json_file:
        ONLINE_FILE_DATA = json.load(json_file)

    print("🔍 Get the dataset id...")
    DATASET_INFO = get_dataset_info()
    DATASET_ID = DATASET_INFO["data"]["id"]
    print(f" 🆔 Dataset ID: {DATASET_ID}\n")
    local_fs_files_array = get_files_with_hashes_list()
    compiled_file_list_with_mimetypes = set_files_and_mimetype_to_exported_file(local_fs_files_array)

    while get_all_local_hashes_that_are_not_online() is not False:
        print("🔄 Checking if all files are online and running the file batch size of {}...".format(FILES_PER_BATCH))
        print("🚀 Identified that not all files were uploaded. Starting the upload process...")
        main(compiled_file_list_with_mimetypes)
        time.sleep(5)

    print("\n\nDone.\n\n")