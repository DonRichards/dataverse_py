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
parser.add_argument("-w", "--wipe", help="Wipe the file hashes json file.", action='store_true', required=False)
parser.add_argument("-d", "--display", help="Hide the display progress.", action='store_true', required=False)

args = parser.parse_args()
if args.files_per_batch is None:
    files_per_batch = 20
else:
    files_per_batch = int(args.files_per_batch)

original_doi_str = args.persistent_id
modified_doi_str = ''.join(['_' if not c.isalnum() else c for c in original_doi_str]) + '.json'

def sanitize_folder_path(folder_path):
    folder_path = folder_path.rstrip('/').lstrip('./').lstrip('/')
    sanitized_name = re.sub(r'[^\w\-\.]', '_', folder_path)
    return sanitized_name

sanitized_filename = sanitize_folder_path(os.path.abspath(args.folder))
local_json_file_with_local_fs_hashes = sanitized_filename + '.json'

def get_dataset_info(server_url, persistent_id):
    """Get the dataset info from the Dataverse server."""
    api = pyDataverse.api.NativeApi(server_url)
    response = api.get_dataset(persistent_id)
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"Error retrieving dataset: {response.json()['message']}")

dataset_info = get_dataset_info(args.server_url, args.persistent_id)
dataset_id = dataset_info["data"]["id"]

class File:
    def __init__(self, directoryLabel, filepath, description, mimeType):
        self.directoryLabel = directoryLabel
        self.filepath = filepath
        self.description = description
        self.mimeType = mimeType
    def __repr__(self):
        return f"File(directoryLabel='{self.directoryLabel}', filepath='{self.filepath}', description='{self.description}', mimeType='{self.mimeType}')"

def show_help():
    print("")
    print("Usage: {} -f FOLDER -t API_TOKEN -p PERSISTENT_ID -u SERVER_URL".format(sys.argv[0]))
    print("  -f FOLDER         The directory containing the FITS files.")
    print("  -t API_TOKEN      API token for authentication.")
    print("  -p PERSISTENT_ID  Persistent ID for the dataset.")
    print("  -u SERVER_URL     URL of the Dataverse server.")
    print("  -b FILES_PER_BATCH Number of files to upload per batch.")
    print("  -w WIPE           Wipe the file hashes json file.")
    print("  -d DISPLAY        Hide the display progress.")
    print("  -h                Display this help message.")
    print("Example: {} -f 'sample_fits/' -t 'xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx' -p 'doi:10.5072/FK2/J8SJZB' -u 'https://localhost:8080'".format(sys.argv[0]))
    print("")
    sys.exit(0)

def hash_file(file_path, hash_algo="md5"):
    """ Hash a single file and return the hash """
    hash_func = getattr(hashlib, hash_algo)()
    try:
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_func.update(chunk)
    except IOError:
        return file_path, None
    return file_path, hash_func.hexdigest()

def is_file_empty_or_brackets(file_path):
    try:
        with open(file_path, 'r') as file:
            content = file.read().strip()
            return content == "" or content == "[]"
    except FileNotFoundError:
        print("File not found.")
        return False

def get_files_with_hashes_list(directory):
    file_paths_unsorted = [os.path.join(directory, filename) for filename in os.listdir(directory) if not filename.startswith(".")]
    file_paths = sorted(file_paths_unsorted, reverse=True)
    if is_file_empty_or_brackets(local_json_file_with_local_fs_hashes):
        print("Calculating hashes...")
        results = {}
        for file_path in file_paths:
            file_path, file_hash = hash_file(file_path)
            if args.display:
                print(f" Hashing file {file_path}... ", end="\r")
            while file_hash is None:
                print(f"File {file_path} is empty. Trying again...")
                hash_file(file_path)
                file_path, file_hash = hash_file(file_path)
            # if file_hash exist in the online list of hashes then skip
            if check_if_hash_is_online(file_hash):
                if args.display:
                    print(f"File with hash {file_hash} is already online. Skipping...")
                continue
            results[file_path] = file_hash
        print("")
        print(f"Writing hashes to {local_json_file_with_local_fs_hashes}...")
        with open(local_json_file_with_local_fs_hashes, 'w') as f:
            json.dump(results, f, indent=4)
    else:
        print(f"Reading hashes from {local_json_file_with_local_fs_hashes}...")
        with open(local_json_file_with_local_fs_hashes) as json_file:
            data = json.load(json_file)
            old_results = list(data.items())
            # Check if the file is already online.
            results = {}
            for file_path, file_hash in old_results:
                if check_if_hash_is_online(file_hash):
                    if args.display:
                        print(f"File with hash {file_hash} is already online. Skipping...")
                    continue
                results[file_path] = file_hash
        print("")
    print(f"Found hashing {len(results)} files not uploaded to DOI yet.")
    return results

def set_files_and_mimetype_to_exported_file(results):
    print("Setting file definitions with mimetypes & metadata together...")
    print("This will likely take a while.")
    directory = args.folder
    files = []

    # Odd bug on first load.
    if type(results) == type({}):
        results = list(results.items())

    for file_path, file_hash in results:
        if args.display:
            print(f" Setting file {file_path}... ", end="\r")
        if file_hash is None or file_hash == "":
            print(f" Hash for file {file_path} is empty... ", end="\r")
            continue
        if file_path is None or file_path == "":
            print(f" File path for file {file_path} is empty... ", end="\r")
            continue
        mimeType = guess_mime_type(os.path.join(directory, file_path))
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
        file_name_without_extension = os.path.splitext(file_path)[0]
        directory_label = ""
        description = f"This file's name is '{file_name_without_extension}' and is a fits file."
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

# Check if all required arguments are provided
if not args.folder or not args.token or not args.persistent_id or not args.server_url:
    print("Error: Missing arguments.")
    if not args.folder:
        print("Missing argument: -f FOLDER")
    if not args.token:
        print("Missing argument: -t API_TOKEN")
    if not args.persistent_id:
        print("Missing argument: -p PERSISTENT_ID")
    if not args.server_url:
        print("Missing argument: -u SERVER_URL")
    show_help()

# Process SERVER_URL to ensure it has the correct protocol
if re.match("^http://", args.server_url):
    # Replace "http://" with "https://"
    args.server_url = re.sub("^http://", "https://", args.server_url)
elif not re.match("^https://", args.server_url):
    # Add "https://" if no protocol is specified
    args.server_url = "https://{}".format(args.server_url)

# Use for testing if connection to Dataverse server is successful
def get_dataset_info(base_url, doi):
    api = pyDataverse.api.NativeApi(base_url)
    response = api.get_dataset(doi)
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"Error retrieving dataset: {response.json()['message']}")

def requests_retry_session(
    retries=3,
    backoff_factor=0.3,
    status_forcelist=(500, 502, 504),
    session=None,
):
    session = session or requests.Session()
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session

def upload_file(api_token, dataverse_url, persistent_id, files, loop_number=0):
    try:
        dvuploader = DVUploader(files=files)
        print("Uploading files...")
        dvuploader.upload(
            api_token=args.token,
            dataverse_url=args.server_url,
            persistent_id=args.persistent_id,
            n_jobs=files_per_batch,
        )
    except Exception as e:
        print(f"An error occurred with uploading: {e}")
        print('Trying again in 10 seconds...')
        time.sleep(10)
        if loop_number > 5:
            print('Loop number is greater than 5. Exiting program.')
        upload_file(api_token, dataverse_url, persistent_id, files, loop_number=loop_number+1)
    return True

def check_if_hash_is_online(file_hash):
    """
    Checks if the file hash is already online.
    """
    # Open the json file containing the list of files and thier "md5" hashes and check if the file_hash is in the list
    with open(modified_doi_str) as json_file:
        data = json.load(json_file)
        for file in data:
            if file_hash in file.values():
                return True
    return False

def check_and_unlock_dataset(server_url, dataset_id, token):
    """
    Checks for any locks on the dataset and attempts to unlock if locked.
    """
    headers = {
        "X-Dataverse-key": token
    }
    lock_url = f"{server_url}/api/datasets/{dataset_id}/locks"
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
            unlock_response = requests.delete(lock_url, headers=headers)
            print('unlock_response: ', unlock_response)
            print('Dataset is locked. Waiting 5 seconds...')
            time.sleep(5)
            print('Trying again...')

def main(loop_number=0, start_time=None, time_per_batch=None, staring_file_number=0):
    try:
        if start_time is None:
            start_time = time.time()
        if time_per_batch is None:
            time_per_batch = []
        local_fs_files_array = get_files_with_hashes_list(args.folder)
        compiled_file_list = set_files_and_mimetype_to_exported_file(local_fs_files_array)
        total_files = len(compiled_file_list)
        restart_number = staring_file_number
        for i in range(staring_file_number, len(compiled_file_list), files_per_batch):
            batch_start_time = time.time()
            if i + files_per_batch > len(compiled_file_list):
                files = compiled_file_list[i:]
            else:
                files = compiled_file_list[i:i+files_per_batch]
            print(f"Uploading files {i} to {i+files_per_batch}... {len(compiled_file_list) - i - files_per_batch}")
            headers = {
                "X-Dataverse-key": args.token
            }
            first_url_call = f"{args.server_url}/api/datasets/:persistentId/?persistentId={args.persistent_id}"
            response = requests_retry_session().get(first_url_call, headers=headers, timeout=15)
            data = response.json()
            dataset_id = data['data']['id']
            check_and_unlock_dataset(args.server_url, dataset_id, args.token)
            upload_file(args.token, args.server_url, args.persistent_id, files)
            batch_end_time = time.time()
            time_per_batch.append(batch_end_time - batch_start_time)
            average_time_per_batch = sum(time_per_batch) / len(time_per_batch)
            batches_left = (total_files - i) / files_per_batch
            estimated_time_left = batches_left * average_time_per_batch
            hours, remainder = divmod(estimated_time_left, 3600)
            minutes, _ = divmod(remainder, 60)
            print(f"Uploading files {i} to {i+files_per_batch}... {total_files - i - files_per_batch} files left to upload. Estimated time remaining: {int(hours)} hours and {int(minutes)} minutes.")
            restart_number = i

    except Exception as e:
        print(f"An error occurred in Main(): {e}")
        traceback.print_exc()
        if loop_number > 5:
            print('Loop number is greater than 5. Exiting program.')
            sys.exit(1)
        main(loop_number=loop_number+1, start_time=start_time, time_per_batch=time_per_batch, staring_file_number=restart_number)

def wipe_report():
    """
    Wipe the file_hashes.json file.
    """
    with open(local_json_file_with_local_fs_hashes, 'w') as outfile:
        json.dump([], outfile)
    with open(modified_doi_str, 'w') as second_outfile:
        json.dump([], second_outfile)

def get_list_of_the_doi_files_online():
    """
    Get a list of files with hashes that are already online.
    """
    headers = {
        "X-Dataverse-key": args.token
    }
    print("Getting the list of files online for this DOI...")
    first_url_call = f"{args.server_url}/api/datasets/:persistentId/?persistentId={args.persistent_id}"
    response = requests_retry_session().get(first_url_call, headers=headers)
    data = response.json()

    if 'status' in data and data['status'] == 'ERROR' and data['message'] == 'Bad api key ':
        print('Bad api key. Exiting program.')
        sys.exit(1)

    dataset_id = data['data']['id']
    check_and_unlock_dataset(args.server_url, dataset_id, args.token)
    url = f"{args.server_url}/api/datasets/{dataset_id}/versions/:latest/files"
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
    print("Checking if all files are online...")
    check_list_list_of_hashes_online = get_list_of_the_doi_files_online()
    # turn the list of hashes from file into a list of hashes
    missing_files = []
    # If the local_json_file_with_local_fs_hashes is empty then run get_files_with_hashes_list(args.folder) to create the file_hashes.json file
    if is_file_empty_or_brackets(local_json_file_with_local_fs_hashes):
        get_files_with_hashes_list(args.folder)
    with open(local_json_file_with_local_fs_hashes) as json_file:
        check_list_data = json.load(json_file)
        for file_path, file_hash in check_list_data.items():
            if file_hash not in check_list_list_of_hashes_online:
                missing_files.append(file_path)
                if args.display:
                    print(f"File with hash {file_hash} is not online.")
    if missing_files != []:
        print(f"Found {len(missing_files)} files locally missing from the DOI.")
        return missing_files
    return False

if __name__ == "__main__":
    if args.display:
        print("Hiding of hashing progress is turned on.")
    else:
        print("Hiding of hashing progress is turned off.")

    if args.wipe and os.path.isfile(local_json_file_with_local_fs_hashes):
        print(f"Wiping the {local_json_file_with_local_fs_hashes} file ...")
        wipe_report()
        print("")
    else:
        print(f"Reading hashes from {local_json_file_with_local_fs_hashes} ...")

    if files_per_batch != 20:
        print("Bigger batch sizes does not mean faster upload times. It is recommended to keep the batch size at 20. This is intended for fine tuning.")

    while get_all_local_hashes_that_are_not_online() is not False:
        print("Checking if all files are online and running the file batch size of {}...".format(files_per_batch))
        print("Identified that not all files were uploaded. Starting the upload process...")
        main()
        time.sleep(5)

    print("Done.")