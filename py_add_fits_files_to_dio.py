#!/usr/bin/env python3

# Path: py_add_fits_files_to_dio.py

# Using pyDataverse to upload files to Dataverse
# ----------------------------------------------

# This script iterates through each (originally only for fits) file in the specified directory, extracts the star number from
# the file name, and uses this information to construct a JSON payload. It then executes a curl command
# to upload each file to a specified Dataverse server using the provided API token and Persistent ID.
# The output of each curl command, along with relevant data, is logged to 'log.txt' for record-keeping
# and debugging purposes.

# Fixes some issues with grequests and urllib3
import gevent.monkey
gevent.monkey.patch_all(thread=False, select=False)
import grequests
import requests

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
import requests

parser = argparse.ArgumentParser()
parser.add_argument("-f", "--folder", help="The directory containing the FITS files.", required=True)
parser.add_argument("-t", "--token", help="API token for authentication.", required=True)
parser.add_argument("-p", "--persistent_id", help="Persistent ID for the dataset.", required=True)

# Define the missing function
def get_dataset_info(server_url, persistent_id):
    """Get the dataset info from the Dataverse server."""
    # Get the dataset ID # of the DOI
    api = pyDataverse.api.NativeApi(server_url)
    response = api.get_dataset(persistent_id)
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"Error retrieving dataset: {response.json()['message']}")

parser.add_argument("-u", "--server_url", help="URL of the Dataverse server.", required=True)
args = parser.parse_args()

# Get the dataset ID # of the DOI
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
    print("  -h                Display this help message.")
    print("Example: {} -f 'sample_fits/' -t 'xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx' -p 'doi:10.5072/FK2/J8SJZB' -u 'https://localhost:8080'".format(sys.argv[0]))
    print("")
    sys.exit(0)

def get_files_array(directory):
    files = []
    for filename in os.listdir(directory):
        # If file is hidden or starts with a period, skip it
        if filename.startswith("."):
            print(f"Skipping hidden file: {filename}")
            continue
        mimeType = guess_mime_type(os.path.join(directory, filename))

        # Manual overrides
        if mimeType == "application/fits":
            mimeType = "image/fits"

        # If mimeType is "None", then set mimeType based on file extension
        # or if the type of variable is a nonetype (Shape Files)
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

        file_path = os.path.join(directory, filename)
        file_name_without_extension = os.path.splitext(filename)[0]
        description = f"This file's name is '{file_name_without_extension}' and is a fits file."
        directory_label = ""
        file_dict = {
            "directoryLabel": directory_label,
            "filepath": file_path,
            "mimeType": mimeType,
            "description": description
        }
        files.append(file_dict)
    # Sort the files array by file name in reverse order
    files_sorted = sorted(files, key=lambda k: k['filepath'], reverse=True)
    return files_sorted

# Check if all required arguments are provided
if not args.folder or not args.token or not args.persistent_id or not args.server_url:
    print("Error: Missing arguments.")
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
# add a loop number to the upload_file function to try again if it fails
def upload_file(api_token, dataverse_url, persistent_id, files, loop_number=0):
    try:
        dvuploader = DVUploader(files=files)
        print("Uploading files...")
        # Print all of the dvuploader.upload functions available
        # redirect dvuploader_log to standard output
        dvuploader_log = dvuploader.upload(
            api_token=args.token,
            dataverse_url=args.server_url,
            persistent_id=args.persistent_id,
        )
    except Exception as e:
        print(f"An error occurred: {e}")
        time.sleep(5)
        print('Trying again...')
        # if the loop_number is greater than 5, then exit the program
        if loop_number > 5:
            print('Loop number is greater than 5. Exiting program.')
            sys.exit(1)
        upload_file(api_token, dataverse_url, persistent_id, files, loop_number=loop_number+1)

    return dvuploader_log

def check_and_unlock_dataset(server_url, dataset_id, token):
    """
    Checks for any locks on the dataset and attempts to unlock if locked.
    """
    headers = {
        "X-Dataverse-key": token
    }
    lock_url = f"{server_url}/api/datasets/{dataset_id}/locks"
    while True:
        lock_list_response = requests.get(lock_url, headers=headers)
        dataset_locks = lock_list_response.json()
        
        if dataset_locks['data'] == []:
            print('Dataset is unlocked. Continuing...')
            break
        else:
            print('dataset_locks: ', dataset_locks)
            unlock_response = requests.delete(lock_url, headers=headers)
            print('unlock_response: ', unlock_response)
            print('Dataset is locked. Waiting 5 seconds...')
            time.sleep(5)
            print('Trying again...')

def main():
    try:
        files_array = get_files_array(args.folder)
        # Iterate 10 items in files_array at a time.
        # This is to avoid the "too many files open" error
        # when uploading a large number of files.
        for i in range(0, len(files_array), 10):
            if i + 10 > len(files_array):
                files = files_array[i:]
            else:
                files = files_array[i:i+10]
            print(f"Uploading files {i} to {i+10}...")
            headers = {
                "X-Dataverse-key": args.token
            }
            first_url_call = f"{args.server_url}/api/datasets/:persistentId/?persistentId={args.persistent_id}"
            response = requests.get(first_url_call, headers=headers)
            data = response.json()
            dataset_id = data['data']['id']
            check_and_unlock_dataset(args.server_url, dataset_id, args.token)
            # Pipe the output of the upload_file function to a variable
            upload_file(args.token, args.server_url, args.persistent_id, files)

    except Exception as e:
        print(f"An error occurred: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
    print("Upload complete.")