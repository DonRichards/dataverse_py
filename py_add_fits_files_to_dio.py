#!/usr/bin/env python3

# Using pyDataverse to upload files to Dataverse
# ----------------------------------------------

# This script iterates through each .fits file in the specified directory, extracts the star number from
# the file name, and uses this information to construct a JSON payload. It then executes a curl command
# to upload each file to a specified Dataverse server using the provided API token and Persistent ID.
# The output of each curl command, along with relevant data, is logged to 'log.txt' for record-keeping
# and debugging purposes.

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
from pyDataverse.api import NativeApi
from dvuploader import DVUploader, File

class File:
    def __init__(self, directoryLabel, filepath, description):
        self.directoryLabel = directoryLabel
        self.filepath = filepath
        self.description = description

    def __repr__(self):
        return f"File(directoryLabel='{self.directoryLabel}', filepath='{self.filepath}', description='{self.description}')"


def get_files_array(directory):
    files = []
    for filename in os.listdir(directory):
        if filename.endswith(".fits"):
            file_path = os.path.join(directory, filename)
            file_name_without_extension = os.path.splitext(filename)[0]
            description = f"This file's name is '{file_name_without_extension}' and is a fits file."
            directory_label = ""  # Update this as needed
            file_dict = {
                "directoryLabel": directory_label,
                "filepath": file_path,
                "description": description
            }
            files.append(file_dict)

    return files


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

parser = argparse.ArgumentParser()
parser.add_argument("-f", "--folder", help="The directory containing the FITS files.", required=True)
parser.add_argument("-t", "--token", help="API token for authentication.", required=True)
parser.add_argument("-p", "--persistent_id", help="Persistent ID for the dataset.", required=True)
parser.add_argument("-u", "--server_url", help="URL of the Dataverse server.", required=True)
args = parser.parse_args()

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

def upload_file_to_dataset(base_url, api_token, doi, file_path):
    cmd = [
        "dvuploader",
        "--token", api_token,
        "--dvurl", server_url,
        "--dataset", persistent_id,
        "--file", file_path
    ]
    api = NativeApi(base_url, api_token)
    dataset_id = api.get_dataset(doi).json()['data']['id']
    with open(file_path, 'rb') as file:
        df = Datafile()
        file_metadata = {
            "description": "FITS file for star {}".format(os.path.basename(file_path)),
            "pid": doi,
            "filename": os.path.basename(file_path),
            "categories": ["Astronomy"]
        }
        df.set(file_metadata)
        response = api.upload_datafile(dataset_id, file, json_str=df.json(), is_pid=False)
        # json_metadata = json.dumps({"description": file_metadata['description'], "categories": file_metadata['categories']})
        # response = api.upload_datafile(dataset_id, file_path, json_str=json_metadata, is_pid=False)

    return response

def get_dataset_info(base_url, doi):
    api = NativeApi(base_url)
    response = api.get_dataset(doi)

    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"Error retrieving dataset: {response.json()['message']}")

def main():
    try:
        files_array = get_files_array(args.folder)
        dvuploader = DVUploader(files=files_array)
        uploader = dvuploader.upload(
            api_token=args.token,
            dataverse_url=args.server_url,
            persistent_id=args.persistent_id,
        )
        print ("Uploader:", uploader)

    except Exception as e:
        print(f"An error occurred: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
