#!/usr/bin/env python3

# Path: create_config_yaml.py

# This script creates a YAML file with the configuration for the dvuploader tool.
# ----------------------------------------------

# Usage:
# pipenv run python create_config_yaml.py -f <directory_path> -t <api_token> -p <persistent_id> -u <server_url>

import argparse
from dvuploader import DVUploader, File
import os
import re
import sys
import yaml

# pipenv install PyYAML DVUploader

parser = argparse.ArgumentParser()
parser.add_argument("-f", "--folder", help="The directory containing the FITS files.", required=True)
parser.add_argument("-t", "--token", help="API token for authentication.", required=True)
parser.add_argument("-p", "--persistent_id", help="Persistent ID for the dataset.", required=True)
parser.add_argument("-u", "--server_url", help="URL of the Dataverse server.", required=True)

args = parser.parse_args()
folder_path = args.folder

def sanitize_folder_path(folder_path):
    folder_path = folder_path.rstrip('/').lstrip('./').lstrip('/')
    sanitized_name = re.sub(r'[^\w\-\.]', '_', folder_path)
    return sanitized_name

normalized_folder_path = os.path.normpath(args.folder)
sanitized_filename = sanitize_folder_path(os.path.abspath(args.folder)) + '.yml'

def create_config(directory_path):
    config = {
        'persistent_id': args.persistent_id,
        'dataverse_url': args.server_url,
        'api_token': args.token,
        'files': []
    }

    for filename in os.listdir(directory_path):
        if not filename.startswith('.'):
            file_path = os.path.join(directory_path, filename)
            if os.path.isfile(file_path):
                config['files'].append({
                    'filepath': file_path,
                    'mimetype': 'image/fits',
                    'description': f"Posterior distributions of the stellar parameters for the star with ID from the Gaia DR3 catalog {os.path.splitext(filename)[0]}."
                })

    with open(sanitized_filename, 'w') as config_file:
        yaml.dump(config, config_file, default_flow_style=False, sort_keys=False)

if __name__ == "__main__":
    directory_path = os.path.abspath(folder_path)

    if not os.path.isdir(directory_path):
        print(f"Error: {directory_path} is not a valid directory")
        sys.exit(1)

    create_config(directory_path)
    print(f"{sanitized_filename} has been created.")
    print("To upload the files to Dataverse, run the following command:")
    # Run DVUploader tool with the configuration file.
    print(f"pipenv run dvuploader --config-path {sanitized_filename}")
