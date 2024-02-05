#!/usr/bin/env python3

# Path: create_config_yaml.py

# This script creates a YAML file with the configuration for the dvuploader tool.
# ----------------------------------------------

# Usage:
# pipenv run python create_config_yaml.py -f <directory_path> -t <api_token> -p <persistent_id> -u <server_url>

import argparse
import os
import re
import sys
import yaml
from concurrent.futures import ThreadPoolExecutor

# pipenv install PyYAML

# Define a custom representer for strings to force them into a double-quoted style
def quoted_presenter(dumper, data):
    return dumper.represent_scalar('tag:yaml.org,2002:str', data, style='"')

# Apply the custom representer to the yaml module
yaml.add_representer(str, quoted_presenter)

def create_config(directory_path, persistent_id, server_url, token):
    config = {
        'persistent_id': persistent_id,
        'dataverse_url': server_url,
        'api_token': token,
        'files': []
    }

    def process_file(filename):
        if not filename.startswith('.'):
            file_path = os.path.join(directory_path, filename)
            if os.path.isfile(file_path):
                return {
                    'filepath': file_path,
                    'mimetype': 'image/fits',
                    'description': f"Posterior distributions of the stellar parameters for the star with ID from the Gaia DR3 catalog {os.path.splitext(filename)[0]}."
                }
        return None

    with ThreadPoolExecutor() as executor:
        # Using os.scandir() instead of os.listdir() for efficiency
        futures = [executor.submit(process_file, entry.name) for entry in os.scandir(directory_path) if entry.is_file()]
        for future in futures:
            result = future.result()
            if result:
                config['files'].append(result)

    return config

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-f", "--folder", required=True, help="The directory containing the FITS files.")
    parser.add_argument("-t", "--token", required=True, help="API token for authentication.")
    parser.add_argument("-p", "--persistent_id", required=True, help="Persistent ID for the dataset.")
    parser.add_argument("-u", "--server_url", required=True, help="URL of the Dataverse server.")
    args = parser.parse_args()

    directory_path = os.path.abspath(args.folder)
    if not os.path.isdir(directory_path):
        print(f"Error: {directory_path} is not a valid directory")
        sys.exit(1)

    config = create_config(directory_path, args.persistent_id, args.server_url, args.token)

    sanitized_filename = re.sub(r'[^\w\-\.]', '_', args.folder.strip('/').strip('./')) + '.yml'
    with open(sanitized_filename, 'w') as config_file:
        yaml.dump(config, config_file, default_flow_style=False, sort_keys=False)

    print(f"{sanitized_filename} has been created.")
    print("To upload the files to Dataverse, run the following command:")
    print(f"pipenv run dvuploader --config-path {sanitized_filename}")