import argparse
from dvuploader import DVUploader, File
import json
from mimetype_description import guess_mime_type, get_mime_type_description
import os
import pandas as pd
import pyDataverse.api
import re
from requests.exceptions import SSLError, ConnectionError
import requests
import shutil
import sys
import time
import zipfile

ZIP_FILE_PATH = '/tmp/ziptests/'
TRACKING_FILE_PATH = ZIP_FILE_PATH + 'uploaded_files.json'

def extract_identifier(filename):
    match = re.search(r'\d+', filename)
    return int(match.group()) if match else None

def round_down(num, divisor):
    return num - (num % divisor)

def round_up(num, divisor):
    if num % divisor == 0:
        return num
    return num + (divisor - num % divisor)

def update_tracking_file(file_path):
    try:
        with open(TRACKING_FILE_PATH, 'r') as file:
            uploaded_files = json.load(file)
    except (FileNotFoundError, json.JSONDecodeError):
        uploaded_files = []

    if file_path not in uploaded_files:
        uploaded_files.append(file_path)
        with open(TRACKING_FILE_PATH, 'w') as file:
            json.dump(uploaded_files, file)

def is_file_uploaded(file_path):
    try:
        with open(TRACKING_FILE_PATH, 'r') as file:
            uploaded_files = json.load(file)
        return file_path in uploaded_files
    except (FileNotFoundError, json.JSONDecodeError):
        return False

def wait_for_200(url, timeout=60, interval=5):
    """
    Check a URL repeatedly until a 200 status code is returned.

    Parameters:
    - url: The URL to check.
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
                print(f"{date_time} Success: Received 200 status code from {url}")
                return True
            elif response.status_code == 403 or "User :guest is not permitted to perform requested action." in response.text:
                # Check for specific error message indicating an invalid API token
                print(f"{date_time} Error: The API token is either empty or isn't valid.")
                return False
            else:
                message = f" {date_time} Warning: Received {response.status_code} status code from {url}. Retrying..."
                print(message, end="\r")
        except requests.RequestException as e:
            message = f" {date_time} An error occurred in wait_for_200(): Request failed: {e}, logging and retrying..."
            print(message, end="\r")
        attempts += 1
        if max_attempts is not None and attempts >= max_attempts:
            message = f" {date_time} An error occurred in wait_for_200(): Reached the maximum number of attempts ({max_attempts}) without success."
            print(message)
            return False

        elapsed_time = time.time() - start_time
        time.sleep(interval)

def upload_file_using_pyDataverse(files):
    """
    Upload files to a Dataverse dataset.

    Args:
    - files (list of dicts): List containing file metadata and paths.
    """
    # Initialize the Dataverse API
    api = pyDataverse.api.NativeApi(SERVER_URL, DATAVERSE_API_TOKEN)
    data_access_api = pyDataverse.api.DataAccessApi(SERVER_URL, DATAVERSE_API_TOKEN)

    # Extract file details
    directory_label = ''
    filepath = files.get('filepath')
    mime_type = files.get('mimeType')
    description = files.get('description')

    # Prepare the metadata for the file
    file_metadata = {
        'description': description,
        'directoryLabel': directory_label,
        'categories': [],
        'restrict': False
    }

    # Convert metadata to JSON format as required by the API
    file_metadata_json = json.dumps(file_metadata)

    # Make sure the server is ready to accept the file
    wait_for_200(SERVER_URL, timeout=60, interval=5)

    # Upload the file
    resp = api.upload_datafile(DATASET_PERSISTENT_ID, filepath, file_metadata_json, mime_type)

    if resp.status_code == 200:
        print(f"File uploaded successfully: {filepath}")
    else:
        print(f"Failed to upload file: {filepath}. Response: {resp.text}")
        sys.exit(1)

def remove_zip_files(directory):
    """
    Remove all zip files within a directory.
    """
    for filename in os.listdir(directory):
        if filename.endswith('.zip'):
            filename = os.path.join(directory, filename)
            os.remove(filename)

def process_directory(directory_path, divisor, num_groups, output_json_path, dry_run):
    """
    Process the directory and create groups of files.
    """
    if os.path.isfile(COMPILED_GROUPED_FILES_JSON) and os.path.getsize(COMPILED_GROUPED_FILES_JSON) > 0:
        print(f"Reading existing results from {COMPILED_GROUPED_FILES_JSON}")
        with open(COMPILED_GROUPED_FILES_JSON, 'r') as file:
            results = json.load(file)
        print(f"Found {len(results)} groups")
        if dry_run:
            print("Dry run. No file will be written. Here's the data that would be included:")
            print(json.dumps(results, indent=4))
            return

        # Create the grouped DataFrame from the existing results
        grouped = pd.DataFrame(results)

        # Check if the 'Range' column has the correct format
        if pd.api.types.is_string_dtype(grouped['Range']):
            # Filter out rows with null or empty 'Range' values
            grouped = grouped[grouped['Range'].notnull() & (grouped['Range'] != '')]

            # Extract 'Rounded_Min' and 'Rounded_Max' using regular expressions
            grouped[['Rounded_Min', 'Rounded_Max']] = grouped['Range'].str.extract(r'^(\d+)-(\d+)$', expand=True).astype(int)
            grouped = grouped[['Group', 'Filenames', 'Rounded_Min', 'Rounded_Max']]
        else:
            print("Error: The 'Range' column is not of string type. Exiting.")
            sys.exit(1)

        results = [{
            'Group': row['Group'],
            'Range': f"{row['Rounded_Min']}-{row['Rounded_Max']}",
            'Filenames': row['Filenames']
        } for _, row in grouped.iterrows()]
        with open(COMPILED_GROUPED_FILES_JSON, 'w') as file:
            json.dump(results, file, indent=4)
        print(f"Output has been written to {COMPILED_GROUPED_FILES_JSON}")

    if not dry_run:
        for index, row in grouped.iterrows():
            zip_filename = f"group_{row['Group']}_{row['Rounded_Min']}-{row['Rounded_Max']}.zip"
            dub_zip_filename = f"{zip_filename}.zip"
            if is_file_uploaded(dub_zip_filename):
                print(f"File already uploaded: {dub_zip_filename}")
                continue

            current_directory = os.path.dirname(os.path.realpath(__file__))

            zip_filepath = os.path.join(ZIP_FILE_PATH, zip_filename)
            dub_zip_filepath = os.path.join(ZIP_FILE_PATH, dub_zip_filename)

            hashes_exist = False
            if LOCAL_FS_HASHES_FROM_JSON:
                hashes_exist = True

            manifest = []
            total, used, free = shutil.disk_usage(ZIP_FILE_PATH)
            free_gb = free / (2**30)
            free_gb_rounded = round(free_gb, 2)
            print("Free space:", free_gb_rounded, "GB")
            total_size = 0
            for size_filename in row['Filenames']:
                size_filename = os.path.join(directory_path, size_filename)
                if os.path.isfile(size_filename):
                    file_size = os.path.getsize(size_filename)
                    total_size += file_size

            # Double the total size to account for the second zip file
            total_size = total_size * 2

            # Convert the total size from bytes to gigabytes without unnecessary multiplication
            total_size_gb = total_size / (1024 ** 3)
            total_size_gb_rounded = round(total_size_gb, 2)

            print(f"Estimated uncompressed total size needed: {total_size_gb_rounded} GB")

            # Compare available space to the required size
            if free_gb_rounded < total_size_gb_rounded:
                print("Not enough space to create zip file. Exiting.")
                sys.exit(1)

            remove_zip_files(ZIP_FILE_PATH)

            with zipfile.ZipFile(zip_filepath, 'w') as zipf:
                for filename in row['Filenames']:
                    filepath = os.path.join(directory_path, filename)
                    file_hash = LOCAL_FS_HASHES_FROM_JSON.get(filepath, None)
                    manifest.append({
                        filename: file_hash
                    })
                    if os.path.isfile(filepath):
                        zipf.write(filepath, arcname=filename)

            description = "Posterior distributions of the stellar parameters for the star with ID from the Gaia DR3 catalog:\n"

            for item in manifest:
                for filepath, hash_value in item.items():
                    filename = os.path.basename(filepath)
                    # Manipulate the filename for the description
                    filename_no_ext = os.path.splitext(filename)[0]
                    filename_final = filename_no_ext.replace('_', ' ')
                    description += filename_final + "\n"
                    # description = description.rstrip('\n')

            # Double zip the file
            with zipfile.ZipFile(dub_zip_filepath, 'a') as dubzipf:
                if os.path.isfile(zip_filepath):
                    dubzipf.write(zip_filepath, arcname=dub_zip_filename)

            print(f"Created zip file: {dub_zip_filepath}")

            if args.debug:
                print("Debug information:")
                print("\nDescription:")
                print(description)
                print(f"Debug: {zip_filename} - {description}")
                file_size = os.path.getsize(dub_zip_filepath)
                # Adjust the file size to gigabytes
                file_size = file_size / (1024 * 1024 * 1024)
                print(f"File size of {zip_filename}: {file_size} GB")
                results[index]['File_Size'] = file_size
                print(f"Extracting zip file: {zip_filename}")
                with zipfile.ZipFile(dub_zip_filepath, 'r') as zip_ref:
                    zip_ref.extractall(f"check_{dub_zip_filepath}")
                print(f"Extracted zip file: {zip_filename}")
                print("Please inspect the zip file and its contents.")
                sys.exit(1)

            time.sleep(3)

            # Upload using upload_file_using_pyDataverse
            file_info = {
                'directoryLabel': '',
                'filepath': f"{dub_zip_filepath}",
                'mimeType': 'application/zip',
                'description': description
            }
            upload_file_using_pyDataverse(file_info)

            print("Deleting zip file...")
            remove_zip_files('/tmp/ziptests/')
            print(f"Deleted zip file: {zip_filename}\n")
            sys.stdout.flush()

        # Writing results to the JSON file
        with open(output_json_path, 'w') as outfile:
            json.dump(results, outfile, indent=4)
        print(f"Output has been written to {output_json_path}")
    else:
        print("Dry run. No file will be written. Here's the data that would be included:")
        print(json.dumps(results, indent=4))

def sanitize_folder_path(folder_path):
    """
    Sanitize the folder path.
    """
    folder_path = folder_path.rstrip('/').lstrip('./').lstrip('/')
    sanitized_name = re.sub(r'[^\w\-\.]', '_', folder_path)
    return sanitized_name

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

LOCAL_FS_HASHES_FROM_JSON = []

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Group files based on identifiers with rounding, adjustable number of groups, and optional JSON output.')
    parser.add_argument('--directory_path', type=str, help='Path to the directory containing files.', required=True)
    parser.add_argument('--divisor', type=int, default=100000, help='Divisor for rounding.', required=False)
    parser.add_argument('--num_groups', type=int, default=250, help='Number of groups to create.', required=False)
    parser.add_argument('--dry_run', action='store_true', help='Perform a dry run without actual processing.', required=False)
    parser.add_argument('--output', type=str, default='grouped_files.json', help='Output JSON file path.', required=False)
    parser.add_argument('--debug', action='store_true', help='Print debug information.', required=False)
    parser.add_argument("-t", "--token", help="API token for authentication.", required=True)
    parser.add_argument("-p", "--persistent_id", help="Persistent ID for the dataset.", required=True)
    parser.add_argument("-u", "--server_url", help="URL of the Dataverse server.", required=True)
    parser.add_argument("-w", "--wipe", help="Wipe the file hashes json file.", action='store_true', required=False)
    args = parser.parse_args()

    if args.token == '':
        print("\n\n ❌ API token is empty.\n")
        sys.exit(1)

    DATAVERSE_API_TOKEN=args.token
    DATASET_PERSISTENT_ID=args.persistent_id
    SERVER_URL=args.server_url
    CURRENT_DIRECTORY = os.path.dirname(os.path.realpath(__file__))
    NORMALIZED_FOLDER_PATH = os.path.normpath(args.directory_path)
    SANITIZED_FILENAME = sanitize_folder_path(os.path.abspath(args.directory_path))
    LOCAL_JSON_FILE_WITH_LOCAL_FS_HASHES = os.getcwd() + '/' + SANITIZED_FILENAME + '.json'
    COMPILED_GROUPED_FILES_JSON = os.getcwd() + '/' + SANITIZED_FILENAME + '_grouped_files.json'

    if args.wipe:
        print("Wiping the group json file...")
        if os.path.isfile(COMPILED_GROUPED_FILES_JSON) and os.path.getsize(COMPILED_GROUPED_FILES_JSON) > 0:
            with open(COMPILED_GROUPED_FILES_JSON, 'w') as file:
                file.write('')
        print("Done wiping COMPILED_GROUPED_FILES_JSON file.")
        sys.exit(1)

    print(f"🔍 - Verifying the existence of the folder: {NORMALIZED_FOLDER_PATH}...")
    if has_read_access(NORMALIZED_FOLDER_PATH):
        print(f" ✓ The user has read access to the folder: {NORMALIZED_FOLDER_PATH}\n")
    else:
        print(f" ❌ - The user does not have read access to the folder: {NORMALIZED_FOLDER_PATH}\n\n")
        sys.exit(1)

    print(f"📁 Checking if the folder: {NORMALIZED_FOLDER_PATH} is empty...")
    if is_directory_empty(NORMALIZED_FOLDER_PATH):
        print(f" ❌ - The folder: {NORMALIZED_FOLDER_PATH} is empty\n\n")
        sys.exit(1)
    else:
        print(f" ✓ The folder: {NORMALIZED_FOLDER_PATH} is not empty\n")
    # if LOCAL_JSON_FILE_WITH_LOCAL_FS_HASHES is a file and not empty, then read the keys from the file.
    if os.path.isfile(LOCAL_JSON_FILE_WITH_LOCAL_FS_HASHES) and os.path.getsize(LOCAL_JSON_FILE_WITH_LOCAL_FS_HASHES) > 0:
        with open(LOCAL_JSON_FILE_WITH_LOCAL_FS_HASHES, 'r') as file:
            data = file.read()
            LOCAL_FS_HASHES_FROM_JSON = json.loads(data)
    else:
        print(f"❌ - The file: {LOCAL_JSON_FILE_WITH_LOCAL_FS_HASHES} does not exist or is empty\n\n")
        sys.exit(1)
    process_directory(NORMALIZED_FOLDER_PATH, args.divisor, args.num_groups, args.output, args.dry_run)
    print("Done...")