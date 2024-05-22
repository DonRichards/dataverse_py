import argparse
from dvuploader import DVUploader, File
import json
from mimetype_description import guess_mime_type, get_mime_type_description
import os
import pyDataverse.api
import re
from requests.exceptions import SSLError, ConnectionError
import requests
import shutil
import subprocess
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

def is_file_uploaded(check_file_name):
    try:
        # If file doesn't exist or is empty, create it with an empty list
        if not os.path.isfile(TRACKING_FILE_PATH) or os.path.getsize(TRACKING_FILE_PATH) == 0:
            with open(TRACKING_FILE_PATH, 'w') as file:
                json.dump([], file)
        # Read the file and check if check_file_name is in the list
        with open(TRACKING_FILE_PATH, 'r') as file:
            uploaded_files = json.load(file)

        # Extract the filename from each path and check if check_file_name matches any
        return any(check_file_name == os.path.basename(file_path) for file_path in uploaded_files)
    except (FileNotFoundError, json.JSONDecodeError):
        return False

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

def s3_direct_upload_file_using_curl(file_info, retry_delay=10):
    """
    Upload files to a Dataverse dataset using curl for S3 direct upload.

    Args:
    - files (list of dicts): List containing file metadata and paths.
    """
    while True:
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
        except subprocess.TimeoutExpired:
            print(f"Failed to upload file: {filepath} because of timeout. Error: {e.output}")
            time.sleep(retry_delay)
        except subprocess.CalledProcessError as e:
            print(f"Failed to upload file: {filepath}. Error: {e.output}")
            time.sleep(retry_delay)
        except SSLError as e:
            print(f"An error occurred in s3_direct_upload_file_using_curl(): SSL error: {e}, retrying...")
            time.sleep(retry_delay)
        except ConnectionError as e:
            print(f"An error occurred in s3_direct_upload_file_using_curl(): Connection error: {e}, retrying...")
            time.sleep(retry_delay)
        except Exception as e:
            print(f"An error occurred in s3_direct_upload_file_using_curl(): {e}, retrying...")
            time.sleep(retry_delay)
        else:
            print(f"File uploaded successfully: {filepath}")
            update_tracking_file(filepath)
            time.sleep(retry_delay)
            break

def upload_file_using_dvuploader(files, retry_delay=10):
    """
    Upload files to a Dataverse dataset and keep trying indefinitely upon SSL and connection errors.

    Args:
    - files (list of dicts): List containing file metadata and paths.
    - retry_delay (int): Delay between retries in seconds.
    """
    # Initialize the Dataverse API
    # api = pyDataverse.api.NativeApi(SERVER_URL, DATAVERSE_API_TOKEN)
    print(f"files: {files}")
    while True:
        try:
            # Extract file details
            directory_label = ''
            filepath = files.get('filepath')
            mime_type = files.get('mimeType')
            description = files.get('description')

            # Prepare the metadata for the file
            file_metadata = {
                'description': description,
                "filepath": filepath,
                'mimeType': mime_type,
                'directoryLabel': directory_label,
                'categories': [],
                'restrict': False
            }

            # convert file metadata to a list
            upload_files = [File(**file_metadata)]

            # Make sure the server is ready to accept the file
            wait_for_200(SERVER_URL, timeout=60, interval=5)

            print("Upload starting...")
            print('-' * 40)
            dvuploader = DVUploader(files=upload_files)
            dvuploader.upload(
                api_token=DATAVERSE_API_TOKEN,
                dataverse_url=SERVER_URL,
                persistent_id=DATASET_PERSISTENT_ID,
            )

            time.sleep(retry_delay)
        except SSLError as e:
            time.sleep(retry_delay)
            print(f"An error occurred in upload_file_using_dvuploader(): SSL error: {e}, retrying...")
            time.sleep(5)
        except ConnectionError as e:
            time.sleep(retry_delay)
            print(f"An error occurred in upload_file_using_dvuploader(): Connection error: {e}, retrying...")
            time.sleep(retry_delay)
        except Exception as e:
            print(f"An error occurred in upload_file_using_dvuploader(): {e}, retrying...")
            time.sleep(retry_delay)
        else:
            print(f"File uploaded successfully: {filepath}")
            update_tracking_file(filepath)
            time.sleep(retry_delay)
            break

def remove_zip_files(directory):
    """
    Remove all zip files within a directory.
    """
    print(f"Removing zip files from {directory}")
    for filename in os.listdir(directory):
        if filename.endswith('.zip'):
            filename = os.path.join(directory, filename)
            os.remove(filename)
def extract_identifier(filename):
    """
    Extract the numeric identifier from thezip_filename = f"group_{row['Group']}_{row['Rounded_Min']}-{row['Rounded_Max']}.zip" filename.
    """
    identifier_part = filename.split('_')[-1].split('.')[0]
    return int(identifier_part)

def group_files(input_file, output_file, max_group_size=1000):
    """
    Group files from the input file and write the groups to the output file.
    """
    # Read the file paths from the input file
    with open(input_file, 'r') as f:
        file_paths = [line.strip() for line in f.readlines()]

    # Sort the file paths based on the extracted identifier
    file_paths.sort(key=lambda x: extract_identifier(os.path.basename(x)))

    # Group the file paths into groups of up to max_group_size
    grouped_files = []
    for i in range(0, len(file_paths), max_group_size):
        group_files = file_paths[i:i+max_group_size]
        group_number = len(grouped_files) + 1
        group_range = f"{extract_identifier(os.path.basename(group_files[0]))}-{extract_identifier(os.path.basename(group_files[-1]))}"
        grouped_files.append({
            "Group": group_number,
            "Range": group_range,
            "Filenames": group_files
        })

    # Write the grouped files to the output file
    with open(output_file, 'w') as f:
        json.dump(grouped_files, f, indent=4)

def process_directory(directory_path, divisor, num_groups, output_json_path, dry_run):
    """
    Process the directory and create groups of files.
    """
    grouped = []
    results = []
     # Check if the compiled JSON file does not exist and start processing
    if not os.path.isfile(COMPILED_GROUPED_FILES_JSON) or args.wipe:
        print(f"Creating new groups from {directory_path}")
        group_files(LOCAL_FILE_LIST, COMPILED_GROUPED_FILES_JSON)
        print(f"Output has been written to {COMPILED_GROUPED_FILES_JSON}")

    print(f"Reading the compiled JSON file: {COMPILED_GROUPED_FILES_JSON}")
    time.sleep(3)
    with open(COMPILED_GROUPED_FILES_JSON, 'r') as file:
        results = json.load(file)

    for index, row in enumerate(results):
        # Results is a json object with the following structure:
        # [
        #     {
        #         "Group": 1,
        #         "Range": "0-999",
        #         "Filenames": [
        #             "file1.txt",
        #             "file2.txt",
        #             ...
        #         ]
        #     },
        #     ...
        # ]

        zip_filename = f"group_{row['Group']}_{row['Range']}.zip"
        
        if is_file_uploaded(zip_filename):
            print(f"File already uploaded: {zip_filename}")
            continue

        zip_filepath = os.path.join(ZIP_FILE_PATH, zip_filename)

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
            for root, dirs, files in os.walk(directory_path):
                for filename in row['Filenames']:
                    filepath = os.path.join(directory_path, filename)
                    file_hash = LOCAL_FS_HASHES_FROM_JSON.get(filepath, None)
                    manifest.append({
                        filename: file_hash
                    })
                    if os.path.isfile(filepath):
                        # Full path to the file
                        file_path = os.path.join(root, filepath)
                        # Calculate relative path for use in the zip
                        relative_path = os.path.relpath(file_path, directory_path)
                        # Add the file to the zip
                        zipf.write(file_path, arcname=relative_path)
        description = f"Posterior distributions of the stellar parameters from 'PlatinumSGB' files for the star with ID from the Gaia DR3 catalog:\n"
        for item in manifest:
            for filepath, hash_value in item.items():
                filename = os.path.basename(filepath)
                # Manipulate the filename for the description
                filename_no_ext = os.path.splitext(filename)[0]
                filename_without_prefix = filename_no_ext.replace('PlatinumSGB_', '')
                # If not the last filename in the list then add a comma and newline else just add the newline.
                if filename != row['Filenames'][-1]:
                    description += f"{filename_without_prefix},\n"
                else:
                    description += f"{filename_without_prefix}"
                # Remove the trailing comma and newline
        description = description.rstrip(',\n')
        if args.debug:
            print("Debug information:")
            print("\nDescription:")
            print(description)
            print(f"Debug: {zip_filename} - {description}")
            file_size = os.path.getsize(zip_filepath)
            # Adjust the file size to gigabytes
            file_size = file_size / (1024 * 1024 * 1024)
            print(f"File size of {zip_filename}: {file_size} GB")
            results[index]['File_Size'] = file_size
            print(f"Extracting zip file: {zip_filename}")
            with zipfile.ZipFile(zip_filepath, 'r') as zip_ref:
                zip_ref.extractall(f"check_{zip_filepath}")
            print(f"Extracted zip file: {zip_filename}")
            print("Please inspect the zip file and its contents.")
            sys.exit(1)

        time.sleep(3)

        # Upload using upload_file_using_dvuploader
        file_info = {
            'directoryLabel': '',
            'filepath': f"{zip_filepath}",
            'mimeType': 'application/zip',
            'description': description
        }
        # Upload Methods
        upload_file_using_dvuploader(file_info)
        # s3_direct_upload_file_using_curl(file_info)

        print("Deleting zip file...")
        remove_zip_files('/tmp/ziptests/')
        print(f"Deleted zip file: {zip_filename}\n")
        sys.stdout.flush()

    # Writing results to the JSON file
    with open(output_json_path, 'w') as outfile:
        json.dump(results, outfile, indent=4)
    print(f"Output has been written to {output_json_path}")

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
    COMPILED_GROUPED_FILES_JSON = os.getcwd() + '/' + SANITIZED_FILENAME + '_' + str(args.num_groups) + '_grouped_files.json'
    LOCAL_FILE_LIST = os.getcwd() + '/' + SANITIZED_FILENAME + '_file_list.txt'

    if args.wipe:
        print("Wiping the group json file...")
        if os.path.isfile(COMPILED_GROUPED_FILES_JSON) or os.path.isfile(TRACKING_FILE_PATH):
            try:
                if os.path.isfile(COMPILED_GROUPED_FILES_JSON):
                    os.remove(COMPILED_GROUPED_FILES_JSON)
                    print(f"\n✓ File Removed:\n\t {COMPILED_GROUPED_FILES_JSON} ...\n")
                if os.path.isfile(TRACKING_FILE_PATH):
                    os.remove(TRACKING_FILE_PATH)
                    print(f"\n✓ File Removed:\n\t {TRACKING_FILE_PATH} ...\n")
            except OSError as e:
                print(f"Error: {e.strerror}")
                sys.exit(1)
        else:
            print(f"\n❌ - The file: {COMPILED_GROUPED_FILES_JSON} either/or {TRACKING_FILE_PATH} \n\t 👻 Scoured the digital realm, but alas, the file is a ghost. Nothing to sweep away from the virtual floors.\n\n")
        remove_zip_files(ZIP_FILE_PATH)

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

    if os.path.isfile(LOCAL_FILE_LIST) and os.path.getsize(LOCAL_FILE_LIST) > 0:
        print(f"✓ - The file: {LOCAL_FILE_LIST} exists and is not empty\n")
    else:
        print(f"❌ - The file: {LOCAL_FILE_LIST} does not exist or is empty\n\n")
        sys.exit(1)

    cleanup_storage()
    process_directory(NORMALIZED_FOLDER_PATH, args.divisor, args.num_groups, args.output, args.dry_run)
    print("Done...")