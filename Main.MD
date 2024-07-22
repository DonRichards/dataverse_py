The provided Python script, `main.py` previously the `py_add_fits_files_to_dio.py`, is designed to upload files, specifically FITS files, to a Dataverse server. Here is a breakdown of what each part of the script does:

### Summary

- **Imports**: The script imports various modules, including those for handling HTTP requests, JSON, file operations, subprocesses, and logging.
- **Argument Parsing**: Uses `argparse` to handle command-line arguments, including the directory containing files, API token, persistent ID, server URL, and other optional parameters.
- **Configuration and Setup**: Initializes various global variables and configures logging.
- **Utility Functions**: Defines several utility functions for tasks like fetching data from URLs, sanitizing folder paths, getting dataset info, handling retries, hashing files, and more.
- **File Upload**: Implements multiple methods for uploading files to Dataverse, including using the Native API, curl for S3 direct upload, and `pyDataverse`.
- **Main Workflow**: The main function coordinates the entire process, including checking if files are already online, hashing files, preparing files for upload, and uploading them in batches.

### Detailed Breakdown

1. **Imports and Patches**:
   - Patches `gevent` for asynchronous HTTP requests to avoid issues with `grequests` and `urllib3`.
   - Imports necessary libraries and modules.

2. **Argument Parsing**:
   ```python
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
   ```

3. **Configuration and Setup**:
   - Initializes global variables like `UPLOAD_DIRECTORY`, `DATAVERSE_API_TOKEN`, `DATASET_PERSISTENT_ID`, `SERVER_URL`, and others.
   - Configures logging to write logs to `wait_for_200.log`.

4. **Utility Functions**:
   - `fetch_data(url, type="GET")`: Fetches data from a given URL with retries.
   - `sanitize_folder_path(folder_path)`: Sanitizes the folder path.
   - `get_dataset_info()`: Retrieves dataset information using `pyDataverse`.
   - `native_api_upload_file_using_request(files)`: Uploads files using the Native API.
   - `s3_direct_upload_file_using_curl(files)`: Uploads files using curl for S3 direct upload.
   - `upload_file_using_pyDataverse(files)`: Uploads files using `pyDataverse`.
   - `populate_online_file_data(json_file_path)`: Populates online file data from a JSON file.
   - `hash_file(file_path, hash_algo="md5")`: Computes the hash of a file.
   - `is_file_online(file_hash)`: Checks if a file is already online.
   - `does_file_exist_and_content_isnt_empty(file_path)`: Checks if a file exists and is not empty.
   - `get_files_with_hashes_list()`: Retrieves a list of files with their hashes.
   - `set_files_and_mimetype_to_exported_file(results)`: Sets file definitions with mimetypes and metadata.
   - `upload_file_with_dvuploader(upload_files, loop_number=0)`: Uploads files using `dvuploader`.
   - `get_count_of_the_doi_files_online()`: Gets the count of DOI files online.
   - `check_dataset_is_unlocked()`: Checks if the dataset is unlocked.
   - `wait_for_200(url, file_number_it_last_completed, timeout=60, interval=5, max_attempts=None)`: Waits for a 200 status code from a URL.
   - `prepare_files_for_upload()`: Prepares the list of files to be uploaded.

5. **Main Workflow**:
   ```python
   def main(loop_number=0, start_time=None, time_per_batch=None, staring_file_number=0):
       # Main function to coordinate the upload process
   ```

6. **Helper Functions**:
   - `wipe_report()`: Wipes the file_hashes.json file.
   - `get_list_of_the_doi_files_online()`: Gets a list of files with hashes that are already online.
   - `check_all_local_hashes_that_are_online()`: Checks if all files are online.
   - `has_read_access(directory)`: Checks if the directory has read access.
   - `is_directory_empty(directory)`: Checks if a directory is empty or not.
   - `cleanup_storage()`: Cleans up storage on the Dataverse server.

7. **Script Execution**:
   ```python
   if __name__ == "__main__":
       # Sets up necessary paths and variables, checks access and file existence, and starts the upload process
   ```

### Conclusion

The script automates the process of uploading FITS files to a Dataverse server, handling tasks like hashing files, checking if they are already online, and uploading them in batches using various methods. It provides detailed logging and retry mechanisms to ensure robustness and reliability.
