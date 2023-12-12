#!/usr/bin/env bash

# Example of running this on a real world doi
# ./add_fits_files_to_doi.sh -f '/home/dricha73/demo_folder' -t 'X-Dataverse-key: xxxxxxxx' -p 'doi:10.7281/T1/NDA8LN' -u 'https://127.0.0.1:8080'

# FITS File Processor for Dataverse Upload
# -----------------------------------------
# This script iterates through each .fits file in the specified directory, extracts the star number from 
# the file name, and uses this information to construct a JSON payload. It then executes a curl command 
# to upload each file to a specified Dataverse server using the provided API token and Persistent ID. 
# The output of each curl command, along with relevant data, is logged to 'log.txt' for record-keeping 
# and debugging purposes.

# Function to display help
show_help() {
    echo ""
    echo "Usage: $0 -f FOLDER -t API_TOKEN -p PERSISTENT_ID -u SERVER_URL"
    echo "  -f FOLDER         The directory containing the FITS files."
    echo "  -t API_TOKEN      API token for authentication."
    echo "  -p PERSISTENT_ID  Persistent ID for the dataset."
    echo "  -u SERVER_URL     URL of the Dataverse server."
    echo "  -h                Display this help message."
    echo ""
    echo "Example: $0 -f '../var/something/' -t 'xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx' -p 'doi:10.5072/FK2/J8SJZB' -u 'https://127.0.0.1:8080'"
    echo ""
}

# Parse command line arguments
while getopts 'hf:t:p:u:' flag; do
    case "${flag}" in
        f) FOLDER="${OPTARG}" ;;
        t) API_TOKEN="${OPTARG}" ;;
        p) PERSISTENT_ID="${OPTARG}" ;;
        u) SERVER_URL="${OPTARG}" ;;
        h) show_help
           exit 0 ;;
        *) show_help
           exit 1 ;;
    esac
done

# Check if all required arguments are provided
if [ -z "$FOLDER" ] || [ -z "$API_TOKEN" ] || [ -z "$PERSISTENT_ID" ] || [ -z "$SERVER_URL" ]; then
    echo "Error: Missing arguments."
    show_help
    exit 1
fi

# Process SERVER_URL to ensure it has the correct protocol
if [[ "$SERVER_URL" =~ ^http:// ]]; then
    # Replace "http://" with "https://"
    SERVER_URL="${SERVER_URL/http:\/\//https://}"
elif ! [[ "$SERVER_URL" =~ ^https:// ]]; then
    # Add "https://" if no protocol is specified
    SERVER_URL="https://${SERVER_URL}"
fi

# Remove trailing slash if present
SERVER_URL="${SERVER_URL%/}"

# Change to the directory containing the FITS files
cd "$FOLDER" || { echo "Error: Directory $FOLDER does not exist."; exit 1; }

# Create a log file
echo -e "\n$PERSISTENT_ID" >> log.txt

# Iterate over each .fit file in the directory
if compgen -G "*.fits" > /dev/null; then
    for FILE in *.fits; do
        FILE="${FOLDER}/$FILE"
        echo "File Found $FILE"
        # Extract the star number from the filename
        # This assumes the star number is after an underscore and before the .fit extension
        STARNUM=$(echo "$FILE" | sed 's/.*_\([0-9]*\)\.fit/\1/')
        echo "$STARNUM"
        
        # Define the JSON data for the curl command.
        # Might need to set blank values to null or empty strings to for a few of these.
                    # --arg desc "This is a fits file for the star $STARNUM." \
                    # --arg dl "data/subdir1" \
                    # --argjson cat '["Data"]' \
                    # --arg restr "false" \
                    # --arg ti "false" \
        JSON_DATA=$(jq -n \
                    --arg desc "This is a fits file for the star $STARNUM." \
                    --arg dl "" \
                    --arg cat "" \
                    --arg restr "false" \
                    --arg ti "false" \
                    '{description: $desc, directoryLabel: $dl, categories: $cat, restrict: $restr, tabIngest: $ti}')

        # Echo the curl command for debugging
        # echo "curl -H \"X-Dataverse-key:$API_TOKEN\" -X POST -F \"file=@$FILE\" -F 'jsonData=$JSON_DATA' \"$SERVER_URL/api/datasets/:persistentId/add?persistentId=$PERSISTENT_ID\""
        echo -e "${FOLDER}/${FILE} \n $JSON_DATA" >> log.txt

        # Execute the curl command
        curl -H "X-Dataverse-key:$API_TOKEN" -X POST -F "file=@$FILE" -F "jsonData=$JSON_DATA" "$SERVER_URL/api/datasets/:persistentId/add?persistentId=$PERSISTENT_ID" >> log.txt 2>&1
        # Command based on https://guides.dataverse.org/en/latest/api/native-api.html#add-a-file-to-a-dataset
    done
else
    echo "No .fits files found."
fi
