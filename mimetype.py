#!/usr/bin/env python3
# python -m pip install mimetype-description

import argparse
import os
import sys
from mimetype_description import guess_mime_type, get_mime_type_description

def checkargs():
    parser = argparse.ArgumentParser(description="Check if a file path exists.")
    parser.add_argument("file_path", help="The file path to check.")
    args = parser.parse_args()

    if not os.path.exists(args.file_path):
        parser.print_help()
        sys.exit(1)

    print(f"File path '{args.file_path}' was found!")
    return args

def main():
    args = checkargs()
    mime_type = guess_mime_type(args.file_path)
    # This will output a description of the mime type
    # description = get_mime_type_description(mime_type)

    # If mime_type is "application/fits", then set mime_type to "image/fits"
    if mime_type == "application/fits":
        mime_type = "image/fits"
    print(mime_type)

if __name__ == "__main__":
    main()