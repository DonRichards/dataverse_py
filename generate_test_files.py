#!/usr/bin/env python3

import shutil
import argparse
import os
import random
import string
import sys
from astropy.io import fits

def random_string(length):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

def create_text_files(num_files, file_type, directory):
    if not os.path.exists(directory):
        os.makedirs(directory)

    for i in range(num_files):
        filename = random_string(random.randint(4, 18)) + "." + file_type
        filepath = os.path.join(directory, filename)
        if file_type == "fits":
            # Copy sample fits file to new file (filepath)
            sample_fits_file = os.path.join(os.path.dirname(__file__), "sample_fits/1904-66_CSC.fits")
            shutil.copyfile(sample_fits_file, filepath)
            with fits.open(filepath, mode='update') as hdul:
                hdr = hdul[0].header
                hdr['comment'] = "This is a sample FITS file " + str(i) + "." + str(filepath)
                hdul.verify('fix')
                hdul.flush()
        else:
            with open(filepath, 'w') as file:
                file.write(str(i) + "_" + random_string(random.randint(10, 1000)))
        remaining = num_files - i - 1
        print(f". Files remaining: {remaining}", end='\r')
        sys.stdout.flush()

    print(f"Files created: {num_files} --------> Directory: ./{directory}\n")

def main():
    parser = argparse.ArgumentParser(description="Generate text files with random names and content.")
    parser.add_argument("num_files", type=int, help="Number of files to create.")
    parser.add_argument("file_type", type=str, help="Type of files to create. Options: text, fits")
    parser.add_argument("directory", type=str, help="Directory where files will be created. If directory does not exist, it will be created.")
    args = parser.parse_args()

    create_text_files(args.num_files, args.file_type, args.directory)

if __name__ == "__main__":
    main()
