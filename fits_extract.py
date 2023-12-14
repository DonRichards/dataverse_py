#!/usr/bin/env python3
# python -m pip install astropy

import argparse
import astropy.io.fits as fits
import json

def extract_fits_metadata(file_path):
    with fits.open(file_path) as hdul:
        metadata = {}
        for i, hdu in enumerate(hdul):
            header = hdu.header
            metadata[f"HDU_{i}"] = {key: str(header[key]) for key in header.keys()}
        return metadata

def main():
    parser = argparse.ArgumentParser(description="Extract metadata from a FITS file.")
    parser.add_argument("file_path", help="Path to the FITS file.")
    parser.add_argument("--pretty", action="store_true", help="Pretty-print the JSON output.")
    args = parser.parse_args()

    metadata = extract_fits_metadata(args.file_path)

    if args.pretty:
        print(json.dumps(metadata, indent=4))
    else:
        print(metadata)

if __name__ == "__main__":
    main()