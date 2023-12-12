#!/usr/bin/env python3
# python -m pip install astropy

import astropy.io.fits as fits

def extract_fits_metadata(file_path):
    """
    Extracts all metadata from a FITS file.

    :param file_path: Path to the FITS file.
    :return: Dictionary containing metadata.
    """
    with fits.open(file_path) as hdul:
        metadata = {}
        for i, hdu in enumerate(hdul):
            header = hdu.header
            metadata[f"HDU_{i}"] = {key: header[key] for key in header.keys()}
        return metadata

# Example usage
file_path = 'sample_fits/detectors.LD.Flow.LD.Occupancy.austria.graz.fits'
metadata = extract_fits_metadata(file_path)
print(metadata)
