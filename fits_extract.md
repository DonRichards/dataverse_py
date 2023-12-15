# FITS Metadata Extractor

## Description
This Python script extracts metadata from FITS (Flexible Image Transport System) files, commonly used in astronomy. It reads the file, gathers metadata from each Header/Data Unit (HDU), and outputs it in a readable format.

## Features
- Extracts and displays metadata from all HDUs in a FITS file.
- Option to pretty-print the output for enhanced readability.

## Usage
Run the script with the path to a FITS file:
```shell
./fits_extract.py path/to/your/fits_file.fits
```

For a pretty-printed JSON output:

```shell
./fits_extract.py path/to/your/fits_file.fits --pretty
```

For help
```shell
./fits_extract.py --help
```


Requirements
------------

*   Python 3
*   Astropy package (install via `pip install astropy`)
