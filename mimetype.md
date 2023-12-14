# Mime Type Checker

## Description
This script checks the existence of a specified file and determines its MIME type. It's useful for quickly identifying the type of a given file.

## Features
- Verifies the existence of a file at a given path.
- Determines the MIME type of the file.
- Special handling for FITS files, interpreting them as "image/fits".

## Usage
Run the script from the command line by passing the path to the file:

```shell
./mimetype.py path/to/your/file
```

Requirements
------------
*   Python 3
*   mimetype-description package (install via `pip install mimetype-description`)

Example
-------
```shell
./mimetype.py sample_file.fits
```

This will output the MIME type of 'sample\_file.fits'.

For help, run:

```shell
./mimetype.py --help
```