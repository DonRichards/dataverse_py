## Dataverse Python Script(s)
Script to automate the process of adding potentially thousands of FITS files to a simple DOI within a Dataverse server.

### Files
1. __dd_fits_files_to_doi.sh__: Original bash file
1. __py_add_fits_files_to_dio.py__: Rewrote Bash script behavior
1. __fits_extract.py__: Extract FITS metadata
1. __FITS_Description.md__: Describe FITS files and what is possible to extract.

Found that using __pyDataverse.api__ added options that would have been difficult to replicate in a bash script.

### Install libraries
```shell
python3 -m pip install dvuploader pyDataverse
```

### py_add_fits_files_to_dio.py
Run help for options
```shell
./py_add_fits_files_to_dio.py --help
```

### fits_extract.py
Intended to be integrated into the py_add_fits_files_to_dio.py script to automate the extraction of the metadata and push it into the description.

Run help for options
```shell
./fits_extract.py --help
```
