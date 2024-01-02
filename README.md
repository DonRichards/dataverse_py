## Dataverse Python Script(s)
Script to automate the process of adding potentially thousands of ~~FITS~~ files to a simple DOI within a Dataverse server. The mimetypes is automatically detected and the appropriate metadata is extracted and added to the description of the DOI. If the mine type is not recognized, the script will assign it as a binary file (_application/octet-stream_) to prevent upload failure. Several [shape files mime types](https://en.wikipedia.org/wiki/Shapefile) are included in the script (_application/x-esri-shape_ & _application/x-qgis_) but more can be added as needed.

### Files
1. __dd_fits_files_to_doi.sh__: Original bash file. see below
1. __py_add_fits_files_to_dio.py__: Rewrote Bash script behavior. see below
1. __fits_extract.py__: Extract FITS metadata. see below
1. __FITS_Description.md__: Describe FITS files and what is possible to extract. see below
1. __generate_test_files.md__: Generate test files. see below

Found that using __pyDataverse.api__ added options that would have been difficult to replicate in a bash script.

### Requirement
1. Python 3 (tested on 3.10.12)
1. Python libraries "dvuploader" & "pyDataverse" installed.
1. Datatverse API Token (https://archive.data.jhu.edu/dataverseuser.xhtml) click on API Token tab after logging in
1. The DOI to run on
1. FITS files to process
  - All FITS files need to be together in a single directory (no subdirectories).

### There are 2 setups suggested in this README
- Python's Virtual Environment
- Locally set up: Utilizing `pip install` to configure the script's dependencies has been the conventional method for setting up Python scripts. However, this approach is becoming less favorable over time.

#### Suggested Setup (virtual environment) - _not required_
Install [pipenv](https://pipenv.pypa.io/en/latest/installation.html) to simplifying dependency management and providing consistent environments across different installations and it should avoid version conflicts with libraries already installed.
```shell
# Install pipenv (Linux)
python -m pip install pipenv
# OR (Mac)
brew install pyenv

# Install Python 3.10.12 using pyenv
pyenv install 3.10.12

# Create a virtual environment at a specific Python version
pipenv --python 3.10.12

# 2 Ways to install packages into the virtual environment.
# Either manually install packages into the virtual environment.
pipenv install dvuploader pyDataverse mimetype-description astropy shutil grequests requests
# OR use the Pipfile files (preferred).
# This is useful for ensuring consistent environments across different installations.
pipenv install

# Optional: To run the following commands as python instead of pipenv run python
# Run a shell within the virtual environment
pipenv shell
# To exit the shell
exit
# To remove the virtual environment
pipenv --rm
```

#### Install libraries (locally)
```shell
python -m pip install dvuploader pyDataverse mimetype-description

# Optional Packages (for the fits_extract.py script)
python -m pip install astropy
```

__Note__: "_./_" is a shorthand notation used by the computer to specify the execution of a file, especially when the file itself indicates that it's a Python script. In simpler terms, "_python foo.py_" and "_./foo.py_" essentially perform the same action.

To elaborate further, when running a script with pipenv instead of the local Python installation, you can simply replace the "_./_" notation with "_pipenv run python_" This allows you to execute the script within the virtual environment managed by pipenv.

__For Example__
```shell
# Run using the "Locally" installed
./py_add_fits_files_to_dio.py --help

# Run using pipenv
pipenv run python py_add_fits_files_to_dio.py --help
```

### File Descriptions
These files can be executed either independently or as dependencies within other scripts. As of the time of writing, there are no instances where they are being called as dependencies.

#### py_add_fits_files_to_dio.py
Run help for options
```shell
# Run using the "Locally" installed
./py_add_fits_files_to_dio.py --help

# Run using pipenv
pipenv run python py_add_fits_files_to_dio.py --help
```

## Support files
These file can be used independently of the main script.

### FITS Metadata Extractor (fits_extract.py)
See [fits_extract.md](fits_extract.md) for details.

### Generate Test Files (generate_test_files.py)
See [generate_test_files.md](generate_test_files.md) for details.

### Mime Type Checker (mimetype.py)
See [mimetype.md](mimetype.md) for details.

### Information on FITS files in general
See [FITS_Description.md](FITS_Description.md) for details.

## Gotchas
1. __Processing order__: there is no telling the order at which the system is reading in the files. If sorting them alphabetically or be creation date is needed please let me know.
1. __Subdirectories with FITS files__: if the directory has subdirectories we need to discuss the expected behavior and modify this code accordingly.

## ToDos
1. ~~Expand its capabilities to include more than just FITS files. This entails identifying the appropriate mimetypes and processing the files accordingly. While this enhancement wouldn't require significant effort, it may not provide immediate value to anyone.~~ ADDED

## References
1. [Sample FITS File](https://open-bitbucket.nrao.edu/projects/CASA/repos/casatestdata/browse/fits/1904-66_CSC.fits)