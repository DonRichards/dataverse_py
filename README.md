## Dataverse Python Script(s)
Script to automate the process of adding potentially thousands of ~~FITS~~ files to a simple DOI within a Dataverse server. The mimetypes is automatically detected and the appropriate metadata is extracted and added to the description of the DOI. If the mine type is not recognized, the script will assign it as a binary file (_application/octet-stream_) to prevent upload failure. Several [shape files mime types](https://en.wikipedia.org/wiki/Shapefile) are included in the script (_application/x-esri-shape_ & _application/x-qgis_) but more can be added as needed.

### Files
1. __py_add_fits_files_to_dio.py__: Rewrote Bash script behavior. see below
1. __fits_extract.py__: Extract FITS metadata. see below
1. __FITS_Description.md__: Describe FITS files and what is possible to extract. see below
1. __generate_test_files.md__: Generate test files. see below
1. __grouped_files.py__: Grouping to zip large number of files. see below

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

### Setup 'API_KEY' Before You Start
Before running the scripts, you need to obtain your API token from Dataverse. This is optional so that you don't have to enter the API key into the terminal and instead can pass __'$API_KEY'__ to the scripts.

1. Navigate to `[Site_URL]/dataverseuser.xhtml?selectTab=dataRelatedToMe` in your web browser.
1. Click on the "API Token" tab.
1. Copy the displayed token string.

Next, set the __'API_KEY'__ environment variable in your terminal:

#### For Linux and Mac:

Open your terminal and execute the following command, replacing __'xxxxxxxxxxxxxxxxxxxxxxxxxx'__ with your actual API token string:

```shell
export API_KEY='xxxxxxxxxxxxxxxxxxxxxxxxxx'
```

To make the __'API_KEY'__ persist across terminal sessions, you can add the above line to your __'~/.bashrc'__, __'~/.bash_profile'__, or __'~/.zshrc'__ file, depending on your shell and operating system.


#### For Windows:

Open Command Prompt or PowerShell and execute the following command, replacing xxxxxxxxxxxxxxxxxxxxxxxxxx with your actual API token string:

__Command Prompt:__
```shell
set API_KEY=xxxxxxxxxxxxxxxxxxxxxxxxxx
```

__PowerShell:__
```shell
$env:API_KEY='xxxxxxxxxxxxxxxxxxxxxxxxxx'
```
To make the __'API_KEY'__ persist across sessions in Windows, you can set it as a user or system environment variable through the System Properties. This can be accessed by searching for "Edit the system environment variables" in the Start menu. In the System Properties window, click on the "Environment Variables" button, and then you can add or edit the __'API_KEY'__ variable under either User or System variables as needed.

#### Suggested Setup (virtual environment) - _not required_
Install [pipenv](https://pipenv.pypa.io/en/latest/installation.html) to simplifying dependency management and providing consistent environments across different installations and it should avoid version conflicts with libraries already installed.
```shell
# Install pipenv (Linux)
python -m pip install pipenv
# OR (Mac)
brew install pyenv

# Linux
git clone https://github.com/pyenv/pyenv.git $(python -m site --user-base)/.pyenv
echo 'export PYENV_ROOT="$(python -m site --user-base)/.pyenv"' >> ~/.bashrc
echo 'export PATH="$PYENV_ROOT/bin:$PATH"' >> ~/.bashrc
echo 'eval "$(pyenv init --path)"' >> ~/.bashrc
source ~/.bashrc

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
python -m pip install dvuploader pyDataverse mimetype-description shutil grequests requests

# Optional Packages (for the fits_extract.py and group_files scripts)
python -m pip install astropy pandas
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

### Grouping to zip large number of files (grouped_files.py)
See [grouped_files.md](grouped_files.md) for details.

## Gotchas
1. __Processing order__: there is no telling the order at which the system is reading in the files. It does sort alphabetically but that doesn't mean it will process them in that order. This is important to know because the order of the files is important to the user.
1. __Subdirectories with FITS files__: if the directory has subdirectories we need to discuss the expected behavior and modify this code accordingly.
1. __Large Number of files__: will cause the script to take a long time to run. Ingestion of data in Dataverse is currently handled natively within Java, using a single-threaded process that reads each cell, row by row, column by column. The performance of ingestion mainly depends on the clock speed of a single core.

## ToDos
...

## Troubleshooting

- error:
  - `SystemError: (libev) error creating signal/async pipe: Too many open files`
- solution (for Mac & Linux):
  - `ulimit -n 4096`

## References
1. [Sample FITS File](https://open-bitbucket.nrao.edu/projects/CASA/repos/casatestdata/browse/fits/1904-66_CSC.fits)
