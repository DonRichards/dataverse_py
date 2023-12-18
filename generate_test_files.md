# Text File Generator

## Description
This Python script generates a specified number of text files with random alphanumeric names and content. It's useful for testing and creating dummy data sets.

## Features
- Generates a user-defined number of text files.
- Each file has a unique name consisting of 4-18 random alphanumeric characters.
- Each file contains random text of varying lengths (between 10 and 10000 characters).
- Includes a countdown displaying the number of files remaining to be created.

## Usage
1. Ensure you have Python installed on your system.
2. Place the script in a directory of your choice.
3. Run the script from the command line, specifying the number of files to create and the target directory. 


### Example: 
```shell
python generate_test_files.py 100 text ./generated_files
```
This command generates 100 text files in the `generated_files` directory. The other option is to create fits files. The command below creates 100 fits files in the `generated_files` directory.
```shell
python generate_test_files.py 100 fits ./generated_files
```

## Verify unique hashes
This will be 1 less than the number of files generated because the script itself is included in the count.
```shell
# Mac
find generated_files/ -type f -exec md5 -r {} + | awk '{print $1}' | sort | uniq | wc -l

# Linux
find generated_files/ -type f -exec md5sum -r {} + | awk '{print $1}' | sort | uniq | wc -l
```