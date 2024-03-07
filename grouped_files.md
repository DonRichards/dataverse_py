# Grouping to zip large number of files

The primary purpose of this script is to group a large set of filenames based on numeric identifiers extracted from the filenames themselves. The script aims to create groups that contain roughly equal numbers of files, with the size of each group being approximately 1% of the total dataset by default (though this percentage can be adjusted via a command-line argument). The script also allows for the specification of a divisor to round the numeric identifiers for grouping, which can be useful for creating more manageable and human-readable groupings.

## Here's a step-by-step breakdown of how the script works

1. **Input and Argument Parsing:** The script expects a path to a file containing a list of filenames (one per line) as a required command-line argument (`--file_list_path`). It also accepts several optional arguments, including the divisor for rounding the numeric identifiers (`--divisor, default: 100000`), the desired percentage of files per group (`--percentage, default: 0.01`), a dry-run flag (`--dry_run`) to preview the results without writing an output file, and the output file path (`--output`, default: 'grouped_files.json').
1. **Reading and Preprocessing Filenames:** The script reads the filenames from the specified file list path and stores them in a Pandas DataFrame. It then applies the _extract_identifier_ function to each filename to extract the numeric identifiers, which are used as the basis for grouping.
1. **Calculating Group Sizes and Number of Groups:** The script calculates the target group size by multiplying the total number of files by the specified percentage (default: 1% of the total). It then determines the number of groups by dividing the total number of files by the target group size, rounding up if necessary. For example, if there are 401,000 files and the target group size is 4,010 (1% of 401,000), the script will create 100 groups (401,000 / 4,010 = 100).
1. **Grouping Files:** The script uses Pandas' qcut function to assign each file to a group based on its numeric identifier. The qcut function is designed to create roughly equal-sized groups based on quantile ranges.
1. **Rounding Group Ranges:** After grouping the files, the script determines the minimum and maximum numeric identifiers for each group. It then rounds down the minimum identifier and rounds up the maximum identifier to the nearest divisible value (e.g., if the divisor is 100000, and the minimum identifier is 123456, it will be rounded down to 100000). This rounding is done for clarity and ease of reference when working with the grouped data.
1. **Outputting Results:** The script generates a list of dictionaries, where each dictionary represents a group and contains the group number, the rounded range of numeric identifiers (e.g., "100000-199999"), and the list of filenames belonging to that group. If the `--dry_run` flag is not set, the script writes this list of dictionaries to the specified output JSON file. If the `--dry_run` flag is set, the script prints the data that would have been included in the output file without actually writing it.

In summary, this script is designed to take a large dataset of filenames, extract numeric identifiers from those filenames, and group the files into roughly equal-sized groups based on those identifiers. The resulting groups are represented as ranges of rounded numeric identifiers, and the script can optionally output this grouped data to a JSON file or print it to the console in a dry-run mode.


## Install libraries and run the script

```shell
python -m pip install argparse json pandas re

# To run the script
pipenv run python grouped_files.py --file_list_path /path/file_list_sorted.txt
```