import argparse
import json
import pandas as pd
import re

def extract_identifier(filename):
    match = re.search(r'\d+', filename)
    return int(match.group()) if match else None

def round_down(num, divisor):
    return num - (num % divisor)

def round_up(num, divisor):
    if num % divisor == 0:
        return num
    return num + (divisor - num % divisor)

def process_file(file_list_path, divisor, num_groups, output_json_path, dry_run):
    filenames = [line.strip() for line in open(file_list_path, 'r').readlines()]
    df = pd.DataFrame({'filename': filenames})
    df['identifier'] = df['filename'].apply(extract_identifier)
    df = df.sort_values(by='identifier')

    # total_files = len(df)

    df['group'] = pd.qcut(df['identifier'], q=num_groups, labels=False, duplicates='drop')

    grouped = df.groupby('group').agg({
        'filename': list,
        'identifier': ['min', 'max']
    }).reset_index()

    grouped['rounded_min'] = grouped[('identifier', 'min')].apply(lambda x: round_down(x, divisor))
    grouped['rounded_max'] = grouped[('identifier', 'max')].apply(lambda x: round_up(x, divisor))

    grouped.columns = ['Group', 'Filenames', 'Min_Identifier', 'Max_Identifier', 'Rounded_Min', 'Rounded_Max']

    results = [{
        'Group': row['Group'],
        'Range': f"{row['Rounded_Min']}-{row['Rounded_Max']}",
        'Filenames': row['Filenames']
    } for _, row in grouped.iterrows()]

    if not dry_run:
        with open(output_json_path, 'w') as outfile:
            json.dump(results, outfile, indent=4)
        print(f"Output has been written to {output_json_path}")
    else:
        print("Dry run. No file will be written. Here's the data that would be included:")
        print(json.dumps(results, indent=4))

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Group files based on identifiers with rounding, adjustable number of groups, and optional JSON output.')
    parser.add_argument('--file_list_path', type=str, required=True, help='Path to the file list.')
    parser.add_argument('--divisor', type=int, default=100000, help='Divisor for rounding.')
    parser.add_argument('--num_groups', type=int, default=250, help='Number of groups to create.')
    parser.add_argument('--dry_run', action='store_true', help='Perform a dry run without actual processing.')
    parser.add_argument('--output', type=str, default='grouped_files.json', help='Output JSON file path.')
    args = parser.parse_args()

    process_file(args.file_list_path, args.divisor, args.num_groups, args.output, args.dry_run)