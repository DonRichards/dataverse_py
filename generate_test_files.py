#!/usr/bin/env python3

#!/usr/bin/env python3

import argparse
import os
import random
import string
import sys

def random_string(length):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

def create_text_files(num_files, directory):
    if not os.path.exists(directory):
        os.makedirs(directory)

    for i in range(num_files):
        filename = random_string(random.randint(4, 18)) + ".txt"
        filepath = os.path.join(directory, filename)
        with open(filepath, 'w') as file:
            file.write(random_string(random.randint(10, 1000)))
        
        # Update counter
        remaining = num_files - i - 1
        print(f". Files remaining: {remaining}", end='\r')
        sys.stdout.flush()

    print("\nFile creation complete.")  # Newline after completion

def main():
    parser = argparse.ArgumentParser(description="Generate text files with random names and content.")
    parser.add_argument("num_files", type=int, help="Number of files to create.")
    parser.add_argument("directory", type=str, help="Directory where files will be created. If directory does not exist, it will be created.")
    args = parser.parse_args()

    create_text_files(args.num_files, args.directory)

if __name__ == "__main__":
    main()
