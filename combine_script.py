import os

def combine_files(file_list, output_file):
    """
    Combines the contents of a list of files into a single file,
    separated by triple quotes.

    Args:
        file_list: A list of file paths to be combined.
        output_file: The path to the output file.
    """
    first_file = True
    try:
        with open(output_file, 'w') as outfile:
            for file_path in file_list:
                try:
                    with open(file_path, 'r') as infile:
                        content = infile.read()
                        outfile.write("```\n")
                        outfile.write(f"Content from: {file_path}\n\n")
                        outfile.write(content)
                        outfile.write("\n```\n\n")
                except FileNotFoundError:
                    print(f"Warning: File not found at {file_path}. Skipping.")
                except Exception as e:
                    print(f"An error occurred while reading {file_path}: {e}")
        print(f"Successfully combined files into {output_file}")
    except Exception as e:
        print(f"An error occurred while writing to {output_file}: {e}")

if __name__ == '__main__':
    # --- Configuration ---
    # Add the paths to the files you want to combine into this list.
    # For example:
    # files_to_combine = ['/path/to/your/file1.txt', 'file2.log', 'data/file3.csv']
    files_to_combine = [
        'etl.py',
        'app.py',
        'database_manager.py',
        'requirements.txt'
    ]

    # Specify the name of the output file.
    combined_output_file = 'combined_output.txt'

    # --- Create dummy files for demonstration ---
    # This part is just to make the example runnable.
    # You can remove this section when using your own files.
    # print("Creating dummy files for demonstration...")
    # for f in files_to_combine:
    #     with open(f, 'w') as temp_file:
    #         temp_file.write(f"This is the content of {f}.\n")
    #         temp_file.write("It can have multiple lines.\n")
    # --- End of dummy file creation ---

    # Call the function to combine the files.
    combine_files(files_to_combine, combined_output_file)

    # --- Optional: Clean up dummy files ---
    # print("\nCleaning up dummy files...")
    # for f in files_to_combine:
    #     os.remove(f)
    # --- End of cleanup ---