# import sys
# import re

# def clean_sql_data_to_csv(input_file_path, output_file_path):
#     """
#     Reads a file containing mixed formats (clean CSV and MySQL INSERT lines)
#     and writes a clean CSV file.

#     Args:
#         input_file_path (str): The path to the messy input file.
#         output_file_path (str): The path where the clean CSV will be saved.
#     """
#     print(f"Starting to process file: {input_file_path}")
    
#     try:
#         with open(input_file_path, 'r') as infile, open(output_file_path, 'w') as outfile:
#             lines_processed = 0
#             lines_cleaned = 0
            
#             for line in infile:
#                 # Strip leading/trailing whitespace from the line
#                 stripped_line = line.strip()
                
#                 # Skip empty lines
#                 if not stripped_line:
#                     continue

#                 cleaned_line = stripped_line
                
#                 # Check if the line is in the MySQL INSERT format, e.g., "(value1, 'value2', ...),"
#                 if stripped_line.startswith('(') and (stripped_line.endswith('),') or stripped_line.endswith(');')):
#                     # Remove the opening parenthesis at the start
#                     cleaned_line = cleaned_line[1:]
                    
#                     # Remove the closing parenthesis and comma/semicolon at the end
#                     cleaned_line = re.sub(r'\)[,;]?$', '', cleaned_line)
                    
#                     # Remove all single quotes used to wrap strings and numbers
#                     cleaned_line = cleaned_line.replace("'", "")
                    
#                     lines_cleaned += 1
                
#                 # Write the (now clean) line to the output file, followed by a newline
#                 outfile.write(cleaned_line + '\n')
#                 lines_processed += 1

#             print("-" * 30)
#             print("Processing Complete!")
#             print(f"Total lines processed: {lines_processed}")
#             print(f"Lines cleaned from SQL format: {lines_cleaned}")
#             print(f"Clean data saved to: {output_file_path}")

#     except FileNotFoundError:
#         print(f"Error: The file '{input_file_path}' was not found.")
#     except Exception as e:
#         print(f"An unexpected error occurred: {e}")

# # --- How to use this script ---
# if __name__ == "__main__":
#     # 1. Save your messy data into a file, for example, "messy_orderdetails.csv".
#     # 2. Change the file names below to match your input and desired output file names.
    
#     # Name of your input file (the one with mixed data)
#     input_filename = "orderdetails.csv" 
    
#     # Name for your new, clean output file
#     output_filename = "cleaned_orderdetails.csv" 
    
#     # Run the cleaning function
#     clean_sql_data_to_csv(input_filename, output_filename)


import re

def clean_orders_sql_to_csv(input_file_path, output_file_path):
    """
    Reads a SQL INSERT script for the 'orders' table and converts the
    data into a clean CSV file with a header row.

    This script specifically handles:
    - Extracting a header from a line like (`col1`,`col2`,...).
    - Parsing data rows like (val1, 'val2', NULL, ...).
    - Correctly converting SQL NULLs to empty CSV fields.
    - Removing SQL-specific formatting.

    Args:
        input_file_path (str): The path to the messy SQL input file.
        output_file_path (str): The path where the clean CSV will be saved.
    """
    print(f"Starting to process file: {input_file_path}")
    
    try:
        with open(input_file_path, 'r') as infile, open(output_file_path, 'w') as outfile:
            header_written = False
            lines_processed = 0
            
            for line in infile:
                stripped_line = line.strip()

                # Skip empty or non-relevant lines
                if not stripped_line:
                    continue

                # Attempt to find and write the header row
                if not header_written and stripped_line.startswith('(') and '`' in stripped_line:
                    # Extracts column names from a line like: (`orderNumber`,`orderDate`,...)
                    header = [name.strip() for name in stripped_line.replace('`', '').strip('(),').split(',')]
                    outfile.write(','.join(header) + '\n')
                    header_written = True
                    print("Header row created successfully.")
                    continue

                # Process data rows
                if stripped_line.startswith('(') and (stripped_line.endswith('),') or stripped_line.endswith(');')):
                    # This regex is robust enough to handle commas inside quoted strings
                    # It finds numbers, NULLs, or anything inside single quotes
                    values = re.findall(r"(\d+(?:\.\d+)?|'[^']*'|NULL)", stripped_line)
                    
                    cleaned_values = []
                    for value in values:
                        if value == 'NULL':
                            # Replace SQL NULL with an empty string for CSV
                            cleaned_values.append('')
                        else:
                            # Remove single quotes from the start and end of other values
                            cleaned_values.append(value.strip("'"))
                    
                    # Join the cleaned values with commas and write to the file
                    outfile.write(','.join(cleaned_values) + '\n')
                    lines_processed += 1

            if not header_written:
                 print("Warning: A header row was not found or created.")
            print("-" * 30)
            print("Processing Complete!")
            print(f"Data rows processed: {lines_processed}")
            print(f"Clean data saved to: {output_file_path}")

    except FileNotFoundError:
        print(f"Error: The file '{input_file_path}' was not found.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

# --- How to use this script ---
if __name__ == "__main__":
    # 1. Save your messy SQL data into a file, for example, "messy_orders.sql".
    #    Make sure the first line is the one with the column names.
    # 2. Change the file names below to match your input and desired output file names.
    
    # Name of your input file (the one with the SQL INSERT statements)
    input_filename = "raw_order_data.csv"
    
    # Name for your new, clean output file
    output_filename = "orders.csv"
    
    # Run the cleaning function
    clean_orders_sql_to_csv(input_filename, output_filename)
