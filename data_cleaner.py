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
    # 1. Save your messy SQL data into a file, for example, "raw_data.sql".
    #    Make sure the first line is the one with the column names.
    # 2. Change the file names below to match your input and desired output file names.
    
    ##################################################################################
    ##################################################################################
    #                             Data Before Cleaning
    ##################################################################################
    ##################################################################################
    # (`orderNumber`,`orderDate`,`requiredDate`,`shippedDate`,`status`,`comments`,`customerNumber`),
    # (10100,'2003-01-06','2003-01-13','2003-01-10','Shipped',NULL,363),
    # (10101,'2003-01-09','2003-01-18','2003-01-11','Shipped','Check on availability.',128),
    # (10102,'2003-01-10','2003-01-18','2003-01-14','Shipped',NULL,181),
    # (10103,'2003-01-29','2003-02-07','2003-02-02','Shipped',NULL,121),
    # Name of your input file (the one with the SQL INSERT statements)
    ##################################################################################
    ##################################################################################
    #                             Data After Cleaning
    ##################################################################################
    ##################################################################################
    # orderNumber,orderDate,requiredDate,shippedDate,status,comments,customerNumber
    # 10100,2003-01-06,2003-01-13,2003-01-10,Shipped,,363
    # 10101,2003-01-09,2003-01-18,2003-01-11,Shipped,Check on availability.,128
    # 10102,2003-01-10,2003-01-18,2003-01-14,Shipped,,181
    # 10103,2003-01-29,2003-02-07,2003-02-02,Shipped,,121
    ##################################################################################
    ##################################################################################
    
    input_filename = "raw_data.csv"
    
    # Name for your new, clean output file
    output_filename = "cleaned_data.csv"
    
    # Run the cleaning function
    clean_orders_sql_to_csv(input_filename, output_filename)
