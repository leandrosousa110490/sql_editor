import pandas as pd
import os
import glob
from typing import Optional, Union, List
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def append_csv_files(
    input_folder: str,
    output_file: str,
    mode: str = 'create_new',
    fill_missing: Union[str, None] = None,
    ignore_index: bool = True,
    encoding: str = 'utf-8',
    file_pattern: str = '*.csv'
) -> pd.DataFrame:
    """
    Append all CSV files in a folder into a single DataFrame and optionally save to file.
    
    Parameters:
    -----------
    input_folder : str
        Path to the folder containing CSV files
    output_file : str
        Path where the merged CSV file will be saved
    mode : str, default 'create_new'
        - 'create_new': Create new file, fail if exists
        - 'replace': Replace existing file if it exists
        - 'append': Append to existing file (if output file exists)
    fill_missing : str or None, default None
        Value to fill missing columns with. If None, uses NaN
    ignore_index : bool, default True
        Whether to ignore the index when concatenating
    encoding : str, default 'utf-8'
        Encoding to use when reading CSV files
    file_pattern : str, default '*.csv'
        Pattern to match CSV files (e.g., '*.csv', 'data_*.csv')
    
    Returns:
    --------
    pd.DataFrame
        The merged DataFrame containing all CSV data
    
    Raises:
    -------
    FileExistsError
        If mode is 'create_new' and output file already exists
    FileNotFoundError
        If input folder doesn't exist or no CSV files found
    """
    
    # Validate input folder
    if not os.path.exists(input_folder):
        raise FileNotFoundError(f"Input folder '{input_folder}' does not exist")
    
    # Find all CSV files in the folder
    csv_pattern = os.path.join(input_folder, file_pattern)
    csv_files = glob.glob(csv_pattern)
    
    if not csv_files:
        raise FileNotFoundError(f"No CSV files found in '{input_folder}' matching pattern '{file_pattern}'")
    
    logger.info(f"Found {len(csv_files)} CSV files to merge")
    
    # Handle output file mode
    if os.path.exists(output_file):
        if mode == 'create_new':
            raise FileExistsError(f"Output file '{output_file}' already exists. Use mode='replace' to overwrite.")
        elif mode == 'replace':
            logger.info(f"Replacing existing file: {output_file}")
        elif mode == 'append':
            logger.info(f"Will append to existing file: {output_file}")
    
    # Read and collect all DataFrames
    dataframes = []
    file_info = []
    
    for i, csv_file in enumerate(csv_files):
        try:
            logger.info(f"Reading file {i+1}/{len(csv_files)}: {os.path.basename(csv_file)}")
            df = pd.read_csv(csv_file, encoding=encoding)
            
            # Add source file column for tracking
            df['_source_file'] = os.path.basename(csv_file)
            
            dataframes.append(df)
            file_info.append({
                'file': os.path.basename(csv_file),
                'rows': len(df),
                'columns': list(df.columns)
            })
            
        except Exception as e:
            logger.error(f"Error reading {csv_file}: {str(e)}")
            continue
    
    if not dataframes:
        raise ValueError("No CSV files could be successfully read")
    
    # Log column information
    all_columns = set()
    for info in file_info:
        all_columns.update(info['columns'])
    
    logger.info(f"Total unique columns found: {len(all_columns)}")
    
    # Check for column mismatches
    for info in file_info:
        missing_cols = all_columns - set(info['columns'])
        if missing_cols:
            logger.info(f"File '{info['file']}' missing columns: {list(missing_cols)}")
    
    # Concatenate all DataFrames
    # pandas.concat automatically handles mismatched columns by creating NaN values
    logger.info("Merging all CSV files...")
    merged_df = pd.concat(dataframes, ignore_index=ignore_index, sort=False)
    
    # Fill missing values if specified
    if fill_missing is not None:
        merged_df = merged_df.fillna(fill_missing)
    
    # Handle output based on mode
    if mode == 'append' and os.path.exists(output_file):
        # Read existing file and append
        try:
            existing_df = pd.read_csv(output_file, encoding=encoding)
            logger.info(f"Appending to existing file with {len(existing_df)} rows")
            final_df = pd.concat([existing_df, merged_df], ignore_index=True, sort=False)
        except Exception as e:
            logger.error(f"Error reading existing file for append: {str(e)}")
            logger.info("Saving as new file instead")
            final_df = merged_df
    else:
        final_df = merged_df
    
    # Save to output file
    logger.info(f"Saving merged data to: {output_file}")
    final_df.to_csv(output_file, index=False, encoding=encoding)
    
    # Log summary
    logger.info(f"Successfully merged {len(csv_files)} files")
    logger.info(f"Total rows in output: {len(final_df)}")
    logger.info(f"Total columns in output: {len(final_df.columns)}")
    
    return final_df


def get_csv_info(folder_path: str, file_pattern: str = '*.csv') -> List[dict]:
    """
    Get information about CSV files in a folder without loading them fully.
    
    Parameters:
    -----------
    folder_path : str
        Path to the folder containing CSV files
    file_pattern : str, default '*.csv'
        Pattern to match CSV files
    
    Returns:
    --------
    List[dict]
        List of dictionaries containing file information
    """
    csv_pattern = os.path.join(folder_path, file_pattern)
    csv_files = glob.glob(csv_pattern)
    
    file_info = []
    for csv_file in csv_files:
        try:
            # Read just the first few rows to get column info
            df_sample = pd.read_csv(csv_file, nrows=1)
            
            info = {
                'filename': os.path.basename(csv_file),
                'full_path': csv_file,
                'columns': list(df_sample.columns),
                'column_count': len(df_sample.columns)
            }
            file_info.append(info)
        except Exception as e:
            logger.error(f"Error reading {csv_file}: {str(e)}")
    
    return file_info


# Example usage and testing functions
if __name__ == "__main__":
    # Example usage
    try:
        # Example 1: Basic usage
        print("Example 1: Basic CSV merging")
        folder_path = "sample_csvs"  # Replace with your folder path
        output_path = "merged_output.csv"
        
        # Uncomment the following lines to test (make sure you have CSV files in the folder)
        # df = append_csv_files(folder_path, output_path, mode='replace')
        # print(f"Merged DataFrame shape: {df.shape}")
        
        # Example 2: Get CSV file information
        print("\nExample 2: Getting CSV file information")
        # info = get_csv_info(folder_path)
        # for file_info in info:
        #     print(f"File: {file_info['filename']}, Columns: {file_info['column_count']}")
        
        print("CSV merger module loaded successfully!")
        print("Use append_csv_files() function to merge CSV files.")
        
    except Exception as e:
        print(f"Example error: {str(e)}")
        print("This is expected if you don't have sample CSV files to test with.") 