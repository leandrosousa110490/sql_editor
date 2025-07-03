#!/usr/bin/env python3
"""
Test script for the new Polars-based CSV automation system.
This script tests the core functionality without the GUI.
"""

import os
import sys
import time
from csv_automation_polars import CSVAutomationWorkerPolars

def test_automation():
    """Test the automation system"""
    print("Testing Polars-based CSV Automation System")
    print("=" * 50)
    
    # Configuration for testing
    sources_config = [
        {
            'table_name': 'csv_data',
            'mode': 'csv_folder',
            'folder_path': 'C:/Users/nbaba/Desktop/csv',
            'file_type': 'csv'
        },
        {
            'table_name': 'excel_data', 
            'mode': 'excel_folder',
            'folder_path': 'C:/Users/nbaba/Desktop/test_excel_bulk',
            'file_type': 'excel'
        }
    ]
    
    db_path = 'C:/Users/nbaba/Desktop/sql_editor/main.duckdb'
    
    # Check if folders exist
    for config in sources_config:
        folder_path = config.get('folder_path')
        if folder_path and not os.path.exists(folder_path):
            print(f"Warning: Folder not found: {folder_path}")
            print(f"Skipping {config['table_name']}")
            continue
        else:
            print(f"✓ Found folder: {folder_path}")
    
    # Create a simple progress callback
    def progress_callback(value, message):
        print(f"[{value:3d}%] {message}")
    
    def error_callback(message):
        print(f"ERROR: {message}")
    
    def finished_callback(results):
        print("\n" + "=" * 50)
        print("PROCESSING COMPLETED!")
        print(f"Sources processed: {results['sources_processed']}")
        print(f"Total rows: {results['total_rows']:,}")
        print(f"Tables created: {', '.join(results['tables_created'])}")
        print(f"Execution time: {results['execution_time']:.2f} seconds")
        print("=" * 50)
    
    # Create worker (without QThread for testing)
    class TestWorker:
        def __init__(self, sources_config, db_path):
            self.sources_config = sources_config
            self.db_path = db_path
            self.connection = None
            self.cancel_requested = False
            self.current_progress = 0
            
            # Import the actual worker methods
            from csv_automation_polars import CSVAutomationWorkerPolars
            worker = CSVAutomationWorkerPolars(sources_config, db_path)
            
            # Copy methods
            self.connect_to_database = worker.connect_to_database
            self.get_file_size_mb = worker.get_file_size_mb
            self.normalize_schema = worker.normalize_schema
            self.discover_all_columns = worker.discover_all_columns
            self.process_file_to_db = worker.process_file_to_db
            self.process_large_file_chunked = worker.process_large_file_chunked
            self.process_folder = worker.process_folder
            self.process_single_file = worker.process_single_file
        
        def run(self):
            """Run the automation (copied from original worker)"""
            try:
                if not self.connect_to_database():
                    error_callback("Failed to connect to database")
                    return
                
                results = {
                    'sources_processed': 0,
                    'total_rows': 0,
                    'tables_created': [],
                    'execution_time': 0
                }
                
                start_time = time.time()
                total_sources = len(self.sources_config)
                
                for i, source_config in enumerate(self.sources_config):
                    if self.cancel_requested:
                        return
                    
                    self.current_progress = int((i / total_sources) * 90)
                    table_name = source_config['table_name']
                    mode = source_config.get('mode', 'csv_folder')
                    
                    progress_callback(
                        self.current_progress,
                        f"Processing source {i+1}/{total_sources}: {table_name}"
                    )
                    
                    try:
                        if mode in ['csv_folder', 'excel_folder']:
                            total_rows = self.process_folder(source_config)
                        else:
                            total_rows = self.process_single_file(source_config)
                        
                        results['sources_processed'] += 1
                        results['total_rows'] += total_rows
                        results['tables_created'].append(table_name)
                        
                        # Verify table creation
                        try:
                            result = self.connection.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
                            row_count = result[0] if result else 0
                            print(f"✓ Verified {table_name}: {row_count:,} rows")
                        except Exception as e:
                            print(f"✗ Error verifying table {table_name}: {e}")
                        
                    except Exception as e:
                        print(f"✗ Error processing source {table_name}: {e}")
                        error_callback(f"Error processing {table_name}: {str(e)}")
                        continue
                
                # Final progress
                execution_time = time.time() - start_time
                results['execution_time'] = execution_time
                
                progress_callback(
                    100,
                    f"Completed! Processed {results['sources_processed']} sources, {results['total_rows']:,} total rows"
                )
                
                finished_callback(results)
                
            except Exception as e:
                print(f"✗ Unexpected error in CSV automation: {e}")
                error_callback(f"Unexpected error: {str(e)}")
            finally:
                if self.connection:
                    self.connection.close()
    
    # Run the test
    print("\nStarting automation test...")
    worker = TestWorker(sources_config, db_path)
    worker.run()

if __name__ == '__main__':
    test_automation()