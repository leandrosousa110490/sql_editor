import os
import sys
import time
import logging
import polars as pl
import duckdb
import glob
from PyQt6.QtCore import QThread, pyqtSignal, Qt
from PyQt6.QtWidgets import (
    QApplication, QMainWindow, QVBoxLayout, QWidget, QPushButton, QProgressBar, QLabel, QTextEdit,
    QDialog, QHBoxLayout, QFileDialog, QGroupBox, QListWidget, QLineEdit, QTabWidget,
    QScrollArea, QFormLayout, QPlainTextEdit, QFrame, QMessageBox,
    QListWidgetItem, QInputDialog, QComboBox
)
from PyQt6.QtGui import QFont
import re
import json
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def clean_column_name(col_name):
    """Clean column names: uppercase, replace spaces with underscores, remove special characters"""
    if not isinstance(col_name, str):
        col_name = str(col_name)
    
    # Convert to uppercase
    col_name = col_name.upper()
    
    # Replace spaces with underscores
    col_name = col_name.replace(' ', '_')
    
    # Remove special characters except underscores and alphanumeric
    col_name = re.sub(r'[^A-Z0-9_]', '', col_name)
    
    # Ensure it starts with a letter or underscore
    if col_name and not col_name[0].isalpha() and col_name[0] != '_':
        col_name = '_' + col_name
    
    # Handle empty names
    if not col_name:
        col_name = 'UNNAMED_COLUMN'
    
    return col_name

class CSVAutomationWorkerPolars(QThread):
    progress = pyqtSignal(int, str)
    error = pyqtSignal(str)
    finished = pyqtSignal(dict)
    
    def __init__(self, sources_config, db_path):
        super().__init__()
        self.sources_config = sources_config
        self.db_path = db_path
        self.connection = None
        self.cancel_requested = False
        self.current_progress = 0
        
    def connect_to_database(self):
        """Establish connection to DuckDB"""
        try:
            if self.connection:
                self.connection.close()
            self.connection = duckdb.connect(self.db_path)
            logger.info(f"Connected to DuckDB: {self.db_path}")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            return False
    
    def get_file_size_mb(self, file_path):
        """Get file size in MB"""
        try:
            return os.path.getsize(file_path) / (1024 * 1024)
        except:
            return 0
    
    def normalize_schema(self, df, target_columns):
        """Normalize DataFrame to match target schema with all string columns"""
        # Clean existing column names
        df = df.rename({col: clean_column_name(col) for col in df.columns})
        
        # Add missing columns as empty strings
        for col in target_columns:
            if col not in df.columns:
                df = df.with_columns(pl.lit("").alias(col))
        
        # Select only target columns in the correct order and cast all to string
        df = df.select([pl.col(col).cast(pl.Utf8).alias(col) for col in target_columns])
        
        return df
    
    def discover_all_columns(self, files, file_type):
        """Discover all unique columns across all files"""
        all_columns = set()
        
        self.progress.emit(self.current_progress, "Discovering column schema...")
        
        for i, file_path in enumerate(files):
            try:
                if file_type == 'excel':
                    # Read just the first few rows to get column names
                    try:
                        df = pl.read_excel(file_path, read_options={"n_rows": 1})
                    except Exception:
                        # Fallback: read without limiting rows
                        df = pl.read_excel(file_path)
                        if len(df) > 0:
                            df = df.head(1)
                else:
                    # Read just the first few rows to get column names
                    try:
                        df = pl.read_csv(file_path, n_rows=1)
                    except Exception:
                        # Fallback: read without limiting rows
                        df = pl.read_csv(file_path)
                        if len(df) > 0:
                            df = df.head(1)
                
                # Clean column names and add to set
                cleaned_cols = [clean_column_name(col) for col in df.columns]
                all_columns.update(cleaned_cols)
                
                # Update progress
                progress = int((i / len(files)) * 10)
                self.progress.emit(
                    self.current_progress + progress,
                    f"Scanning file {i+1}/{len(files)} for columns..."
                )
                
            except Exception as e:
                logger.warning(f"Could not read columns from {file_path}: {e}")
                continue
        
        # Add source file column
        all_columns.add('_SOURCE_FILE')
        
        # Convert to sorted list for consistent ordering
        return sorted(list(all_columns))
    
    def process_file_to_db(self, file_path, table_name, target_columns, file_type, mode='replace'):
        """Process a single file and load into database"""
        try:
            file_name = os.path.basename(file_path)
            file_size_mb = self.get_file_size_mb(file_path)
            
            self.progress.emit(
                self.current_progress,
                f"Processing {file_name} ({file_size_mb:.1f} MB)..."
            )
            
            # Read file with Polars, forcing all columns to string type
            if file_type == 'excel':
                df = pl.read_excel(file_path)
                # Convert all columns to string after reading
                df = df.select([pl.col(col).cast(pl.Utf8) for col in df.columns])
            else:
                # Read CSV with string schema to avoid type conflicts
                try:
                    df = pl.read_csv(file_path, dtypes={col: pl.Utf8 for col in target_columns if col != '_SOURCE_FILE'})
                except Exception:
                    # Fallback: read without specifying dtypes and convert to string
                    df = pl.read_csv(file_path)
                    df = df.select([pl.col(col).cast(pl.Utf8) for col in df.columns])
            
            # Add source file column
            df = df.with_columns(pl.lit(file_name).alias('_SOURCE_FILE'))
            
            # Normalize to target schema
            df = self.normalize_schema(df, target_columns)
            
            # Load into database
            if mode == 'replace':
                # Drop table if exists and create new
                self.connection.execute(f"DROP TABLE IF EXISTS {table_name}")
                self.connection.execute(f"CREATE TABLE {table_name} AS SELECT * FROM df")
            else:
                # Append to existing table
                self.connection.execute(f"INSERT INTO {table_name} SELECT * FROM df")
            
            logger.info(f"Successfully loaded {len(df)} rows from {file_name} into {table_name} (mode: {mode})")
            return len(df)
            
        except Exception as e:
            logger.error(f"Error processing {file_path}: {e}")
            raise
    
    def process_large_file_chunked(self, file_path, table_name, target_columns, file_type, chunk_size=50000, mode='replace'):
        """Process large files in chunks"""
        try:
            file_name = os.path.basename(file_path)
            total_rows = 0
            
            if file_type == 'excel':
                # For Excel, read entire file (Polars doesn't support chunked Excel reading)
                df = pl.read_excel(file_path)
                df = df.with_columns(pl.lit(file_name).alias('_SOURCE_FILE'))
                df = self.normalize_schema(df, target_columns)
                
                # Process in chunks
                for i in range(0, len(df), chunk_size):
                    if self.cancel_requested:
                        return total_rows
                    
                    chunk = df.slice(i, chunk_size)
                    
                    if i == 0 and mode == 'replace':
                        # First chunk - create table
                        self.connection.execute(f"DROP TABLE IF EXISTS {table_name}")
                        self.connection.execute(f"CREATE TABLE {table_name} AS SELECT * FROM chunk")
                    else:
                        # Subsequent chunks - append
                        self.connection.execute(f"INSERT INTO {table_name} SELECT * FROM chunk")
                    
                    total_rows += len(chunk)
                    
                    self.progress.emit(
                        self.current_progress + 10,
                        f"Loaded chunk {i//chunk_size + 1}: {total_rows:,} rows from {file_name}"
                    )
            else:
                # For CSV, read in batches using streaming approach
                try:
                    # Try to read with streaming batches
                    batch_reader = pl.read_csv_batched(
                        file_path, 
                        batch_size=chunk_size,
                        dtypes={col: pl.Utf8 for col in target_columns if col != '_SOURCE_FILE'}
                    )
                    
                    chunk_num = 0
                    while True:
                        if self.cancel_requested:
                            return total_rows
                        
                        try:
                            chunk = batch_reader.next_batches(1)
                            if not chunk:
                                break
                            chunk = chunk[0]
                        except StopIteration:
                            break
                        
                        if len(chunk) == 0:
                            break
                        
                        # Add source file and normalize
                        chunk = chunk.with_columns(pl.lit(file_name).alias('_SOURCE_FILE'))
                        chunk = self.normalize_schema(chunk, target_columns)
                        
                        if chunk_num == 0 and mode == 'replace':
                            # First chunk - create table
                            self.connection.execute(f"DROP TABLE IF EXISTS {table_name}")
                            self.connection.execute(f"CREATE TABLE {table_name} AS SELECT * FROM chunk")
                        else:
                            # Subsequent chunks - append
                            self.connection.execute(f"INSERT INTO {table_name} SELECT * FROM chunk")
                        
                        total_rows += len(chunk)
                        chunk_num += 1
                        
                        self.progress.emit(
                            self.current_progress + 10, 
                            f"Loaded chunk {chunk_num}: {total_rows:,} rows from {file_name}"
                        )
                        
                except Exception as batch_error:
                    # Fallback to regular reading if batched reading fails
                    logger.warning(f"Batched reading failed for {file_path}, using regular reading: {batch_error}")
                    
                    # Read entire file and process in memory chunks
                    df = pl.read_csv(file_path, dtypes={col: pl.Utf8 for col in target_columns if col != '_SOURCE_FILE'})
                    df = df.with_columns(pl.lit(file_name).alias('_SOURCE_FILE'))
                    df = self.normalize_schema(df, target_columns)
                    
                    # Process in chunks
                    for i in range(0, len(df), chunk_size):
                        if self.cancel_requested:
                            return total_rows
                        
                        chunk = df.slice(i, chunk_size)
                        
                        if i == 0 and mode == 'replace':
                            # First chunk - create table
                            self.connection.execute(f"DROP TABLE IF EXISTS {table_name}")
                            self.connection.execute(f"CREATE TABLE {table_name} AS SELECT * FROM chunk")
                        else:
                            # Subsequent chunks - append
                            self.connection.execute(f"INSERT INTO {table_name} SELECT * FROM chunk")
                        
                        total_rows += len(chunk)
                        
                        self.progress.emit(
                            self.current_progress + 10,
                            f"Loaded chunk {i//chunk_size + 1}: {total_rows:,} rows from {file_name}"
                        )
            
            return total_rows
            
        except Exception as e:
            logger.error(f"Error processing large file {file_path}: {e}")
            raise
    
    def process_folder(self, source_config):
        """Process all files in a folder"""
        folder_path = source_config.get('folder_path')
        table_name = source_config['table_name']
        file_type = source_config.get('file_type', 'csv')
        
        if not os.path.exists(folder_path):
            raise FileNotFoundError(f"Folder not found: {folder_path}")
        
        # Get all files of the specified type
        if file_type == 'excel':
            extensions = ['.xlsx', '.xls']
        else:
            extensions = ['.csv']
        
        files = []
        for ext in extensions:
            files.extend([os.path.join(folder_path, f) for f in os.listdir(folder_path) 
                         if f.lower().endswith(ext)])
        
        if not files:
            raise ValueError(f"No {file_type} files found in {folder_path}")
        
        logger.info(f"Found {len(files)} {file_type} files to process")
        
        # Discover all columns across all files
        target_columns = self.discover_all_columns(files, file_type)
        logger.info(f"Unified schema has {len(target_columns)} columns: {target_columns}")
        
        # Process each file
        total_rows = 0
        large_file_threshold = 50  # MB
        
        for i, file_path in enumerate(files):
            if self.cancel_requested:
                return total_rows
            
            file_size_mb = self.get_file_size_mb(file_path)
            mode = 'replace' if i == 0 else 'append'
            
            try:
                if file_size_mb > large_file_threshold:
                    # Process large files in chunks
                    rows_processed = self.process_large_file_chunked(
                        file_path, table_name, target_columns, file_type, mode=mode
                    )
                else:
                    # Process small files normally
                    rows_processed = self.process_file_to_db(
                        file_path, table_name, target_columns, file_type, mode=mode
                    )
                
                total_rows += rows_processed
                
                # Update progress
                file_progress = int(((i + 1) / len(files)) * 80)
                self.progress.emit(
                    self.current_progress + file_progress,
                    f"Completed {i+1}/{len(files)} files: {total_rows:,} total rows"
                )
                
            except Exception as e:
                logger.error(f"Error processing {file_path}: {e}")
                # Continue with other files instead of failing completely
                continue
        
        return total_rows
    
    def process_single_file(self, source_config):
        """Process a single file"""
        file_path = source_config.get('file_path')
        table_name = source_config['table_name']
        file_type = source_config.get('file_type', 'csv')
        
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")
        
        # For single files, discover columns from just this file
        target_columns = self.discover_all_columns([file_path], file_type)
        
        file_size_mb = self.get_file_size_mb(file_path)
        large_file_threshold = 50  # MB
        
        if file_size_mb > large_file_threshold:
            # Process large files in chunks
            total_rows = self.process_large_file_chunked(
                file_path, table_name, target_columns, file_type, mode='replace'
            )
        else:
            # Process small files normally
            total_rows = self.process_file_to_db(
                file_path, table_name, target_columns, file_type, mode='replace'
            )
        
        return total_rows
    
    def run(self):
        """Main execution method"""
        try:
            if not self.connect_to_database():
                self.error.emit("Failed to connect to database")
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
                
                self.progress.emit(
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
                        logger.info(f"Verified {table_name}: {row_count:,} rows")
                    except Exception as e:
                        logger.error(f"Error verifying table {table_name}: {e}")
                    
                except Exception as e:
                    logger.error(f"Error processing source {table_name}: {e}")
                    self.error.emit(f"Error processing {table_name}: {str(e)}")
                    continue
            
            # Final progress
            execution_time = time.time() - start_time
            results['execution_time'] = execution_time
            
            self.progress.emit(
                100,
                f"Completed! Processed {results['sources_processed']} sources, {results['total_rows']:,} total rows"
            )
            
            self.finished.emit(results)
            
        except Exception as e:
            logger.error(f"Unexpected error in CSV automation: {e}")
            self.error.emit(f"Unexpected error: {str(e)}")
        finally:
            if self.connection:
                self.connection.close()
    
    def cancel(self):
        """Cancel the operation"""
        self.cancel_requested = True

# Test GUI Application
class CSVAutomationApp(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("CSV Automation with Polars")
        self.setGeometry(100, 100, 600, 400)
        
        # Central widget
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        layout = QVBoxLayout(central_widget)
        
        # Progress bar
        self.progress_bar = QProgressBar()
        layout.addWidget(self.progress_bar)
        
        # Status label
        self.status_label = QLabel("Ready")
        layout.addWidget(self.status_label)
        
        # Log text area
        self.log_text = QTextEdit()
        self.log_text.setMaximumHeight(200)
        layout.addWidget(self.log_text)
        
        # Start button
        self.start_button = QPushButton("Start Processing")
        self.start_button.clicked.connect(self.start_processing)
        layout.addWidget(self.start_button)
        
        # Cancel button
        self.cancel_button = QPushButton("Cancel")
        self.cancel_button.clicked.connect(self.cancel_processing)
        self.cancel_button.setEnabled(False)
        layout.addWidget(self.cancel_button)
        
        self.worker = None
    
    def start_processing(self):
        """Start the CSV processing"""
        # Example configuration
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
        
        self.worker = CSVAutomationWorkerPolars(sources_config, db_path)
        self.worker.progress.connect(self.update_progress)
        self.worker.error.connect(self.show_error)
        self.worker.finished.connect(self.processing_finished)
        
        self.start_button.setEnabled(False)
        self.cancel_button.setEnabled(True)
        self.log_text.clear()
        
        self.worker.start()
    
    def cancel_processing(self):
        """Cancel the processing"""
        if self.worker:
            self.worker.cancel()
            self.worker.wait()
        
        self.start_button.setEnabled(True)
        self.cancel_button.setEnabled(False)
        self.status_label.setText("Cancelled")
    
    def update_progress(self, value, message):
        """Update progress bar and status"""
        self.progress_bar.setValue(value)
        self.status_label.setText(message)
        self.log_text.append(f"[{value}%] {message}")
    
    def show_error(self, message):
        """Show error message"""
        self.log_text.append(f"ERROR: {message}")
        self.start_button.setEnabled(True)
        self.cancel_button.setEnabled(False)
    
    def processing_finished(self, results):
        """Handle processing completion"""
        self.start_button.setEnabled(True)
        self.cancel_button.setEnabled(False)
        
        summary = f"""Processing completed!
Sources processed: {results['sources_processed']}
Total rows: {results['total_rows']:,}
Tables created: {', '.join(results['tables_created'])}
Execution time: {results['execution_time']:.2f} seconds"""
        
        self.log_text.append(summary)
        self.status_label.setText("Completed")

class CSVSourceWidget(QWidget):
    """Widget for configuring a single CSV source"""
    
    def __init__(self, parent=None, index=0, dialog=None):
        super().__init__(parent)
        self.index = index
        self.dialog = dialog
        self.init_ui()
    
    def init_ui(self):
        layout = QVBoxLayout(self)
        layout.setContentsMargins(5, 10, 5, 10)
        
        # Source header
        header_layout = QHBoxLayout()
        
        source_label = QLabel(f"Source #{self.index + 1}")
        source_label.setFont(QFont("Arial", 12, QFont.Weight.Bold))
        header_layout.addWidget(source_label)
        
        header_layout.addStretch()
        
        # Remove button
        if self.index > 0:  # Allow removing all but the first source
            remove_btn = QPushButton("Remove")
            remove_btn.setMaximumWidth(80)
            remove_btn.clicked.connect(self.remove_source)
            header_layout.addWidget(remove_btn)
        
        layout.addLayout(header_layout)
        
        # Add a separator line
        line = QFrame()
        line.setFrameShape(QFrame.Shape.HLine)
        line.setFrameShadow(QFrame.Shadow.Sunken)
        layout.addWidget(line)
        
        # Form layout for source configuration
        form_layout = QFormLayout()
        form_layout.setContentsMargins(10, 5, 10, 5)
        
        # Source type selection
        self.source_type = QComboBox()
        self.source_type.addItems([
            "CSV Folder", 
            "Excel Folder", 
            "Single CSV File", 
            "Single Excel File"
        ])
        self.source_type.currentIndexChanged.connect(self.update_source_type)
        form_layout.addRow("Source Type:", self.source_type)
        
        # Path selection
        path_layout = QHBoxLayout()
        self.path_line = QLineEdit()
        self.path_line.setPlaceholderText("Select folder or file path...")
        path_layout.addWidget(self.path_line)
        
        self.browse_btn = QPushButton("Browse...")
        self.browse_btn.clicked.connect(self.browse_path)
        path_layout.addWidget(self.browse_btn)
        
        form_layout.addRow("Path:", path_layout)
        
        # Excel sheet selection (initially hidden)
        self.sheet_selection = QComboBox()
        self.sheet_selection.addItems(["First sheet only", "All sheets (combined)"])
        self.sheet_selection.setVisible(False)
        form_layout.addRow("Sheet Selection:", self.sheet_selection)
        
        # Sheet name (initially hidden)
        self.sheet_name = QLineEdit()
        self.sheet_name.setPlaceholderText("Leave empty for first sheet")
        self.sheet_name.setVisible(False)
        form_layout.addRow("Sheet Name:", self.sheet_name)
        
        # Table name
        self.table_line = QLineEdit()
        self.table_line.setPlaceholderText("Enter table name for this source...")
        form_layout.addRow("Table Name:", self.table_line)
        
        layout.addLayout(form_layout)
        
        # Update UI based on initial source type
        self.update_source_type()
    
    def update_source_type(self, index=None):
        """Update UI based on selected source type"""
        source_type = self.source_type.currentText().lower()
        
        # Show/hide Excel-specific controls
        is_excel = 'excel' in source_type
        self.sheet_selection.setVisible(is_excel)
        self.sheet_name.setVisible(is_excel and self.sheet_selection.currentText() == "First sheet only")
        
        # Update browse button behavior
        is_folder = 'folder' in source_type
        self.browse_btn.setToolTip(f"Browse for {'folder' if is_folder else 'file'}")
        
        # Update table name placeholder if empty
        if not self.table_line.text():
            if 'csv' in source_type:
                self.table_line.setPlaceholderText("csv_data")
            elif 'excel' in source_type:
                self.table_line.setPlaceholderText("excel_data")
    
    def browse_path(self):
        """Open file or folder browser based on source type"""
        source_type = self.source_type.currentText().lower()
        is_folder = 'folder' in source_type
        
        if is_folder:
            path = QFileDialog.getExistingDirectory(self, "Select Folder")
        else:
            file_filter = "CSV Files (*.csv)" if 'csv' in source_type else "Excel Files (*.xlsx *.xls *.xlsm)"
            path, _ = QFileDialog.getOpenFileName(self, "Select File", filter=file_filter)
        
        if path:
            self.path_line.setText(path)
            
            # Auto-generate table name if empty
            if not self.table_line.text():
                base_name = os.path.basename(path)
                if os.path.isfile(path):
                    base_name = os.path.splitext(base_name)[0]
                
                # Clean the name for SQL
                table_name = re.sub(r'[^\w]', '_', base_name).lower()
                if table_name:
                    self.table_line.setText(table_name)
    
    def remove_source(self):
        """Remove this source from the parent dialog"""
        if self.dialog:
            self.dialog.remove_source(self.index)
    
    def get_config(self):
        """Get the configuration for this source"""
        source_type = self.source_type.currentText().lower()
        
        config = {
            'path': self.path_line.text(),
            'table_name': self.table_line.text(),
        }
        
        # Set mode based on source type
        if 'csv folder' in source_type:
            config['mode'] = 'csv_folder'
            config['file_type'] = 'csv'
        elif 'excel folder' in source_type:
            config['mode'] = 'excel_folder'
            config['file_type'] = 'excel'
        elif 'single csv' in source_type:
            config['mode'] = 'single_file'
            config['file_type'] = 'csv'
        elif 'single excel' in source_type:
            config['mode'] = 'single_file'
            config['file_type'] = 'excel'
        
        # Add Excel-specific settings
        if 'excel' in source_type:
            config['sheet_selection'] = self.sheet_selection.currentText()
            if self.sheet_selection.currentText() == "First sheet only":
                config['sheet_name'] = self.sheet_name.text()
        
        return config
    
    def set_config(self, config):
        """Set the widget from a configuration"""
        # Set path
        if 'path' in config:
            self.path_line.setText(config['path'])
        
        # Set table name
        if 'table_name' in config:
            self.table_line.setText(config['table_name'])
        
        # Set source type
        mode = config.get('mode', '')
        file_type = config.get('file_type', '')
        
        if mode == 'csv_folder' or (mode == 'folder' and file_type == 'csv'):
            self.source_type.setCurrentText("CSV Folder")
        elif mode == 'excel_folder' or (mode == 'folder' and file_type == 'excel'):
            self.source_type.setCurrentText("Excel Folder")
        elif mode == 'single_file' and file_type == 'csv':
            self.source_type.setCurrentText("Single CSV File")
        elif mode == 'single_file' and file_type == 'excel':
            self.source_type.setCurrentText("Single Excel File")
        
        # Set Excel-specific settings
        if 'excel' in file_type:
            if 'sheet_selection' in config:
                self.sheet_selection.setCurrentText(config['sheet_selection'])
            
            if 'sheet_name' in config:
                self.sheet_name.setText(config['sheet_name'])
        
        # Update UI based on selected type
        self.update_source_type()


class CSVAutomationDialog(QDialog):
    """Dialog for configuring and executing CSV automation"""
    
    def __init__(self, parent=None, connection=None, connection_info=None):
        super().__init__(parent)
        self.connection = connection
        self.connection_info = connection_info
        self.csv_sources = []
        self.worker = None
        
        # Store executed automation info for returning to main app
        self.executed_sql_query = None
        self.executed_output_table = None
        self.automation_results = None
        
        # Create automations directory if it doesn't exist
        self.automations_dir = "automations"
        os.makedirs(self.automations_dir, exist_ok=True)
        
        self.setWindowTitle("CSV Automation")
        self.setModal(True)
        self.resize(1200, 800)
        self.init_ui()
    
    def init_ui(self):
        layout = QVBoxLayout(self)
        
        # Header
        header_label = QLabel("CSV Automation - Process Multiple CSV Sources")
        header_label.setFont(QFont("Arial", 16, QFont.Weight.Bold))
        header_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        layout.addWidget(header_label)
        
        # Instructions
        instructions = QLabel(
            "1. Add CSV folder sources and specify table names\n"
            "2. Optional: Write SQL query to combine/transform data and execute automation"
        )
        instructions.setStyleSheet("color: gray; margin: 10px; padding: 10px; background-color: #f0f0f0;")
        layout.addWidget(instructions)
        
        # Main content with tabs
        self.tab_widget = QTabWidget()
        
        # Tab 1: Sources Configuration  
        sources_tab = QWidget()
        sources_main_layout = QHBoxLayout(sources_tab)
        
        # Left side - CSV Sources
        sources_left = QWidget()
        sources_layout = QVBoxLayout(sources_left)
        
        # Add source button
        self.add_source_btn = QPushButton("Add CSV Source")
        self.add_source_btn.clicked.connect(self.add_csv_source)
        sources_layout.addWidget(self.add_source_btn)
        
        # Sources scroll area
        self.sources_scroll = QScrollArea()
        self.sources_widget = QWidget()
        self.sources_layout = QVBoxLayout(self.sources_widget)
        self.sources_scroll.setWidget(self.sources_widget)
        self.sources_scroll.setWidgetResizable(True)
        sources_layout.addWidget(self.sources_scroll)
        
        sources_main_layout.addWidget(sources_left, 2)  # 2/3 of the space
        
        # Right side - Saved Automations
        automations_right = QWidget()
        automations_layout = QVBoxLayout(automations_right)
        
        automations_title = QLabel("Saved Automations")
        automations_title.setFont(QFont("Arial", 12, QFont.Weight.Bold))
        automations_layout.addWidget(automations_title)
        
        # Automations list
        self.automations_list = QListWidget()
        self.automations_list.setMaximumWidth(300)
        self.automations_list.itemDoubleClicked.connect(self.load_selected_automation)
        self.automations_list.setToolTip("Double-click to load automation for editing\nUse buttons below to Load, Run, or Delete")
        automations_layout.addWidget(self.automations_list)
        
        # Automation management buttons
        automation_buttons = QHBoxLayout()
        
        self.refresh_automations_btn = QPushButton("Refresh")
        self.refresh_automations_btn.clicked.connect(self.refresh_automations_list)
        self.refresh_automations_btn.setToolTip("Refresh the list of saved automations")
        automation_buttons.addWidget(self.refresh_automations_btn)
        
        self.load_selected_btn = QPushButton("Load")
        self.load_selected_btn.clicked.connect(self.load_selected_automation)
        self.load_selected_btn.setToolTip("Load selected automation for editing and review")
        automation_buttons.addWidget(self.load_selected_btn)
        
        self.run_selected_btn = QPushButton("Run")
        self.run_selected_btn.clicked.connect(self.run_selected_automation)
        self.run_selected_btn.setStyleSheet("QPushButton { background-color: #4CAF50; color: white; font-weight: bold; }")
        self.run_selected_btn.setToolTip("Load and immediately execute the selected automation")
        automation_buttons.addWidget(self.run_selected_btn)
        
        self.delete_automation_btn = QPushButton("Delete")
        self.delete_automation_btn.clicked.connect(self.delete_selected_automation)
        self.delete_automation_btn.setToolTip("Delete the selected automation permanently")
        automation_buttons.addWidget(self.delete_automation_btn)
        
        automations_layout.addLayout(automation_buttons)
        
        # Automation details
        self.automation_details = QLabel("Select an automation to see details")
        self.automation_details.setWordWrap(True)
        self.automation_details.setStyleSheet("color: gray; font-style: italic; padding: 10px;")
        automations_layout.addWidget(self.automation_details)
        
        automations_layout.addStretch()
        
        sources_main_layout.addWidget(automations_right, 1)  # 1/3 of the space
        
        self.tab_widget.addTab(sources_tab, "1. CSV Sources")
        
        # Tab 2: SQL Query
        sql_tab = QWidget()
        sql_layout = QVBoxLayout(sql_tab)
        
        # SQL header
        sql_title = QLabel("SQL Query (Optional)")
        sql_title.setFont(QFont("Arial", 14, QFont.Weight.Bold))
        sql_layout.addWidget(sql_title)
        
        sql_description = QLabel(
            "Write an SQL query to combine or transform your CSV data.\n"
            "Reference tables by the names specified in the Sources tab."
        )
        sql_description.setStyleSheet("color: gray;")
        sql_layout.addWidget(sql_description)
        
        # Use enhanced SQL editor if available
        try:
            from app import SQLTextEdit, SQLHighlighter
            self.sql_editor = SQLTextEdit()
            self.sql_editor.setFont(QFont("Consolas", 10))
            
            # Apply syntax highlighting
            self.sql_highlighter = SQLHighlighter(self.sql_editor.document())
        except ImportError:
            self.sql_editor = QPlainTextEdit()
            self.sql_editor.setFont(QFont("Consolas", 10))
        
        self.sql_editor.setPlaceholderText(
            "Example:\n"
            "SELECT table1.*, table2.additional_column\n"
            "FROM table1 \n"
            "LEFT JOIN table2 ON table1.id = table2.id\n"
            "WHERE table1.date >= '2024-01-01'"
        )
        sql_layout.addWidget(self.sql_editor)
        
        self.tab_widget.addTab(sql_tab, "2. SQL Query")
        
        layout.addWidget(self.tab_widget)
        
        # Progress section
        progress_group = QGroupBox("Progress")
        progress_layout = QVBoxLayout(progress_group)
        
        self.progress_bar = QProgressBar()
        self.progress_label = QLabel("Ready to start automation...")
        
        progress_layout.addWidget(self.progress_bar)
        progress_layout.addWidget(self.progress_label)
        
        layout.addWidget(progress_group)
        
        # Buttons
        button_layout = QHBoxLayout()
        
        # Save automation button
        self.save_automation_btn = QPushButton("Save Automation")
        self.save_automation_btn.clicked.connect(self.save_automation)
        button_layout.addWidget(self.save_automation_btn)
        
        button_layout.addStretch()  # Add some space between groups
        
        self.execute_btn = QPushButton("Execute Automation")
        self.execute_btn.clicked.connect(self.execute_automation)
        button_layout.addWidget(self.execute_btn)
        
        self.cancel_btn = QPushButton("Cancel")
        self.cancel_btn.clicked.connect(self.cancel_automation)
        self.cancel_btn.setEnabled(False)
        button_layout.addWidget(self.cancel_btn)
        
        self.close_btn = QPushButton("Close")
        self.close_btn.clicked.connect(self.close)
        button_layout.addWidget(self.close_btn)
        
        layout.addLayout(button_layout)
        
        # Initialize automations directory
        self.automations_dir = "automations"
        os.makedirs(self.automations_dir, exist_ok=True)
        
        # Load saved automations list
        self.refresh_automations_list()
        
        # Connect list selection to show details
        self.automations_list.itemSelectionChanged.connect(self.show_automation_details)
        
        # Add initial CSV source
        self.add_csv_source()
    
    def add_csv_source(self):
        """Add a new CSV source widget"""
        source_widget = CSVSourceWidget(self.sources_widget, len(self.csv_sources), self)
        self.csv_sources.append(source_widget)
        self.sources_layout.addWidget(source_widget)
    
    def remove_source(self, index):
        """Remove a CSV source widget"""
        if index < 0 or index >= len(self.csv_sources):
            return
        
        # Remove the widget from layout and list
        source_widget = self.csv_sources.pop(index)
        self.sources_layout.removeWidget(source_widget)
        source_widget.deleteLater()
        
        # Renumber remaining sources
        for i, source in enumerate(self.csv_sources):
            source.index = i
            source.init_ui()  # Reinitialize UI to update index in title
    
    def get_automation_config(self):
        """Get the current automation configuration"""
        sources = []
        for source_widget in self.csv_sources:
            config = source_widget.get_config()
            if config['path'] and config['table_name']:
                sources.append(config)
        
        sql_query = self.sql_editor.toPlainText().strip()
        
        # Output table name is derived from SQL query if provided
        output_table = None
        if sql_query:
            # Extract table name from first line if it's a CREATE TABLE statement
            lines = sql_query.split('\n')
            first_line = lines[0].strip().upper()
            if first_line.startswith('CREATE TABLE'):
                parts = first_line.split()
                if len(parts) > 2:
                    output_table = parts[2].strip(';')
            
            # If not a CREATE TABLE, generate a default output name
            if not output_table:
                output_table = f"query_result_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        return {
            'sources': sources,
            'sql_query': sql_query,
            'output_table': output_table
        }
    
    def set_automation_config(self, config):
        """Set the dialog from a configuration"""
        # Clear existing sources
        while self.csv_sources:
            self.remove_source(len(self.csv_sources) - 1)
        
        # Add sources from config
        sources = config.get('sources', [])
        for source_config in sources:
            self.add_csv_source()
            self.csv_sources[-1].set_config(source_config)
        
        # If no sources were added, add one empty source
        if not self.csv_sources:
            self.add_csv_source()
        
        # Set SQL query
        sql_query = config.get('sql_query', '')
        self.sql_editor.setPlainText(sql_query)
    
    def execute_automation(self):
        """Execute the automation with the current configuration"""
        config = self.get_automation_config()
        
        if not config['sources']:
            QMessageBox.warning(self, "Warning", "No valid sources configured. Please add at least one source with path and table name.")
            return
        
        # Prepare sources configuration for worker
        sources_config = []
        for source in config['sources']:
            source_config = {
                'table_name': source['table_name'],
                'mode': source['mode'],
                'file_type': source['file_type']
            }
            
            # Add path as folder_path or file_path based on mode
            if 'folder' in source['mode']:
                source_config['folder_path'] = source['path']
            else:
                source_config['file_path'] = source['path']
            
            # Add Excel-specific settings
            if source['file_type'] == 'excel':
                source_config['sheet_selection'] = source.get('sheet_selection', 'First sheet only')
                if source_config['sheet_selection'] == 'First sheet only' and source.get('sheet_name'):
                    source_config['sheet_name'] = source.get('sheet_name')
            
            sources_config.append(source_config)
        
        # Prepare output configuration
        output_config = {
            'table_name': config.get('output_table')
        }
        
        # Get SQL query if provided
        sql_query = config.get('sql_query', '').strip()
        
        # Validate connection info
        if not self.connection_info:
            raise ValueError("No database connection available. Please ensure you have a database connection established.")
        
        # Get database path from connection info
        db_path = self.connection_info.get('file_path') or self.connection_info.get('path')
        if not db_path:
            raise ValueError("No database path found in connection info. Please ensure you have a valid database connection.")
        
        # Create and start worker
        self.worker = CSVAutomationWorkerPolars(sources_config, db_path)
        
        # Connect signals
        self.worker.progress.connect(self.update_progress)
        self.worker.error.connect(self.show_error)
        self.worker.finished.connect(self.automation_finished)
        
        # Store executed info for returning to main app
        self.executed_sql_query = sql_query if sql_query else None
        self.executed_output_table = output_config['table_name'] if sql_query else None
        
        # Update UI
        self.progress_bar.setValue(0)
        self.progress_label.setText("Starting automation...")
        self.execute_btn.setEnabled(False)
        self.cancel_btn.setEnabled(True)
        self.save_automation_btn.setEnabled(False)
        
        # Start worker
        self.worker.start()
    
    def cancel_automation(self):
        """Cancel the running automation"""
        if self.worker and self.worker.isRunning():
            self.worker.cancel()
            self.progress_label.setText("Cancelling...")
    
    def update_progress(self, value, message):
        """Update progress bar and label"""
        self.progress_bar.setValue(value)
        self.progress_label.setText(message)
    
    def show_error(self, message):
        """Show error message"""
        QMessageBox.critical(self, "Error", message)
        self.progress_label.setText(f"Error: {message}")
        self.execute_btn.setEnabled(True)
        self.cancel_btn.setEnabled(False)
        self.save_automation_btn.setEnabled(True)
    
    def automation_finished(self, results):
        """Handle automation completion"""
        self.automation_results = results
        
        # Update UI
        self.execute_btn.setEnabled(True)
        self.cancel_btn.setEnabled(False)
        self.save_automation_btn.setEnabled(True)
        
        # Show summary
        summary = f"Automation completed!\n\n"
        summary += f"Sources processed: {results['sources_processed']}\n"
        summary += f"Total rows: {results['total_rows']:,}\n"
        summary += f"Tables created: {', '.join(results['tables_created'])}\n"
        summary += f"Execution time: {results['execution_time']:.2f} seconds"
        
        QMessageBox.information(self, "Automation Complete", summary)
    
    def refresh_automations_list(self):
        """Refresh the list of saved automations"""
        self.automations_list.clear()
        
        # Get all JSON files in automations directory
        automation_files = glob.glob(os.path.join(self.automations_dir, "*.json"))
        
        if not automation_files:
            item = QListWidgetItem("No saved automations")
            item.setData(Qt.ItemDataRole.UserRole, None)
            self.automations_list.addItem(item)
            return
        
        # Sort by modification time (newest first)
        automation_files.sort(key=lambda x: os.path.getmtime(x), reverse=True)
        
        for file_path in automation_files:
            try:
                # Get basic info without loading entire file
                filename = os.path.basename(file_path)
                mod_time = datetime.fromtimestamp(os.path.getmtime(file_path))
                mod_time_str = mod_time.strftime("%Y-%m-%d %H:%M")
                
                # Try to get source count from file
                source_count = "?"
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        config = json.load(f)
                        source_count = len(config.get('sources', []))
                except:
                    pass
                
                # Create list item
                display_name = f"{filename} ({source_count} sources, {mod_time_str})"
                item = QListWidgetItem(display_name)
                item.setData(Qt.ItemDataRole.UserRole, file_path)
                self.automations_list.addItem(item)
                
            except Exception as e:
                logger.error(f"Error loading automation file {file_path}: {e}")
    
    def show_automation_details(self):
        """Show details for the selected automation"""
        try:
            current_item = self.automations_list.currentItem()
            if not current_item or current_item.text() == "No saved automations":
                self.automation_details.setText("Select an automation to see details")
                return
            
            file_path = current_item.data(Qt.ItemDataRole.UserRole)
            if not file_path:
                return
            
            # Load configuration
            with open(file_path, 'r', encoding='utf-8') as f:
                config = json.load(f)
            
            # Build details text
            details = f"<b>File:</b> {os.path.basename(file_path)}<br>"
            details += f"<b>Sources:</b> {len(config.get('sources', []))}<br>"
            
            # List source details
            for i, source in enumerate(config.get('sources', [])):
                details += f"<br><b>Source #{i+1}:</b><br>"
                details += f"&nbsp;&nbsp;Type: {source.get('mode', 'unknown').replace('_', ' ').title()}<br>"
                details += f"&nbsp;&nbsp;Table: {source.get('table_name', 'unknown')}<br>"
                
                # Show path (truncated if too long)
                path = source.get('path', 'unknown')
                if len(path) > 40:
                    path = path[:20] + "..." + path[-17:]
                details += f"&nbsp;&nbsp;Path: {path}<br>"
            
            # Show SQL info
            sql_query = config.get('sql_query', '')
            if sql_query:
                sql_preview = sql_query[:100] + "..." if len(sql_query) > 100 else sql_query
                details += f"<br><b>SQL Query:</b> Yes<br>"
                details += f"<small>{sql_preview}</small>"
            else:
                details += f"<br><b>SQL Query:</b> No"
            
            self.automation_details.setText(details)
            self.automation_details.setTextFormat(Qt.TextFormat.RichText)
            
        except Exception as e:
            self.automation_details.setText(f"Error loading details: {str(e)}")
    
    def load_selected_automation(self):
        """Load the selected automation into the dialog"""
        try:
            current_item = self.automations_list.currentItem()
            if not current_item or current_item.text() == "No saved automations":
                QMessageBox.warning(self, "Warning", "Please select an automation to load.")
                return
            
            file_path = current_item.data(Qt.ItemDataRole.UserRole)
            if not file_path:
                return
            
            # Load configuration
            with open(file_path, 'r', encoding='utf-8') as f:
                config = json.load(f)
            
            # Apply configuration to dialog
            self.set_automation_config(config)
            
            # Switch to first tab
            self.tab_widget.setCurrentIndex(0)
            
            QMessageBox.information(
                self,
                "Success",
                f"Automation loaded successfully!\n\n"
                f"Sources: {len(config.get('sources', []))}\n"
                f"SQL Query: {'Yes' if config.get('sql_query') else 'No'}"
            )
            
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Failed to load selected automation:\n{str(e)}")
    
    def run_selected_automation(self):
        """Load and immediately run the selected automation"""
        try:
            current_item = self.automations_list.currentItem()
            if not current_item or current_item.text() == "No saved automations":
                QMessageBox.warning(self, "Warning", "Please select an automation to run.")
                return
            
            file_path = current_item.data(Qt.ItemDataRole.UserRole)
            if not file_path:
                return
            
            # Load configuration first
            with open(file_path, 'r', encoding='utf-8') as f:
                config = json.load(f)
            
            # Show confirmation dialog with automation details
            sources_count = len(config.get('sources', []))
            has_sql = bool(config.get('sql_query', '').strip())
            
            confirmation_text = (
                f"Are you sure you want to run this automation?\n\n"
                f"File: {os.path.basename(file_path)}\n"
                f"Sources: {sources_count}\n"
                f"SQL Query: {'Yes' if has_sql else 'No'}"
            )
            
            reply = QMessageBox.question(
                self,
                "Run Automation",
                confirmation_text,
                QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
                QMessageBox.StandardButton.No
            )
            
            if reply == QMessageBox.StandardButton.Yes:
                # Load the configuration
                self.set_automation_config(config)
                
                # Switch to progress view
                self.tab_widget.setCurrentIndex(0)
                
                # Execute immediately
                self.execute_automation()
            
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Failed to run selected automation:\n{str(e)}")
    
    def delete_selected_automation(self):
        """Delete the selected automation file"""
        try:
            current_item = self.automations_list.currentItem()
            if not current_item or current_item.text() == "No saved automations":
                QMessageBox.warning(self, "Warning", "Please select an automation to delete.")
                return
            
            file_path = current_item.data(Qt.ItemDataRole.UserRole)
            if not file_path:
                return
            
            filename = os.path.basename(file_path)
            
            # Confirm deletion
            reply = QMessageBox.question(
                self,
                "Confirm Deletion",
                f"Are you sure you want to delete this automation?\n\n{filename}",
                QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
                QMessageBox.StandardButton.No
            )
            
            if reply == QMessageBox.StandardButton.Yes:
                os.remove(file_path)
                self.refresh_automations_list()
                self.automation_details.setText("Automation deleted successfully")
                
                QMessageBox.information(self, "Success", f"Automation '{filename}' deleted successfully.")
            
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Failed to delete automation:\n{str(e)}")
    
    def save_automation(self):
        """Save the current automation configuration automatically to the automations directory"""
        try:
            config = self.get_automation_config()
            
            if not config['sources']:
                QMessageBox.warning(self, "Warning", "No valid sources configured to save.")
                return
            
            # Ask for automation name
            default_name = f"automation_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            automation_name, ok = QInputDialog.getText(
                self,
                "Save Automation",
                "Enter a name for this automation:",
                QLineEdit.EchoMode.Normal,
                default_name
            )
            
            if not ok or not automation_name.strip():
                return  # User cancelled or empty name
            
            # Clean the name for filename
            automation_name = automation_name.strip()
            # Remove invalid filename characters
            import re
            automation_name = re.sub(r'[<>:"/\\|?*]', '_', automation_name)
            
            # Ensure .json extension
            if not automation_name.endswith('.json'):
                automation_name += '.json'
            
            # Save to automations directory
            file_path = os.path.join(self.automations_dir, automation_name)
            
            # Check if file exists
            if os.path.exists(file_path):
                reply = QMessageBox.question(
                    self,
                    "File Exists",
                    f"An automation named '{automation_name}' already exists.\n\nDo you want to overwrite it?",
                    QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
                    QMessageBox.StandardButton.No
                )
                if reply == QMessageBox.StandardButton.No:
                    return
            
            # Save the automation
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(config, f, indent=2, ensure_ascii=False)
            
            # Refresh the automations list
            self.refresh_automations_list()
            
            # Select the newly saved automation in the list
            self.select_automation_by_name(automation_name)
            
            QMessageBox.information(
                self, 
                "Success", 
                f"Automation '{automation_name}' saved successfully!\n\nIt's now available in the Saved Automations panel."
            )
            
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Failed to save automation:\n{str(e)}")
    
    def select_automation_by_name(self, filename):
        """Select an automation in the list by filename"""
        try:
            for i in range(self.automations_list.count()):
                item = self.automations_list.item(i)
                if item and filename in item.text():
                    self.automations_list.setCurrentItem(item)
                    break
        except:
            pass  # If selection fails, it's not critical
    
    def get_executed_automation_info(self):
        """Get information about the executed automation for main app integration"""
        return {
            'sql_query': self.executed_sql_query,
            'output_table': self.executed_output_table,
            'results': self.automation_results,
            'has_sql_query': bool(self.executed_sql_query and self.executed_output_table)
        }


def show_csv_automation_dialog(parent=None, connection=None, connection_info=None):
    """Show the CSV automation dialog and return the dialog object for accessing results"""
    dialog = CSVAutomationDialog(parent, connection, connection_info)
    result = dialog.exec()
    # Return both the result and the dialog object so caller can access executed query info
    return result, dialog


if __name__ == '__main__':
    app = QApplication(sys.argv)
    window = CSVAutomationApp()
    window.show()
    sys.exit(app.exec())