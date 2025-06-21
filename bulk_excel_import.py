"""
Bulk Excel Import Module using Polars for Extremely Fast Processing
Includes automatic schema evolution - creates missing columns automatically
"""

import os
import sys
import glob
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple, Set
import time
import re

try:
    import polars as pl
    POLARS_AVAILABLE = True
except ImportError:
    POLARS_AVAILABLE = False
    print("Polars not installed. Using pandas fallback.")

import duckdb
import sqlite3
import pandas as pd
from PyQt6.QtWidgets import (QDialog, QVBoxLayout, QHBoxLayout, QPushButton, 
                             QLabel, QFileDialog, QProgressBar, QTextEdit, 
                             QGroupBox, QCheckBox, QSpinBox, QComboBox,
                             QTableWidget, QTableWidgetItem, QTabWidget,
                             QWidget, QSplitter, QMessageBox, QLineEdit,
                             QRadioButton, QButtonGroup)
from PyQt6.QtCore import QThread, pyqtSignal, Qt, QTimer
from PyQt6.QtGui import QFont
import qtawesome as qta


class SchemaEvolutionEngine:
    """Handles automatic schema evolution for database tables"""
    
    def __init__(self, connection, connection_info):
        self.connection = connection
        self.connection_info = connection_info
        self.db_type = connection_info['type'].lower()
        
    def get_table_schema(self, table_name: str) -> Dict[str, str]:
        """Get current table schema as column_name -> data_type mapping"""
        try:
            if self.db_type == 'duckdb':
                result = self.connection.execute(f"""
                    SELECT column_name, data_type 
                    FROM information_schema.columns 
                    WHERE table_name = '{table_name}'
                    AND table_schema = 'main'
                """).fetchall()
            else:  # sqlite
                cursor = self.connection.cursor()
                cursor.execute(f"PRAGMA table_info({table_name})")
                result = [(row[1], row[2]) for row in cursor.fetchall()]
            
            return {col_name: data_type for col_name, data_type in result}
        except:
            return {}
        
    def table_exists(self, table_name: str) -> bool:
        """Check if table exists"""
        try:
            if self.db_type == 'duckdb':
                result = self.connection.execute(f"""
                    SELECT COUNT(*) FROM information_schema.tables 
                    WHERE table_name = '{table_name}' AND table_schema = 'main'
                """).fetchone()
                return result[0] > 0
            else:  # sqlite
                cursor = self.connection.cursor()
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table_name,))
                return cursor.fetchone() is not None
        except:
            return False

    def pandas_to_sql_type(self, pandas_type: str, db_type: str) -> str:
        """Convert pandas data type to SQL data type"""
        type_mapping = {
            'duckdb': {
                'int64': 'BIGINT',
                'int32': 'INTEGER',
                'int16': 'SMALLINT',
                'int8': 'TINYINT',
                'float64': 'DOUBLE',
                'float32': 'REAL',
                'bool': 'BOOLEAN',
                'object': 'VARCHAR',
                'datetime64[ns]': 'TIMESTAMP',
                'category': 'VARCHAR'
            },
            'sqlite': {
                'int64': 'INTEGER',
                'int32': 'INTEGER',
                'int16': 'INTEGER',
                'int8': 'INTEGER',
                'float64': 'REAL',
                'float32': 'REAL',
                'bool': 'INTEGER',
                'object': 'TEXT',
                'datetime64[ns]': 'TEXT',
                'category': 'TEXT'
            }
        }
        
        pandas_str = str(pandas_type).lower()
        return type_mapping.get(db_type, {}).get(pandas_str, 'TEXT' if db_type == 'sqlite' else 'VARCHAR')

    def add_missing_columns(self, table_name: str, new_columns: Dict[str, str]) -> List[str]:
        """Add missing columns to existing table - only if table exists"""
        if not self.table_exists(table_name):
            return []  # Don't try to add columns to non-existent table
            
        added_columns = []
        
        for col_name, pandas_type in new_columns.items():
            try:
                sql_type = self.pandas_to_sql_type(pandas_type, self.db_type)
                
                if self.db_type == 'duckdb':
                    self.connection.execute(f'ALTER TABLE "{table_name}" ADD COLUMN "{col_name}" {sql_type}')
                else:  # sqlite
                    cursor = self.connection.cursor()
                    cursor.execute(f'ALTER TABLE "{table_name}" ADD COLUMN "{col_name}" {sql_type}')
                    self.connection.commit()
                
                added_columns.append(f"{col_name} ({sql_type})")
            except Exception as e:
                # Column might already exist or other error
                if "already exists" not in str(e).lower():
                    print(f"Failed to add column {col_name}: {e}")
        
        return added_columns

    def get_missing_columns(self, table_name: str, df_schema: Dict[str, str]) -> Dict[str, str]:
        """Get columns that exist in DataFrame but not in table"""
        if not self.table_exists(table_name):
            return {}  # No missing columns if table doesn't exist
        
        existing_schema = self.get_table_schema(table_name)
        existing_columns = set(existing_schema.keys())
        df_columns = set(df_schema.keys())
        
        missing_columns = df_columns - existing_columns
        return {col: df_schema[col] for col in missing_columns}


class BulkExcelProcessor:
    """High-performance bulk Excel processor using Polars or Pandas"""
    
    def __init__(self, connection, connection_info):
        self.connection = connection
        self.connection_info = connection_info
        self.schema_engine = SchemaEvolutionEngine(connection, connection_info)
        self.cancel_requested = False
        
    def find_excel_files(self, folder_path: str) -> List[str]:
        """Find all Excel files in folder and subfolders"""
        patterns = ['*.xlsx', '*.xls', '*.xlsm']
        excel_files = []
        
        for pattern in patterns:
            excel_files.extend(glob.glob(os.path.join(folder_path, '**', pattern), recursive=True))
        
        return sorted(excel_files)

    def clean_column_name(self, name: str) -> str:
        """Clean column name for SQL compatibility"""
        # Remove or replace problematic characters
        name = str(name).strip()
        name = re.sub(r'[^\w\s]', '_', name)  # Replace special chars with underscore
        name = re.sub(r'\s+', '_', name)      # Replace spaces with underscore
        name = re.sub(r'_+', '_', name)       # Replace multiple underscores with single
        name = name.strip('_')                # Remove leading/trailing underscores
        
        # Ensure it doesn't start with a number
        if name and name[0].isdigit():
            name = f"col_{name}"
        
        return name or "unnamed_column"

    def read_excel_optimized(self, file_path: str, sheet_name: Optional[str] = None) -> pd.DataFrame:
        """Read Excel file using the best available method"""
        try:
            if POLARS_AVAILABLE:
                # Try Polars first for maximum speed
                try:
                    df_pl = pl.read_excel(file_path, sheet_name=sheet_name)
                    df = df_pl.to_pandas()
                except:
                    # Fallback to pandas
                    df = pd.read_excel(file_path, sheet_name=sheet_name)
            else:
                # Use pandas directly
                df = pd.read_excel(file_path, sheet_name=sheet_name)
            
            # Clean column names
            df.columns = [self.clean_column_name(col) for col in df.columns]
            
            return df
            
        except Exception as e:
            raise Exception(f"Failed to read Excel file: {e}")

    def get_excel_sheets(self, file_path: str) -> List[str]:
        """Get list of sheet names in Excel file"""
        try:
            excel_file = pd.ExcelFile(file_path)
            return excel_file.sheet_names
        except:
            return []

    def align_dataframe_to_table(self, df: pd.DataFrame, table_name: str) -> pd.DataFrame:
        """Align DataFrame columns to match existing table schema"""
        if not self.schema_engine.table_exists(table_name):
            return df
            
        try:
            # Get existing table schema
            existing_schema = self.schema_engine.get_table_schema(table_name)
            if not existing_schema:
                return df
                
            existing_columns = list(existing_schema.keys())
            df_columns = list(df.columns)
            
            # Create a new DataFrame with all required columns
            aligned_df = pd.DataFrame()
            
            for col in existing_columns:
                if col in df_columns:
                    # Column exists in DataFrame
                    aligned_df[col] = df[col]
                else:
                    # Column missing in DataFrame - fill with None
                    aligned_df[col] = None
                    
            return aligned_df
            
        except Exception as e:
            print(f"Warning: Failed to align DataFrame to table schema: {e}")
            return df

    def process_single_file(self, file_path: str, table_name: str, mode: str = 'create_new', 
                          sheet_option: str = 'first', specific_sheet: str = None) -> Dict[str, Any]:
        """Process a single Excel file with specified sheet options"""
        start_time = time.time()
        total_rows = 0
        processed_sheets = []
        errors = []
        
        try:
            # Determine which sheets to process
            excel_file = pd.ExcelFile(file_path)
            all_sheet_names = excel_file.sheet_names
            
            if sheet_option == 'first':
                sheets_to_process = [all_sheet_names[0]] if all_sheet_names else []
            elif sheet_option == 'specific' and specific_sheet:
                sheets_to_process = [specific_sheet] if specific_sheet in all_sheet_names else []
            else:  # all_sheets
                sheets_to_process = all_sheet_names
            
            if not sheets_to_process:
                return {
                    'file_path': file_path,
                    'total_rows': 0,
                    'processed_sheets': [],
                    'errors': ['No sheets to process'],
                    'processing_time': time.time() - start_time
                }
            
            # Handle table creation/replacement at file level
            table_exists_before = self.schema_engine.table_exists(table_name) if mode != 'replace' else False
            table_created_this_file = False
            
            for i, sheet_name in enumerate(sheets_to_process):
                if self.cancel_requested:
                    break
                    
                try:
                    # Read sheet
                    df = self.read_excel_optimized(file_path, sheet_name)
                    
                    if len(df) == 0:
                        continue
                    
                    # Determine the mode for this sheet
                    added_columns = []
                    
                    if mode == 'replace' and not table_created_this_file:
                        # Drop table if exists and create new one (IF EXISTS handles both cases efficiently)
                        if self.connection_info['type'].lower() == 'duckdb':
                            self.connection.execute(f'DROP TABLE IF EXISTS "{table_name}"')
                        else:
                            cursor = self.connection.cursor()
                            cursor.execute(f'DROP TABLE IF EXISTS "{table_name}"')
                            self.connection.commit()
                        # Always create the table in replace mode
                        current_mode = 'create'
                        table_created_this_file = True
                    elif mode == 'create_new' and not table_exists_before and not table_created_this_file:
                        # Create new table (first sheet of first file)
                        current_mode = 'create'
                        table_created_this_file = True
                    elif mode == 'append_to_existing' and i == 0:
                        # This is a subsequent file in create_new mode - append to existing table
                        current_mode = 'append'
                        # Check for schema evolution (add missing columns to table)
                        df_schema = {col: str(df[col].dtype) for col in df.columns}
                        missing_columns = self.schema_engine.get_missing_columns(table_name, df_schema)
                        if missing_columns:
                            added_columns = self.schema_engine.add_missing_columns(table_name, missing_columns)
                        # Align DataFrame to match table schema
                        df = self.align_dataframe_to_table(df, table_name)
                    else:
                        # Subsequent sheets in same file - always append
                        current_mode = 'append'
                        # Check for schema evolution (add missing columns to table)
                        df_schema = {col: str(df[col].dtype) for col in df.columns}
                        missing_columns = self.schema_engine.get_missing_columns(table_name, df_schema)
                        if missing_columns:
                            added_columns = self.schema_engine.add_missing_columns(table_name, missing_columns)
                        # Align DataFrame to match table schema
                        df = self.align_dataframe_to_table(df, table_name)
                    
                    # Insert data
                    if self.connection_info['type'].lower() == 'duckdb':
                        self.fast_duckdb_insert(df, table_name, current_mode)
                    else:
                        self.fast_sqlite_insert(df, table_name, current_mode)
                    
                    total_rows += len(df)
                    processed_sheets.append({
                        'sheet': sheet_name,
                        'rows': len(df),
                        'columns': len(df.columns),
                        'added_columns': added_columns if current_mode == 'append' else []
                    })
                    
                except Exception as e:
                    errors.append(f"Sheet '{sheet_name}': {str(e)}")
                    
        except Exception as e:
            errors.append(f"File processing error: {str(e)}")
        
        processing_time = time.time() - start_time
        
        return {
            'file_path': file_path,
            'total_rows': total_rows,
            'processed_sheets': processed_sheets,
            'errors': errors,
            'processing_time': processing_time
        }

    def fast_duckdb_insert(self, df, table_name: str, mode: str):
        """Fast DuckDB insertion using native methods"""
        if mode == 'create':
            # Create table from DataFrame
            self.connection.execute(f'CREATE TABLE "{table_name}" AS SELECT * FROM df')
        else:  # append
            self.connection.execute(f'INSERT INTO "{table_name}" SELECT * FROM df')

    def fast_sqlite_insert(self, df, table_name: str, mode: str):
        """Fast SQLite insertion using optimized methods"""
        if mode == 'create':
            # Create new table
            df.to_sql(table_name, self.connection, if_exists='replace', index=False, method='multi', chunksize=5000)
        else:  # append
            df.to_sql(table_name, self.connection, if_exists='append', index=False, method='multi', chunksize=5000)


class BulkImportWorker(QThread):
    """Worker thread for bulk Excel import with progress reporting"""
    
    progress = pyqtSignal(int, str, dict)
    finished = pyqtSignal(bool, str, dict)
    error = pyqtSignal(str)
    
    def __init__(self, connection, connection_info, folder_path: str, table_name: str, 
                 mode: str = 'create_new', sheet_option: str = 'first', specific_sheet: str = None):
        super().__init__()
        self.connection = connection
        self.connection_info = connection_info
        self.folder_path = folder_path
        self.table_name = table_name
        self.mode = mode
        self.sheet_option = sheet_option
        self.specific_sheet = specific_sheet
        self.cancel_requested = False
        
    def cancel(self):
        self.cancel_requested = True
        
    def run(self):
        try:
            processor = BulkExcelProcessor(self.connection, self.connection_info)
            
            # Find all Excel files
            excel_files = processor.find_excel_files(self.folder_path)
            
            if not excel_files:
                self.finished.emit(False, "No Excel files found in the selected folder", {})
                return
            
            total_files = len(excel_files)
            processed_files = 0
            total_rows = 0
            total_errors = []
            file_results = []
            start_time = time.time()
            
            self.progress.emit(0, f"Found {total_files} Excel files to process", {})
            
            # For multiple files with create_new, only first file creates, rest append
            current_mode = self.mode
            
            for i, file_path in enumerate(excel_files):
                if self.cancel_requested:
                    break
                
                try:
                    processor.cancel_requested = self.cancel_requested
                    
                    # For create_new mode, only first file creates table
                    file_mode = current_mode
                    if current_mode == 'create_new' and i > 0:
                        file_mode = 'append_to_existing'
                    
                    result = processor.process_single_file(
                        file_path, self.table_name, file_mode, 
                        self.sheet_option, self.specific_sheet
                    )
                    
                    file_results.append(result)
                    total_rows += result['total_rows']
                    total_errors.extend(result['errors'])
                    processed_files += 1
                    
                    # After first file, subsequent files should append (for create_new mode)
                    if current_mode == 'create_new':
                        current_mode = 'append_to_existing'
                    
                    # Report progress
                    progress_percent = int((processed_files / total_files) * 100)
                    status = f"Processed {processed_files}/{total_files} files - {result['total_rows']} rows from {os.path.basename(file_path)}"
                    
                    self.progress.emit(progress_percent, status, result)
                    
                except Exception as e:
                    error_msg = f"Failed to process {os.path.basename(file_path)}: {str(e)}"
                    total_errors.append(error_msg)
                    file_results.append({
                        'file_path': file_path,
                        'total_rows': 0,
                        'processed_sheets': [],
                        'errors': [error_msg],
                        'processing_time': 0
                    })
            
            total_time = time.time() - start_time
            
            # Prepare summary
            summary = {
                'total_files': total_files,
                'processed_files': processed_files,
                'total_rows': total_rows,
                'total_errors': len(total_errors),
                'errors': total_errors,
                'file_results': file_results,
                'processing_time': total_time,
                'avg_time_per_file': total_time / max(processed_files, 1),
                'rows_per_second': total_rows / max(total_time, 0.001)
            }
            
            if self.cancel_requested:
                self.finished.emit(False, "Import cancelled by user", summary)
            elif total_errors:
                message = f"Import completed with {len(total_errors)} errors: {processed_files} files processed, {total_rows:,} rows imported"
                self.finished.emit(True, message, summary)
            else:
                message = f"Import successful: {processed_files} files processed, {total_rows:,} rows imported in {total_time:.2f}s"
                self.finished.emit(True, message, summary)
                
        except Exception as e:
            self.error.emit(f"Bulk import failed: {str(e)}")


class BulkExcelImportDialog(QDialog):
    """Dialog for configuring and running bulk Excel import"""
    
    def __init__(self, parent=None, connection=None, connection_info=None):
        super().__init__(parent)
        self.connection = connection
        self.connection_info = connection_info
        self.worker = None
        
        self.setWindowTitle("Bulk Excel Import - Polars Powered" if POLARS_AVAILABLE else "Bulk Excel Import")
        self.setModal(True)
        self.resize(900, 700)
        
        self.init_ui()
        
    def init_ui(self):
        layout = QVBoxLayout(self)
        
        # Title
        title_label = QLabel("🚀 Bulk Excel Import" + (" with Polars" if POLARS_AVAILABLE else ""))
        title_label.setStyleSheet("font-size: 16px; font-weight: bold; margin-bottom: 10px;")
        layout.addWidget(title_label)
        
        # Configuration Group
        config_group = QGroupBox("Import Configuration")
        config_layout = QVBoxLayout(config_group)
        
        # Folder selection
        folder_layout = QHBoxLayout()
        folder_layout.addWidget(QLabel("Excel Files Folder:"))
        
        self.folder_edit = QLineEdit()
        self.folder_edit.setPlaceholderText("Select folder containing Excel files...")
        folder_layout.addWidget(self.folder_edit)
        
        self.browse_button = QPushButton("Browse")
        self.browse_button.setIcon(qta.icon('fa5s.folder-open'))
        self.browse_button.clicked.connect(self.browse_folder)
        folder_layout.addWidget(self.browse_button)
        
        config_layout.addLayout(folder_layout)
        
        # Table configuration
        table_layout = QHBoxLayout()
        table_layout.addWidget(QLabel("Target Table:"))
        
        self.table_edit = QLineEdit()
        self.table_edit.setPlaceholderText("Enter table name (e.g., excel_data)")
        table_layout.addWidget(self.table_edit)
        
        config_layout.addLayout(table_layout)
        
        # Table mode selection
        mode_group = QGroupBox("Table Mode")
        mode_layout = QVBoxLayout(mode_group)
        
        self.mode_group = QButtonGroup()
        self.create_new_radio = QRadioButton("Create new table (or append if exists)")
        self.replace_radio = QRadioButton("Replace table (create if not exists)")
        
        self.create_new_radio.setChecked(True)
        self.mode_group.addButton(self.create_new_radio)
        self.mode_group.addButton(self.replace_radio)
        
        mode_layout.addWidget(self.create_new_radio)
        mode_layout.addWidget(self.replace_radio)
        config_layout.addWidget(mode_group)
        
        # Sheet selection
        sheet_group = QGroupBox("Sheet Selection")
        sheet_layout = QVBoxLayout(sheet_group)
        
        self.sheet_group = QButtonGroup()
        self.first_sheet_radio = QRadioButton("Import first sheet only")
        self.all_sheets_radio = QRadioButton("Import all sheets")
        self.specific_sheet_radio = QRadioButton("Import specific sheet:")
        
        self.first_sheet_radio.setChecked(True)
        self.sheet_group.addButton(self.first_sheet_radio)
        self.sheet_group.addButton(self.all_sheets_radio)
        self.sheet_group.addButton(self.specific_sheet_radio)
        
        sheet_layout.addWidget(self.first_sheet_radio)
        sheet_layout.addWidget(self.all_sheets_radio)
        
        specific_layout = QHBoxLayout()
        specific_layout.addWidget(self.specific_sheet_radio)
        self.specific_sheet_combo = QComboBox()
        self.specific_sheet_combo.setEnabled(False)
        specific_layout.addWidget(self.specific_sheet_combo)
        sheet_layout.addLayout(specific_layout)
        
        # Connect specific sheet radio to enable/disable combo
        self.specific_sheet_radio.toggled.connect(self.specific_sheet_combo.setEnabled)
        
        config_layout.addWidget(sheet_group)
        
        # Schema evolution
        self.schema_evolution_check = QCheckBox("Enable automatic schema evolution (add missing columns)")
        self.schema_evolution_check.setChecked(True)
        config_layout.addWidget(self.schema_evolution_check)
        
        layout.addWidget(config_group)
        
        # Preview Group
        preview_group = QGroupBox("File Preview")
        preview_layout = QVBoxLayout(preview_group)
        
        self.preview_button = QPushButton("Scan Folder for Excel Files")
        self.preview_button.setIcon(qta.icon('fa5s.search'))
        self.preview_button.clicked.connect(self.scan_folder)
        preview_layout.addWidget(self.preview_button)
        
        self.file_list = QTextEdit()
        self.file_list.setMaximumHeight(150)
        self.file_list.setReadOnly(True)
        preview_layout.addWidget(self.file_list)
        
        layout.addWidget(preview_group)
        
        # Progress Group
        progress_group = QGroupBox("Import Progress")
        progress_layout = QVBoxLayout(progress_group)
        
        self.progress_bar = QProgressBar()
        progress_layout.addWidget(self.progress_bar)
        
        self.status_label = QLabel("Ready to import")
        progress_layout.addWidget(self.status_label)
        
        # Results tabs
        self.results_tabs = QTabWidget()
        
        # Live progress tab
        self.progress_text = QTextEdit()
        self.progress_text.setMaximumHeight(150)
        self.progress_text.setReadOnly(True)
        self.progress_text.setFont(QFont("Consolas", 9))
        self.results_tabs.addTab(self.progress_text, "Live Progress")
        
        # Summary tab
        self.summary_text = QTextEdit()
        self.summary_text.setReadOnly(True)
        self.summary_text.setFont(QFont("Consolas", 9))
        self.results_tabs.addTab(self.summary_text, "Summary")
        
        progress_layout.addWidget(self.results_tabs)
        layout.addWidget(progress_group)
        
        # Buttons
        button_layout = QHBoxLayout()
        
        self.import_button = QPushButton("Start Bulk Import")
        self.import_button.setIcon(qta.icon('fa5s.rocket'))
        self.import_button.clicked.connect(self.start_import)
        button_layout.addWidget(self.import_button)
        
        self.cancel_button = QPushButton("Cancel")
        self.cancel_button.setIcon(qta.icon('fa5s.times'))
        self.cancel_button.clicked.connect(self.cancel_import)
        self.cancel_button.setEnabled(False)
        button_layout.addWidget(self.cancel_button)
        
        button_layout.addStretch()
        
        self.close_button = QPushButton("Close")
        self.close_button.setIcon(qta.icon('fa5s.door-open'))
        self.close_button.clicked.connect(self.close)
        button_layout.addWidget(self.close_button)
        
        layout.addLayout(button_layout)
        
    def browse_folder(self):
        folder = QFileDialog.getExistingDirectory(self, "Select Folder with Excel Files")
        if folder:
            self.folder_edit.setText(folder)
            self.update_sheet_options()
            
    def update_sheet_options(self):
        """Update sheet combo box with sheets from first Excel file"""
        folder_path = self.folder_edit.text().strip()
        if not folder_path or not os.path.exists(folder_path):
            return
            
        processor = BulkExcelProcessor(self.connection, self.connection_info)
        excel_files = processor.find_excel_files(folder_path)
        
        self.specific_sheet_combo.clear()
        
        if excel_files:
            try:
                # Get sheets from first Excel file as example
                sheets = processor.get_excel_sheets(excel_files[0])
                self.specific_sheet_combo.addItems(sheets)
            except:
                pass
            
    def scan_folder(self):
        folder_path = self.folder_edit.text().strip()
        if not folder_path or not os.path.exists(folder_path):
            QMessageBox.warning(self, "Invalid Folder", "Please select a valid folder path.")
            return
            
        processor = BulkExcelProcessor(self.connection, self.connection_info)
        excel_files = processor.find_excel_files(folder_path)
        
        if not excel_files:
            self.file_list.setText("No Excel files found in the selected folder.")
            return
            
        file_info = [f"Found {len(excel_files)} Excel files:\n"]
        total_size = 0
        
        for file_path in excel_files:
            try:
                size = os.path.getsize(file_path)
                total_size += size
                size_mb = size / (1024 * 1024)
                relative_path = os.path.relpath(file_path, folder_path)
                
                # Try to get sheet count
                try:
                    sheets = processor.get_excel_sheets(file_path)
                    sheet_count = len(sheets)
                    file_info.append(f"  • {relative_path} ({size_mb:.1f} MB, {sheet_count} sheets)")
                except:
                    file_info.append(f"  • {relative_path} ({size_mb:.1f} MB)")
            except:
                relative_path = os.path.relpath(file_path, folder_path)
                file_info.append(f"  • {relative_path} (size unknown)")
        
        file_info.append(f"\nTotal estimated size: {total_size / (1024 * 1024):.1f} MB")
        self.file_list.setText("\n".join(file_info))
        
        # Update sheet options after scan
        self.update_sheet_options()
        
    def start_import(self):
        # Validate inputs
        folder_path = self.folder_edit.text().strip()
        table_name = self.table_edit.text().strip()
        
        if not folder_path or not os.path.exists(folder_path):
            QMessageBox.warning(self, "Invalid Folder", "Please select a valid folder path.")
            return
            
        if not table_name:
            QMessageBox.warning(self, "Invalid Table Name", "Please enter a table name.")
            return
            
        if not self.connection:
            QMessageBox.warning(self, "No Connection", "No database connection available.")
            return
            
        # Determine mode
        mode = 'create_new' if self.create_new_radio.isChecked() else 'replace'
        
        # Determine sheet option
        if self.first_sheet_radio.isChecked():
            sheet_option = 'first'
            specific_sheet = None
        elif self.specific_sheet_radio.isChecked():
            sheet_option = 'specific'
            specific_sheet = self.specific_sheet_combo.currentText()
        else:
            sheet_option = 'all_sheets'
            specific_sheet = None
            
        # Start import
        self.import_button.setEnabled(False)
        self.cancel_button.setEnabled(True)
        self.progress_bar.setValue(0)
        self.progress_text.clear()
        self.summary_text.clear()
        
        self.worker = BulkImportWorker(
            self.connection,
            self.connection_info,
            folder_path,
            table_name,
            mode,
            sheet_option,
            specific_sheet
        )
        
        self.worker.progress.connect(self.update_progress)
        self.worker.finished.connect(self.import_finished)
        self.worker.error.connect(self.import_error)
        
        self.worker.start()
        
    def cancel_import(self):
        if self.worker and self.worker.isRunning():
            self.worker.cancel()
            self.status_label.setText("Cancelling import...")
            
    def update_progress(self, progress: int, status: str, file_result: dict):
        self.progress_bar.setValue(progress)
        self.status_label.setText(status)
        
        # Add to live progress
        timestamp = time.strftime("%H:%M:%S")
        if file_result.get('file_path'):
            filename = os.path.basename(file_result['file_path'])
            self.progress_text.append(f"[{timestamp}] {filename}: {file_result['total_rows']:,} rows, {file_result['processing_time']:.2f}s")
            
            if file_result.get('errors'):
                for error in file_result['errors']:
                    self.progress_text.append(f"    ERROR: {error}")
                    
        self.progress_text.verticalScrollBar().setValue(
            self.progress_text.verticalScrollBar().maximum()
        )
        
    def import_finished(self, success: bool, message: str, summary: dict):
        self.import_button.setEnabled(True)
        self.cancel_button.setEnabled(False)
        self.progress_bar.setValue(100 if success else 0)
        self.status_label.setText("Import completed" if success else "Import failed")
        
        # Update summary tab
        if summary:
            summary_text = [
                f"BULK EXCEL IMPORT SUMMARY",
                f"=" * 50,
                f"Total Files Found: {summary['total_files']}",
                f"Files Processed: {summary['processed_files']}",
                f"Total Rows Imported: {summary['total_rows']:,}",
                f"Total Processing Time: {summary['processing_time']:.2f} seconds",
                f"Average Time per File: {summary['avg_time_per_file']:.2f} seconds",
                f"Processing Speed: {summary['rows_per_second']:,.0f} rows/second",
                f"Errors: {summary['total_errors']}",
                f"",
                f"FILE DETAILS:",
                f"-" * 20
            ]
            
            for file_result in summary['file_results']:
                filename = os.path.basename(file_result['file_path'])
                summary_text.append(f"{filename}:")
                summary_text.append(f"  Rows: {file_result['total_rows']:,}")
                summary_text.append(f"  Time: {file_result['processing_time']:.2f}s")
                summary_text.append(f"  Sheets: {len(file_result['processed_sheets'])}")
                
                for sheet in file_result['processed_sheets']:
                    summary_text.append(f"    • {sheet['sheet']}: {sheet['rows']:,} rows, {sheet['columns']} cols")
                    if sheet['added_columns']:
                        summary_text.append(f"      Added columns: {', '.join(sheet['added_columns'])}")
                
                if file_result['errors']:
                    summary_text.append(f"  Errors: {len(file_result['errors'])}")
                    for error in file_result['errors']:
                        summary_text.append(f"    ✗ {error}")
                summary_text.append("")
            
            if summary['errors']:
                summary_text.extend([
                    f"ERRORS SUMMARY:",
                    f"-" * 15
                ])
                for error in summary['errors']:
                    summary_text.append(f"✗ {error}")
            
            self.summary_text.setText("\n".join(summary_text))
        
        # Switch to summary tab
        self.results_tabs.setCurrentIndex(1)
        
        # Show result message
        if success:
            QMessageBox.information(self, "Import Successful", message)
        else:
            QMessageBox.warning(self, "Import Failed", message)
            
    def import_error(self, error_message: str):
        self.import_button.setEnabled(True)
        self.cancel_button.setEnabled(False)
        self.status_label.setText("Import error")
        QMessageBox.critical(self, "Import Error", error_message)


def show_bulk_excel_import_dialog(parent=None, connection=None, connection_info=None):
    """Convenience function to show the bulk import dialog"""
    dialog = BulkExcelImportDialog(parent, connection, connection_info)
    return dialog.exec()


if __name__ == "__main__":
    print("Bulk Excel Import Module")
    print(f"Polars available: {POLARS_AVAILABLE}")
    print("This module provides high-performance bulk Excel import capabilities.") 