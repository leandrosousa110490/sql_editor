"""
CSV Automation Module - Process Multiple CSV Sources with SQL
"""

import os
import glob
import pandas as pd
import duckdb
import time
import logging
import json
import gc
import psutil
from typing import List, Dict, Optional
from pathlib import Path
from datetime import datetime

from PyQt6.QtWidgets import (
    QDialog, QVBoxLayout, QHBoxLayout, QPushButton, QLabel, QFileDialog,
    QProgressBar, QTextEdit, QGroupBox, QListWidget, QLineEdit, QTabWidget, 
    QWidget, QScrollArea, QFormLayout, QPlainTextEdit, QFrame, QMessageBox,
    QListWidgetItem, QInputDialog, QComboBox
)
from PyQt6.QtCore import QThread, pyqtSignal, Qt
from PyQt6.QtGui import QFont
import qtawesome as qta

from csv_merger import append_csv_files, get_csv_info

# Import SQL editor components from main app
try:
    from app import SQLTextEdit, SQLHighlighter, ColorScheme
    SQL_EDITOR_AVAILABLE = True
except ImportError:
    SQL_EDITOR_AVAILABLE = False

logger = logging.getLogger(__name__)


class CSVSourceWidget(QWidget):
    """Widget for configuring a single CSV source"""
    
    def __init__(self, parent=None, source_index=0, dialog=None):
        super().__init__(parent)
        self.source_index = source_index
        self.dialog = dialog  # Reference to the main dialog
        self.init_ui()
    
    def init_ui(self):
        layout = QVBoxLayout(self)
        
        # Header
        header_frame = QFrame()
        header_frame.setFrameStyle(QFrame.Shape.StyledPanel)
        header_layout = QHBoxLayout(header_frame)
        
        self.title_label = QLabel(f"CSV Source {self.source_index + 1}")
        self.title_label.setFont(QFont("Arial", 12, QFont.Weight.Bold))
        header_layout.addWidget(self.title_label)
        
        header_layout.addStretch()
        
        # Mode toggle
        self.mode_combo = QComboBox()
        self.mode_combo.addItems(["Folder (Multiple Files)", "Single File"])
        self.mode_combo.currentTextChanged.connect(self.on_mode_changed)
        header_layout.addWidget(self.mode_combo)
        
        self.remove_btn = QPushButton("Remove")
        self.remove_btn.clicked.connect(self.request_remove)
        header_layout.addWidget(self.remove_btn)
        
        layout.addWidget(header_frame)
        
        # Configuration form
        self.form_layout = QFormLayout()
        
        # Path selection (folder or file depending on mode)
        path_layout = QHBoxLayout()
        self.path_line = QLineEdit()
        self.path_line.setPlaceholderText("Select CSV folder...")
        self.path_line.textChanged.connect(self.on_path_changed)
        
        self.browse_btn = QPushButton("Browse")
        self.browse_btn.clicked.connect(self.browse_path)
        
        path_layout.addWidget(self.path_line)
        path_layout.addWidget(self.browse_btn)
        
        # Store label references for dynamic updating
        self.path_label = QLabel("CSV Folder:")
        self.form_layout.addRow(self.path_label, path_layout)
        
        # Table name
        self.table_line = QLineEdit()
        self.table_line.setPlaceholderText("Enter table name")
        self.form_layout.addRow("Table Name:", self.table_line)
        
        # File pattern (only for folder mode)
        self.pattern_line = QLineEdit("*.csv")
        self.pattern_label = QLabel("File Pattern:")
        self.form_layout.addRow(self.pattern_label, self.pattern_line)
        
        layout.addLayout(self.form_layout)
        
        # File preview
        self.file_list = QListWidget()
        self.file_list.setMaximumHeight(80)
        self.preview_title = QLabel("Files Preview:")
        layout.addWidget(self.preview_title)
        layout.addWidget(self.file_list)
        
        self.preview_label = QLabel("No folder selected")
        self.preview_label.setStyleSheet("color: gray;")
        layout.addWidget(self.preview_label)
        
        # Set initial mode
        self.current_mode = "folder"
        self.update_ui_for_mode()
    
    def on_mode_changed(self):
        """Handle mode change between folder and single file"""
        current_text = self.mode_combo.currentText()
        if "Single File" in current_text:
            self.current_mode = "file"
        else:
            self.current_mode = "folder"
        self.update_ui_for_mode()
        
    def update_ui_for_mode(self):
        """Update UI elements based on current mode"""
        if self.current_mode == "file":
            # Single file mode
            self.path_label.setText("CSV File:")
            self.path_line.setPlaceholderText("Select CSV file...")
            self.pattern_line.setVisible(False)
            self.pattern_label.setVisible(False)
            self.preview_title.setText("File Preview:")
        else:
            # Folder mode
            self.path_label.setText("CSV Folder:")
            self.path_line.setPlaceholderText("Select CSV folder...")
            self.pattern_line.setVisible(True)
            self.pattern_label.setVisible(True)
            self.preview_title.setText("Files Preview:")
        
        # Update preview without clearing if we're loading configuration
        self.update_preview()
    
    def browse_path(self):
        """Browse for folder or file depending on mode"""
        if self.current_mode == "file":
            file_path, _ = QFileDialog.getOpenFileName(
                self, 
                "Select CSV File", 
                "", 
                "CSV Files (*.csv);;All Files (*)"
            )
            if file_path:
                self.path_line.setText(file_path)
        else:
            folder = QFileDialog.getExistingDirectory(self, "Select CSV Folder")
            if folder:
                self.path_line.setText(folder)
    
    def on_path_changed(self):
        """Handle path change for both folder and file modes"""
        path = self.path_line.text()
        if path and os.path.exists(path):
            self.update_preview()
            # Auto-suggest table name
            if not self.table_line.text():
                if self.current_mode == "file":
                    # Use filename without extension
                    suggested_name = os.path.splitext(os.path.basename(path))[0]
                else:
                    # Use folder name
                    suggested_name = os.path.basename(path.rstrip('/\\'))
                self.table_line.setText(self.clean_table_name(suggested_name))
    
    def update_preview(self):
        """Update preview for both folder and file modes"""
        self.file_list.clear()
        path = self.path_line.text()
        
        if not path or not os.path.exists(path):
            self.preview_label.setText("Invalid path")
            return
        
        if self.current_mode == "file":
            # Single file mode
            if path.lower().endswith('.csv'):
                filename = os.path.basename(path)
                self.file_list.addItem(filename)
                
                # Try to get file info
                try:
                    file_size = os.path.getsize(path)
                    size_mb = file_size / (1024 * 1024)
                    
                    # Try to read first few rows to get column info
                    df_sample = pd.read_csv(path, nrows=0)  # Just headers
                    col_count = len(df_sample.columns)
                    
                    self.preview_label.setText(f"File: {filename} ({size_mb:.1f} MB, {col_count} columns)")
                except Exception as e:
                    self.preview_label.setText(f"File: {filename} (Unable to read: {str(e)})")
            else:
                self.preview_label.setText("Selected file is not a CSV file")
        else:
            # Folder mode
            pattern = self.pattern_line.text() or "*.csv"
            csv_files = glob.glob(os.path.join(path, pattern))
            
            if csv_files:
                for file_path in sorted(csv_files)[:5]:  # Show max 5 files
                    filename = os.path.basename(file_path)
                    self.file_list.addItem(filename)
                
                if len(csv_files) > 5:
                    self.file_list.addItem(f"... and {len(csv_files) - 5} more files")
                
                self.preview_label.setText(f"Found {len(csv_files)} CSV files")
            else:
                self.preview_label.setText("No CSV files found")
    
    def clean_table_name(self, name):
        import re
        name = str(name).lower().strip()
        name = re.sub(r'[^\w]', '_', name)
        name = re.sub(r'_+', '_', name)
        return name.strip('_') or f"table_{self.source_index + 1}"
    
    def request_remove(self):
        if self.dialog:
            self.dialog.remove_source(self.source_index)
    
    def get_config(self):
        config = {
            'table_name': self.table_line.text(),
            'mode': self.current_mode
        }
        
        if self.current_mode == "file":
            config['file_path'] = self.path_line.text()
        else:
            config['folder_path'] = self.path_line.text()
            config['file_pattern'] = self.pattern_line.text() or "*.csv"
        
        return config
    
    def is_valid(self):
        config = self.get_config()
        path_exists = False
        
        if self.current_mode == "file":
            path_exists = config.get('file_path') and os.path.exists(config['file_path'])
        else:
            path_exists = config.get('folder_path') and os.path.exists(config['folder_path'])
        
        return path_exists and config['table_name']


class CSVAutomationWorker(QThread):
    """Worker thread for CSV automation processing"""
    
    progress = pyqtSignal(int, str)
    finished = pyqtSignal(bool, str, dict)
    error = pyqtSignal(str)
    
    def __init__(self, connection, sources_config, output_config, sql_query=None):
        super().__init__()
        self.connection = connection
        self.sources_config = sources_config
        self.output_config = output_config
        self.sql_query = sql_query
        self.cancel_requested = False
        
    def cancel(self):
        self.cancel_requested = True
    
    def get_file_size_mb(self, file_path):
        """Get file size in MB"""
        try:
            return os.path.getsize(file_path) / (1024 * 1024)
        except:
            return 0
    
    def check_memory_usage(self):
        """Check current memory usage and trigger garbage collection if needed"""
        try:
            # Get process memory info
            process = psutil.Process()
            memory_info = process.memory_info()
            memory_usage_mb = memory_info.rss / (1024 * 1024)
            
            # Get system memory info
            virtual_memory = psutil.virtual_memory()
            available_memory_mb = virtual_memory.available / (1024 * 1024)
            
            # If memory usage is high or available memory is low, trigger garbage collection
            if memory_usage_mb > 2000 or available_memory_mb < 500:  # 2GB process or <500MB available
                gc.collect()
                
                # Re-check after garbage collection
                memory_info = process.memory_info()
                memory_usage_mb = memory_info.rss / (1024 * 1024)
                
                self.progress.emit(
                    self.current_progress,
                    f"Memory management: {memory_usage_mb:.0f}MB used, {available_memory_mb:.0f}MB available"
                )
            
            # Return True if we have enough memory to continue
            return available_memory_mb > 200  # At least 200MB available
            
        except Exception:
            # If we can't check memory, assume it's OK
            return True
    
    def force_cleanup(self):
        """Force cleanup of temporary variables and garbage collection"""
        gc.collect()
        import threading
        if hasattr(threading, 'active_count'):
            # Log thread count for debugging
            logger.debug(f"Active threads: {threading.active_count()}")
    
    def cleanup_temp_files(self):
        """Clean up all temporary files created during processing"""
        try:
            import glob
            # Clean up temp files with various patterns
            temp_patterns = [
                "temp_*.csv",
                "temp_chunk_*.csv"
            ]
            
            for pattern in temp_patterns:
                for temp_file in glob.glob(pattern):
                    try:
                        os.remove(temp_file)
                        logger.debug(f"Cleaned up temporary file: {temp_file}")
                    except Exception as e:
                        logger.warning(f"Could not remove temp file {temp_file}: {e}")
        except Exception as e:
            logger.warning(f"Error during temp file cleanup: {e}")
    
    def process_large_csv_chunked(self, file_path, output_file, source_file_name, chunk_size=50000):
        """Process large CSV files in chunks to prevent memory issues"""
        try:
            total_rows = 0
            first_chunk = True
            
            # Read in chunks with flexible type handling
            try:
                # First try with normal pandas reading
                chunk_reader = pd.read_csv(file_path, encoding='utf-8', chunksize=chunk_size)
            except Exception as e:
                logger.warning(f"Standard CSV reading failed for {file_path}, trying with dtype=str: {e}")
                # Fallback to reading all columns as strings
                chunk_reader = pd.read_csv(file_path, encoding='utf-8', chunksize=chunk_size, dtype=str)
            
            for chunk_num, chunk in enumerate(chunk_reader):
                if self.cancel_requested:
                    return None
                
                # Add source file column
                chunk['_source_file'] = source_file_name
                
                # Write chunk (append after first chunk)
                mode = 'w' if first_chunk else 'a'
                header = first_chunk
                
                try:
                    chunk.to_csv(output_file, mode=mode, header=header, index=False, encoding='utf-8')
                except Exception as e:
                    logger.warning(f"Error writing chunk {chunk_num} from {file_path}: {e}")
                    # Try writing with string conversion
                    chunk_str = chunk.astype(str)
                    chunk_str.to_csv(output_file, mode=mode, header=header, index=False, encoding='utf-8')
                
                total_rows += len(chunk)
                first_chunk = False
                
                # Update progress
                self.progress.emit(
                    self.current_progress + 15,
                    f"Processed {total_rows:,} rows from {source_file_name}..."
                )
            
            return total_rows
            
        except Exception as e:
            logger.error(f"Error processing large file {file_path}: {str(e)}")
            raise ValueError(f"Failed to process large CSV file: {str(e)}")
    
    def process_csv_source_with_progress(self, source_config, output_file, source_name):
        """Process CSV source (folder or single file) with progress reporting and crash prevention"""
        
        # Memory management settings
        LARGE_FILE_THRESHOLD_MB = 100  # Files larger than 100MB are processed in chunks
        MAX_MEMORY_USAGE_MB = 1000     # Maximum memory usage before switching to chunked processing
        CHUNK_SIZE = 50000             # Rows per chunk for large files
        
        try:
            if source_config.get('mode') == 'file':
                # Single file processing with memory management
                file_path = source_config.get('file_path')
                if not file_path or not os.path.exists(file_path):
                    raise FileNotFoundError(f"CSV file not found: '{file_path}'")
                
                file_size_mb = self.get_file_size_mb(file_path)
                file_name = os.path.basename(file_path)
                
                self.progress.emit(
                    self.current_progress,
                    f"Processing file: {file_name} ({file_size_mb:.1f} MB)"
                )
                
                # Use chunked processing for large files
                if file_size_mb > LARGE_FILE_THRESHOLD_MB:
                    self.progress.emit(
                        self.current_progress + 5,
                        f"Large file detected - using memory-safe processing..."
                    )
                    
                    total_rows = self.process_large_csv_chunked(file_path, output_file, file_name, CHUNK_SIZE)
                    
                    self.progress.emit(
                        self.current_progress + 25,
                        f"Completed {total_rows:,} rows from {source_name}"
                    )
                    
                    # Return a minimal DataFrame with row count info for results
                    return pd.DataFrame({'_row_count': [total_rows], '_source_file': [file_name]})
                
                else:
                    # Normal processing for smaller files
                    self.progress.emit(
                        self.current_progress + 10,
                        f"Reading {file_name}..."
                    )
                    
                    try:
                        # Try reading with automatic type inference
                        df = pd.read_csv(file_path, encoding='utf-8')
                        df['_source_file'] = file_name
                        
                        self.progress.emit(
                            self.current_progress + 20,
                            f"Processing {len(df):,} rows from {source_name}..."
                        )
                        
                        # Save to output file
                        df.to_csv(output_file, index=False, encoding='utf-8')
                        
                        return df
                        
                    except (pd.errors.DtypeWarning, pd.errors.ParserError, ValueError) as e:
                        # Fallback to reading all columns as strings for mixed types
                        logger.warning(f"Type inference failed for {file_path}, reading as strings: {e}")
                        try:
                            df = pd.read_csv(file_path, encoding='utf-8', dtype=str)
                            df['_source_file'] = file_name
                            
                            self.progress.emit(
                                self.current_progress + 20,
                                f"Processing {len(df):,} rows from {source_name} (as text)..."
                            )
                            
                            df.to_csv(output_file, index=False, encoding='utf-8')
                            return df
                            
                        except Exception as e2:
                            logger.warning(f"String reading also failed for {file_path}, switching to chunked: {e2}")
                            total_rows = self.process_large_csv_chunked(file_path, output_file, file_name, CHUNK_SIZE)
                            return pd.DataFrame({'_row_count': [total_rows], '_source_file': [file_name]})
                        
                    except MemoryError:
                        # Fallback to chunked processing if we run out of memory
                        logger.warning(f"Memory error reading {file_path}, switching to chunked processing")
                        total_rows = self.process_large_csv_chunked(file_path, output_file, file_name, CHUNK_SIZE)
                        return pd.DataFrame({'_row_count': [total_rows], '_source_file': [file_name]})
            
            else:
                # Folder processing with memory management
                input_folder = source_config.get('folder_path')
                file_pattern = source_config.get('file_pattern', '*.csv')
                
                # Find all CSV files
                csv_pattern = os.path.join(input_folder, file_pattern)
                csv_files = glob.glob(csv_pattern)
                
                if not csv_files:
                    raise FileNotFoundError(f"No CSV files found in '{input_folder}' matching pattern '{file_pattern}'")
                
                # Calculate total size
                total_size_mb = sum(self.get_file_size_mb(f) for f in csv_files)
                total_files = len(csv_files)
                
                self.progress.emit(
                    self.current_progress,
                    f"Found {total_files} files ({total_size_mb:.1f} MB) in {source_name}"
                )
                
                # Use chunked processing if total size is large
                use_chunked = total_size_mb > MAX_MEMORY_USAGE_MB
                
                if use_chunked:
                    self.progress.emit(
                        self.current_progress + 2,
                        f"Large dataset detected - using memory-safe processing..."
                    )
                
                total_rows = 0
                first_file = True
                
                for i, csv_file in enumerate(csv_files):
                    if self.cancel_requested:
                        return None
                    
                    file_size_mb = self.get_file_size_mb(csv_file)
                    file_name = os.path.basename(csv_file)
                    
                    # Update progress for each file
                    file_progress = int((i / total_files) * 25)
                    self.progress.emit(
                        self.current_progress + file_progress,
                        f"Processing file {i+1}/{total_files}: {file_name} ({file_size_mb:.1f} MB)"
                    )
                    
                    try:
                        if use_chunked or file_size_mb > LARGE_FILE_THRESHOLD_MB:
                            # Process large files in chunks, append to output
                            import uuid
                            temp_output = f"temp_chunk_{i}_{uuid.uuid4().hex[:8]}.csv"
                            
                            if file_size_mb > LARGE_FILE_THRESHOLD_MB:
                                rows_processed = self.process_large_csv_chunked(csv_file, temp_output, file_name, CHUNK_SIZE)
                            else:
                                # Normal read but write to temp file
                                df = pd.read_csv(csv_file, encoding='utf-8')
                                df['_source_file'] = file_name
                                df.to_csv(temp_output, index=False, encoding='utf-8')
                                rows_processed = len(df)
                                del df  # Free memory immediately
                            
                            # Append temp file to main output
                            if first_file:
                                # First file - copy directly, remove destination if it exists
                                if os.path.exists(temp_output):
                                    try:
                                        # Remove destination file if it exists (Windows fix)
                                        if os.path.exists(output_file):
                                            os.remove(output_file)
                                        os.rename(temp_output, output_file)
                                    except OSError as e:
                                        # Fallback: copy content instead of rename
                                        logger.warning(f"Rename failed, copying file instead: {e}")
                                        with open(temp_output, 'r', encoding='utf-8') as src:
                                            with open(output_file, 'w', encoding='utf-8') as dst:
                                                dst.write(src.read())
                                        os.remove(temp_output)
                                first_file = False
                            else:
                                # Append to existing file
                                if os.path.exists(temp_output):
                                    try:
                                        with open(temp_output, 'r', encoding='utf-8') as temp_f:
                                            next(temp_f)  # Skip header
                                            with open(output_file, 'a', encoding='utf-8') as output_f:
                                                output_f.writelines(temp_f)
                                        os.remove(temp_output)
                                    except Exception as e:
                                        logger.error(f"Error appending temp file {temp_output}: {e}")
                                        # Clean up temp file even if append fails
                                        if os.path.exists(temp_output):
                                            os.remove(temp_output)
                            
                            total_rows += rows_processed
                            
                        else:
                            # Normal processing for small files
                            try:
                                df = pd.read_csv(csv_file, encoding='utf-8')
                                df['_source_file'] = file_name
                                
                                # Write to output file
                                mode = 'w' if first_file else 'a'
                                header = first_file
                                df.to_csv(output_file, mode=mode, header=header, index=False, encoding='utf-8')
                                
                                total_rows += len(df)
                                first_file = False
                                del df  # Free memory immediately
                                
                            except (pd.errors.DtypeWarning, pd.errors.ParserError, ValueError) as e:
                                # Fallback to string reading for problematic files
                                logger.warning(f"Type issues with {csv_file}, reading as strings: {e}")
                                df = pd.read_csv(csv_file, encoding='utf-8', dtype=str)
                                df['_source_file'] = file_name
                                
                                mode = 'w' if first_file else 'a'
                                header = first_file
                                df.to_csv(output_file, mode=mode, header=header, index=False, encoding='utf-8')
                                
                                total_rows += len(df)
                                first_file = False
                                del df  # Free memory immediately
                        
                    except Exception as e:
                        logger.error(f"Error reading {csv_file}: {str(e)}")
                        # Continue with other files instead of failing completely
                        continue
                
                if total_rows == 0:
                    raise ValueError("No CSV files could be successfully processed")
                
                self.progress.emit(
                    self.current_progress + 28,
                    f"Completed merging {total_files} files - {total_rows:,} total rows"
                )
                
                # Return minimal DataFrame with summary info  
                return pd.DataFrame({'_row_count': [total_rows], '_files_processed': [total_files]})
                
        except MemoryError as e:
            error_msg = f"Out of memory processing {source_name}. Try processing smaller batches or increase system memory."
            logger.error(error_msg)
            raise MemoryError(error_msg)
            
        except Exception as e:
            error_msg = f"Error processing {source_name}: {str(e)}"
            logger.error(error_msg)
            raise ValueError(error_msg)
    
    def run(self):
        try:
            results = {
                'sources_processed': 0,
                'total_rows': 0,
                'tables_created': [],
                'output_table': None,
                'execution_time': 0
            }
            
            start_time = time.time()
            total_sources = len(self.sources_config)
            
            # Calculate progress steps
            source_progress_step = 60 // total_sources if total_sources > 0 else 60  # 60% for all sources
            
            # Process each CSV source
            for i, source_config in enumerate(self.sources_config):
                if self.cancel_requested:
                    return
                
                # Check memory before processing each source
                if not self.check_memory_usage():
                    self.error.emit("Insufficient memory to continue processing. Please close other applications or restart the program.")
                    return
                
                self.current_progress = int(i * source_progress_step)
                
                self.progress.emit(
                    self.current_progress,
                    f"Processing source {i+1}/{total_sources}: {source_config['table_name']}"
                )
                
                # Merge CSV files from this source
                temp_output = f"temp_{source_config['table_name']}.csv"
                
                # Clean up any existing temp files from previous runs
                if os.path.exists(temp_output):
                    try:
                        os.remove(temp_output)
                    except Exception as e:
                        logger.warning(f"Could not remove existing temp file {temp_output}: {e}")
                
                # Clean up any chunk files that might be left over
                import glob
                chunk_pattern = f"temp_chunk_*_{source_config['table_name']}*.csv"
                for old_chunk in glob.glob(chunk_pattern):
                    try:
                        os.remove(old_chunk)
                    except:
                        pass
                
                try:
                    # Use our progress-enabled CSV processor (handles both folder and single file)
                    df = self.process_csv_source_with_progress(
                        source_config=source_config,
                        output_file=temp_output,
                        source_name=source_config['table_name']
                    )
                    
                    if df is None:  # Cancelled
                        return
                    
                    # Update progress for database loading
                    self.progress.emit(
                        self.current_progress + 30,
                        f"Loading {source_config['table_name']} into database..."
                    )
                    
                    # Load into DuckDB
                    table_name = source_config['table_name']
                    
                    # Check if table exists first for progress reporting
                    table_exists = False
                    try:
                        result = self.connection.execute(f"SELECT 1 FROM {table_name} LIMIT 1").fetchone()
                        table_exists = result is not None
                    except:
                        table_exists = False
                    
                    if table_exists:
                        self.progress.emit(
                            self.current_progress + 25,
                            f"Replacing existing table '{table_name}'..."
                        )
                    
                    # Always drop the table if it exists to ensure clean replacement
                    self.connection.execute(f"DROP TABLE IF EXISTS {table_name}")
                    
                    # Use absolute path for temp file to avoid path issues
                    temp_output_abs = os.path.abspath(temp_output)
                    
                    # Ensure temp file exists before trying to load it
                    if not os.path.exists(temp_output_abs):
                        raise FileNotFoundError(f"Temp file not found: {temp_output_abs}")
                    
                    # Load with proper path escaping
                    escaped_path = temp_output_abs.replace('\\', '\\\\').replace("'", "''")
                    
                    # Execute with timeout protection
                    import signal
                    
                    def timeout_handler(signum, frame):
                        raise TimeoutError("Database operation timed out")
                    
                    try:
                        # Set timeout for large file loads (5 minutes)
                        if hasattr(signal, 'SIGALRM'):  # Unix systems
                            signal.signal(signal.SIGALRM, timeout_handler)
                            signal.alarm(300)  # 5 minutes
                        
                        # Try multiple approaches for robust CSV loading
                        try:
                            # First attempt: Use read_csv_auto with flexible settings
                            self.progress.emit(
                                self.current_progress + 32,
                                f"Loading {table_name} with auto-detection..."
                            )
                            self.connection.execute(f"""
                                CREATE TABLE {table_name} AS 
                                SELECT * FROM read_csv_auto('{escaped_path}', 
                                    sample_size=-1,
                                    ignore_errors=true,
                                    null_padding=true
                                )
                            """)
                        except Exception as e1:
                            logger.warning(f"Auto-detection failed for {table_name}, trying with all VARCHAR: {e1}")
                            try:
                                # Second attempt: Force all columns as VARCHAR to avoid type issues
                                self.progress.emit(
                                    self.current_progress + 32,
                                    f"Loading {table_name} as text (type detection failed)..."
                                )
                                self.connection.execute(f"""
                                    CREATE TABLE {table_name} AS 
                                    SELECT * FROM read_csv_auto('{escaped_path}', 
                                        all_varchar=true,
                                        ignore_errors=true,
                                        null_padding=true
                                    )
                                """)
                            except Exception as e2:
                                logger.warning(f"All VARCHAR failed for {table_name}, trying manual approach: {e2}")
                                # Third attempt: Use basic CSV reading
                                self.progress.emit(
                                    self.current_progress + 32,
                                    f"Loading {table_name} with basic CSV reader..."
                                )
                                self.connection.execute(f"""
                                    CREATE TABLE {table_name} AS 
                                    SELECT * FROM read_csv('{escaped_path}', 
                                        auto_detect=true,
                                        ignore_errors=true,
                                        header=true
                                    )
                                """)
                        
                        if hasattr(signal, 'SIGALRM'):
                            signal.alarm(0)  # Cancel timeout
                            
                    except TimeoutError:
                        if hasattr(signal, 'SIGALRM'):
                            signal.alarm(0)  # Cancel timeout
                        raise TimeoutError(f"Database loading timeout for {table_name}. File may be too large.")
                    
                    # Clean up temp file
                    if os.path.exists(temp_output_abs):
                        os.remove(temp_output_abs)
                    
                    results['sources_processed'] += 1
                    
                    # Handle both normal DataFrames and chunked processing results
                    if '_row_count' in df.columns:
                        # Chunked processing result
                        row_count = df['_row_count'].iloc[0] if len(df) > 0 else 0
                        results['total_rows'] += row_count
                    else:
                        # Normal DataFrame
                        results['total_rows'] += len(df)
                    
                    results['tables_created'].append(table_name)
                    
                    # Complete progress for this source
                    row_count_display = df['_row_count'].iloc[0] if '_row_count' in df.columns else len(df)
                    self.progress.emit(
                        int((i + 1) * source_progress_step),
                        f"Completed {source_config['table_name']} - {row_count_display:,} rows loaded"
                    )
                    
                    # Clean up memory after each source
                    del df
                    self.force_cleanup()
                    
                except MemoryError as e:
                    logger.error(f"Memory error processing source {source_config['table_name']}: {e}")
                    self.error.emit(f"Out of memory processing {source_config['table_name']}. Please try processing smaller files or free up system memory.")
                    return
                except FileNotFoundError as e:
                    logger.error(f"File not found for source {source_config['table_name']}: {e}")
                    self.error.emit(f"File not found for {source_config['table_name']}: {str(e)}")
                    return
                except PermissionError as e:
                    logger.error(f"Permission error processing source {source_config['table_name']}: {e}")
                    self.error.emit(f"Permission denied accessing files for {source_config['table_name']}. Please check file permissions.")
                    return
                except TimeoutError as e:
                    logger.error(f"Timeout error processing source {source_config['table_name']}: {e}")
                    self.error.emit(f"Operation timed out for {source_config['table_name']}. The dataset may be too large for available system resources.")
                    return
                except Exception as e:
                    logger.error(f"Error processing source {source_config['table_name']}: {e}")
                    self.error.emit(f"Error processing {source_config['table_name']}: {str(e)}")
                    return
            
            # Execute SQL query if provided
            if self.sql_query and self.output_config['table_name']:
                self.progress.emit(75, "Executing SQL transformation...")
                
                try:
                    output_table = self.output_config['table_name']
                    
                    # Check if output table exists first for progress reporting
                    output_table_exists = False
                    try:
                        result = self.connection.execute(f"SELECT 1 FROM {output_table} LIMIT 1").fetchone()
                        output_table_exists = result is not None
                    except:
                        output_table_exists = False
                    
                    if output_table_exists:
                        self.progress.emit(77, f"Replacing existing output table '{output_table}'...")
                    
                    # Always drop the output table if it exists to ensure clean replacement
                    self.connection.execute(f"DROP TABLE IF EXISTS {output_table}")
                    
                    # Create output table from SQL query
                    create_sql = f"CREATE TABLE {output_table} AS ({self.sql_query})"
                    self.connection.execute(create_sql)
                    
                    self.progress.emit(85, "Getting output table statistics...")
                    
                    # Get row count
                    result = self.connection.execute(f"SELECT COUNT(*) FROM {output_table}").fetchone()
                    output_rows = result[0] if result else 0
                    
                    results['output_table'] = output_table
                    results['output_rows'] = output_rows
                    
                    self.progress.emit(95, f"SQL transformation complete - {output_rows:,} rows in {output_table}")
                    
                except Exception as e:
                    logger.error(f"Error executing SQL query: {e}")
                    self.error.emit(f"Error executing SQL query: {str(e)}")
                    return
            
            results['execution_time'] = time.time() - start_time
            self.progress.emit(100, "Processing completed successfully!")
            
            # Clean up all temporary files
            self.cleanup_temp_files()
            
            self.finished.emit(True, "CSV automation completed successfully!", results)
            
        except Exception as e:
            logger.error(f"Unexpected error in CSV automation: {e}")
            # Clean up temporary files even on error
            self.cleanup_temp_files()
            self.error.emit(f"Unexpected error: {str(e)}")


class CSVAutomationDialog(QDialog):
    """Main dialog for CSV automation functionality"""
    
    def __init__(self, parent=None, connection=None, connection_info=None):
        super().__init__(parent)
        self.connection = connection
        self.connection_info = connection_info
        self.csv_sources = []
        self.worker = None
        
        self.setWindowTitle("CSV Automation")
        self.resize(800, 600)
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
            "2. Optional: Write SQL query to combine/transform data\n"
            "3. Specify output table name and execute automation"
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
        if SQL_EDITOR_AVAILABLE:
            self.sql_editor = SQLTextEdit()
            self.sql_editor.setFont(QFont("Consolas", 10))
            
            # Apply syntax highlighting
            self.sql_highlighter = SQLHighlighter(self.sql_editor.document())
            
            # Set up SQL completions if we have connection info
            if self.connection_info:
                self.update_sql_completions()
        else:
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
        
        # Tab 3: Output
        output_tab = QWidget()
        output_layout = QVBoxLayout(output_tab)
        
        output_title = QLabel("Output Configuration")
        output_title.setFont(QFont("Arial", 14, QFont.Weight.Bold))
        output_layout.addWidget(output_title)
        
        # Output table name
        output_form = QFormLayout()
        self.output_table_line = QLineEdit()
        self.output_table_line.setPlaceholderText("Enter output table name (only if using SQL query)")
        output_form.addRow("Output Table Name:", self.output_table_line)
        
        output_layout.addLayout(output_form)
        output_layout.addStretch()
        
        self.tab_widget.addTab(output_tab, "3. Output")
        
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
        
        # Connect table name changes to update SQL completions
        source_widget.table_line.textChanged.connect(self.update_sql_completions)
    
    def remove_source(self, index):
        """Remove a CSV source widget"""
        if len(self.csv_sources) <= 1:
            QMessageBox.warning(self, "Warning", "At least one CSV source is required.")
            return
        
        widget = self.csv_sources.pop(index)
        widget.setParent(None)
        widget.deleteLater()
        
        # Update indices
        for i, source in enumerate(self.csv_sources):
            source.source_index = i
            source.title_label.setText(f"CSV Source {i + 1}")
        
        # Update SQL completions after removing source
        self.update_sql_completions()
    
    def execute_automation(self):
        """Execute the CSV automation process"""
        # Ensure we have sources first
        if not self.csv_sources:
            QMessageBox.warning(self, "Warning", "No CSV sources configured. Please add at least one source.")
            return
            
        # Validate sources
        valid_sources = [s for s in self.csv_sources if s.is_valid()]
        
        if not valid_sources:
            QMessageBox.warning(self, "Warning", "At least one valid CSV source is required. Please check that all sources have valid folder paths and table names.")
            return
        
        # Check for duplicate table names
        table_names = [s.get_config()['table_name'] for s in valid_sources]
        if len(table_names) != len(set(table_names)):
            QMessageBox.warning(self, "Warning", "Table names must be unique.")
            return
        
        # Get configurations
        sources_config = [s.get_config() for s in valid_sources]
        
        sql_query = self.sql_editor.toPlainText().strip()
        output_table = self.output_table_line.text().strip()
        
        # Validate SQL query and output table
        if sql_query and not output_table:
            QMessageBox.warning(self, "Warning", "Please specify an output table name when using SQL query.")
            return
        
        output_config = {'table_name': output_table}
        
        # Start worker thread
        self.worker = CSVAutomationWorker(
            self.connection, sources_config, output_config, sql_query
        )
        
        self.worker.progress.connect(self.update_progress)
        self.worker.finished.connect(self.automation_finished)
        self.worker.error.connect(self.automation_error)
        
        # Update UI
        self.execute_btn.setEnabled(False)
        self.cancel_btn.setEnabled(True)
        self.progress_bar.setValue(0)
        
        self.worker.start()
    
    def cancel_automation(self):
        """Cancel the running automation"""
        if self.worker:
            self.worker.cancel()
            self.worker.wait()
            self.worker = None
        
        self.execute_btn.setEnabled(True)
        self.cancel_btn.setEnabled(False)
        self.progress_label.setText("Automation cancelled")
    
    def update_progress(self, value, message):
        """Update progress bar and message"""
        self.progress_bar.setValue(value)
        self.progress_label.setText(message)
    
    def automation_finished(self, success, message, results):
        """Handle automation completion"""
        self.execute_btn.setEnabled(True)
        self.cancel_btn.setEnabled(False)
        self.worker = None
        
        if success:
            result_text = f"Automation completed successfully!\n\n"
            result_text += f"Sources processed: {results['sources_processed']}\n"
            result_text += f"Total rows loaded: {results['total_rows']:,}\n"
            result_text += f"Tables created: {', '.join(results['tables_created'])}\n"
            
            if results.get('output_table'):
                result_text += f"Output table: {results['output_table']}\n"
                result_text += f"Output rows: {results.get('output_rows', 0):,}\n"
            
            result_text += f"Execution time: {results['execution_time']:.2f} seconds"
            
            QMessageBox.information(self, "Success", result_text)
            
            # Refresh schema if possible
            if hasattr(self.parent(), 'refresh_schema_browser'):
                self.parent().refresh_schema_browser()
        else:
            QMessageBox.critical(self, "Error", message)
    
    def automation_error(self, error_message):
        """Handle automation error"""
        self.execute_btn.setEnabled(True)
        self.cancel_btn.setEnabled(False)
        self.worker = None
        
        QMessageBox.critical(self, "Error", f"Automation failed:\n{error_message}")
        self.progress_label.setText("Automation failed")
    
    def update_sql_completions(self):
        """Update SQL completions with table names from sources"""
        if not SQL_EDITOR_AVAILABLE or not hasattr(self.sql_editor, 'completer'):
            return
            
        # Get table names from current sources
        table_names = []
        for source in self.csv_sources:
            if source.is_valid():
                config = source.get_config()
                table_names.append(config['table_name'])
        
        # Update completions
        if hasattr(self.sql_editor, 'update_completions'):
            self.sql_editor.update_completions(table_names=table_names)
    
    def get_automation_config(self):
        """Get the current automation configuration as a dictionary"""
        config = {
            'version': '1.0',
            'created': datetime.now().isoformat(),
            'description': 'CSV Automation Configuration',
            'sources': [],
            'sql_query': '',
            'output_table': ''
        }
        
        # Get sources configuration
        for source in self.csv_sources:
            if source.is_valid():
                source_config = source.get_config()
                config['sources'].append(source_config)
        
        # Get SQL query
        if hasattr(self.sql_editor, 'toPlainText'):
            config['sql_query'] = self.sql_editor.toPlainText().strip()
        else:
            config['sql_query'] = self.sql_editor.toPlainText().strip()
        
        # Get output table
        config['output_table'] = self.output_table_line.text().strip()
        
        return config
    
    def set_automation_config(self, config):
        """Load an automation configuration"""
        try:
            # Clear existing sources (but ensure we always have at least one)
            while len(self.csv_sources) > 1:
                self.remove_source(len(self.csv_sources) - 1)
            
            # Clear the first source
            if self.csv_sources:
                first_source = self.csv_sources[0]
                first_source.path_line.clear()
                first_source.table_line.clear()
                first_source.pattern_line.setText("*.csv")
                # Reset to folder mode by default
                first_source.mode_combo.setCurrentText("Folder (Multiple Files)")
                first_source.current_mode = 'folder'
                first_source.update_ui_for_mode()
            
            # Load sources
            sources = config.get('sources', [])
            if not sources:
                # Just clear everything but don't return - user might want to add sources manually
                # Make sure we have at least one empty source to work with
                if not self.csv_sources:
                    self.add_csv_source()
                return  # No sources to load, but we have a clean slate
                
            for i, source_config in enumerate(sources):
                # Use existing source or add new one
                if i < len(self.csv_sources):
                    source_widget = self.csv_sources[i]
                else:
                    self.add_csv_source()
                    source_widget = self.csv_sources[-1]
                
                # Set source configuration
                source_widget.table_line.setText(source_config.get('table_name', ''))
                
                # Handle mode-specific configuration
                mode = source_config.get('mode', 'folder')  # Default to folder for backward compatibility
                if mode == 'file':
                    source_widget.mode_combo.setCurrentText("Single File")
                    source_widget.current_mode = 'file'
                    source_widget.update_ui_for_mode()
                    source_widget.path_line.setText(source_config.get('file_path', ''))
                else:
                    source_widget.mode_combo.setCurrentText("Folder (Multiple Files)")
                    source_widget.current_mode = 'folder'
                    source_widget.update_ui_for_mode()
                    source_widget.path_line.setText(source_config.get('folder_path', ''))
                    source_widget.pattern_line.setText(source_config.get('file_pattern', '*.csv'))
                
                # Trigger updates
                source_widget.on_path_changed()
            
            # Load SQL query
            sql_query = config.get('sql_query', '')
            if hasattr(self.sql_editor, 'setPlainText'):
                self.sql_editor.setPlainText(sql_query)
            else:
                self.sql_editor.setPlainText(sql_query)
            
            # Load output table
            self.output_table_line.setText(config.get('output_table', ''))
            
            # Update SQL completions
            self.update_sql_completions()
            
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Failed to load automation configuration:\n{str(e)}")
    

    
    def load_automation(self):
        """Load an automation configuration from a JSON file"""
        try:
            # Open file dialog
            file_path, _ = QFileDialog.getOpenFileName(
                self,
                "Load CSV Automation",
                "",
                "JSON Files (*.json);;All Files (*)"
            )
            
            if not file_path:
                return
            
            with open(file_path, 'r', encoding='utf-8') as f:
                config = json.load(f)
            
            # Validate configuration
            if not isinstance(config, dict):
                raise ValueError("Invalid automation file format")
            
            if 'sources' not in config:
                raise ValueError("No sources found in automation file")
            
            # Load configuration
            self.set_automation_config(config)
            
            QMessageBox.information(
                self, 
                "Success", 
                f"Automation configuration loaded from:\n{file_path}\n\n"
                f"Sources loaded: {len(config.get('sources', []))}\n"
                f"SQL Query: {'Yes' if config.get('sql_query') else 'No'}\n"
                f"Output Table: {config.get('output_table', 'Not specified')}"
            )
            
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Failed to load automation:\n{str(e)}")
    
    def refresh_automations_list(self):
        """Refresh the list of saved automations"""
        try:
            self.automations_list.clear()
            
            # Find all JSON files in automations directory
            json_files = glob.glob(os.path.join(self.automations_dir, "*.json"))
            
            if not json_files:
                self.automations_list.addItem("No saved automations")
                return
            
            # Sort by modification time (newest first)
            json_files.sort(key=lambda x: os.path.getmtime(x), reverse=True)
            
            for json_file in json_files:
                try:
                    # Try to load and validate the file
                    with open(json_file, 'r', encoding='utf-8') as f:
                        config = json.load(f)
                    
                    # Create display name
                    filename = os.path.basename(json_file)
                    display_name = filename.replace('.json', '')
                    
                    # Add creation date if available
                    if 'created' in config:
                        try:
                            created_date = datetime.fromisoformat(config['created'].replace('Z', '+00:00'))
                            display_name += f" ({created_date.strftime('%Y-%m-%d %H:%M')})"
                        except:
                            pass
                    
                    # Add to list with full path as data
                    item = QListWidgetItem(display_name)
                    item.setData(Qt.ItemDataRole.UserRole, json_file)  # Store full path
                    self.automations_list.addItem(item)
                    
                except Exception as e:
                    # Skip invalid files
                    logger.error(f"Error reading automation file {json_file}: {e}")
                    continue
        
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Failed to refresh automations list:\n{str(e)}")
    
    def show_automation_details(self):
        """Show details of the selected automation"""
        try:
            current_item = self.automations_list.currentItem()
            if not current_item or current_item.text() == "No saved automations":
                self.automation_details.setText("Select an automation to see details")
                return
            
            file_path = current_item.data(Qt.ItemDataRole.UserRole)
            if not file_path:
                return
            
            # Load automation config
            with open(file_path, 'r', encoding='utf-8') as f:
                config = json.load(f)
            
            # Build details text
            details = []
            details.append(f"<b>File:</b> {os.path.basename(file_path)}")
            
            if 'created' in config:
                details.append(f"<b>Created:</b> {config['created']}")
            
            sources_count = len(config.get('sources', []))
            details.append(f"<b>Sources:</b> {sources_count}")
            
            for i, source in enumerate(config.get('sources', []), 1):
                table_name = source.get('table_name', 'Unknown')
                
                # Handle both file and folder modes
                if source.get('mode') == 'file':
                    file_path = source.get('file_path', 'Unknown')
                    if file_path != 'Unknown':
                        source_display = os.path.basename(file_path)
                    else:
                        source_display = 'Unknown file'
                else:
                    folder_path = source.get('folder_path', 'Unknown')
                    if folder_path != 'Unknown':
                        source_display = os.path.basename(folder_path)
                    else:
                        source_display = 'Unknown folder'
                
                details.append(f"  {i}. {table_name} ← {source_display}")
            
            if config.get('sql_query'):
                query_preview = config['sql_query'][:100]
                if len(config['sql_query']) > 100:
                    query_preview += "..."
                details.append(f"<b>SQL Query:</b> {query_preview}")
            else:
                details.append(f"<b>SQL Query:</b> None")
            
            if config.get('output_table'):
                details.append(f"<b>Output Table:</b> {config['output_table']}")
            
            self.automation_details.setText("<br/>".join(details))
            
        except Exception as e:
            self.automation_details.setText(f"Error reading automation: {str(e)}")
    
    def load_selected_automation(self):
        """Load the selected automation from the list"""
        try:
            current_item = self.automations_list.currentItem()
            if not current_item or current_item.text() == "No saved automations":
                QMessageBox.warning(self, "Warning", "Please select an automation to load.")
                return
            
            file_path = current_item.data(Qt.ItemDataRole.UserRole)
            if not file_path:
                return
            
            # Load and apply the configuration
            with open(file_path, 'r', encoding='utf-8') as f:
                config = json.load(f)
            
            self.set_automation_config(config)
            
            QMessageBox.information(
                self,
                "Success",
                f"Automation loaded successfully!\n\n"
                f"Sources: {len(config.get('sources', []))}\n"
                f"SQL Query: {'Yes' if config.get('sql_query') else 'No'}\n"
                f"Output Table: {config.get('output_table', 'Not specified')}"
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
            output_table = config.get('output_table', '')
            
            confirmation_text = (
                f"Are you sure you want to run this automation?\n\n"
                f"File: {os.path.basename(file_path)}\n"
                f"Sources: {sources_count}\n"
                f"SQL Query: {'Yes' if has_sql else 'No'}\n"
                f"Output Table: {output_table if output_table else 'Not specified'}"
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


def show_csv_automation_dialog(parent=None, connection=None, connection_info=None):
    """Show the CSV automation dialog"""
    dialog = CSVAutomationDialog(parent, connection, connection_info)
    return dialog.exec()


if __name__ == "__main__":
    # Test the dialog
    from PyQt6.QtWidgets import QApplication
    import sys
    
    app = QApplication(sys.argv)
    
    # Create a test DuckDB connection
    conn = duckdb.connect(':memory:')
    conn_info = {'type': 'duckdb', 'database': ':memory:'}
    
    dialog = CSVAutomationDialog(None, conn, conn_info)
    dialog.show()
    
    sys.exit(app.exec()) 