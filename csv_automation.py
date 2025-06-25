"""
CSV Automation Module - Process Multiple CSV Sources with SQL
"""

import os
import glob
import pandas as pd
import duckdb
import time
import logging
from typing import List, Dict, Optional
from pathlib import Path

from PyQt6.QtWidgets import (
    QDialog, QVBoxLayout, QHBoxLayout, QPushButton, QLabel, QFileDialog,
    QProgressBar, QTextEdit, QGroupBox, QListWidget, QLineEdit, QTabWidget, 
    QWidget, QScrollArea, QFormLayout, QPlainTextEdit, QFrame, QMessageBox
)
from PyQt6.QtCore import QThread, pyqtSignal, Qt
from PyQt6.QtGui import QFont
import qtawesome as qta

from csv_merger import append_csv_files, get_csv_info

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
        
        self.remove_btn = QPushButton("Remove")
        self.remove_btn.clicked.connect(self.request_remove)
        header_layout.addWidget(self.remove_btn)
        
        layout.addWidget(header_frame)
        
        # Configuration form
        form_layout = QFormLayout()
        
        # Folder selection
        folder_layout = QHBoxLayout()
        self.folder_line = QLineEdit()
        self.folder_line.setPlaceholderText("Select CSV folder...")
        self.folder_line.textChanged.connect(self.on_folder_changed)
        
        self.browse_btn = QPushButton("Browse")
        self.browse_btn.clicked.connect(self.browse_folder)
        
        folder_layout.addWidget(self.folder_line)
        folder_layout.addWidget(self.browse_btn)
        form_layout.addRow("CSV Folder:", folder_layout)
        
        # Table name
        self.table_line = QLineEdit()
        self.table_line.setPlaceholderText("Enter table name")
        form_layout.addRow("Table Name:", self.table_line)
        
        # File pattern
        self.pattern_line = QLineEdit("*.csv")
        form_layout.addRow("File Pattern:", self.pattern_line)
        
        layout.addLayout(form_layout)
        
        # File preview
        self.file_list = QListWidget()
        self.file_list.setMaximumHeight(80)
        layout.addWidget(QLabel("Files Preview:"))
        layout.addWidget(self.file_list)
        
        self.preview_label = QLabel("No folder selected")
        self.preview_label.setStyleSheet("color: gray;")
        layout.addWidget(self.preview_label)
    
    def browse_folder(self):
        folder = QFileDialog.getExistingDirectory(self, "Select CSV Folder")
        if folder:
            self.folder_line.setText(folder)
    
    def on_folder_changed(self):
        folder_path = self.folder_line.text()
        if folder_path and os.path.exists(folder_path):
            self.update_file_preview()
            # Auto-suggest table name
            if not self.table_line.text():
                suggested_name = os.path.basename(folder_path.rstrip('/\\'))
                self.table_line.setText(self.clean_table_name(suggested_name))
    
    def update_file_preview(self):
        self.file_list.clear()
        folder_path = self.folder_line.text()
        
        if not folder_path or not os.path.exists(folder_path):
            self.preview_label.setText("Invalid folder path")
            return
        
        pattern = self.pattern_line.text() or "*.csv"
        csv_files = glob.glob(os.path.join(folder_path, pattern))
        
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
        return {
            'folder_path': self.folder_line.text(),
            'table_name': self.table_line.text(),
            'file_pattern': self.pattern_line.text() or "*.csv"
        }
    
    def is_valid(self):
        config = self.get_config()
        return (config['folder_path'] and 
                os.path.exists(config['folder_path']) and 
                config['table_name'])


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
    
    def append_csv_files_with_progress(self, input_folder, output_file, file_pattern, source_name):
        """Modified version of append_csv_files with progress reporting"""
        
        # Find all CSV files
        csv_pattern = os.path.join(input_folder, file_pattern)
        csv_files = glob.glob(csv_pattern)
        
        if not csv_files:
            raise FileNotFoundError(f"No CSV files found in '{input_folder}' matching pattern '{file_pattern}'")
        
        self.progress.emit(
            self.current_progress,
            f"Found {len(csv_files)} files in {source_name}. Starting merge..."
        )
        
        # Read and collect all DataFrames
        dataframes = []
        total_files = len(csv_files)
        
        for i, csv_file in enumerate(csv_files):
            if self.cancel_requested:
                return None
                
            try:
                # Update progress for each file
                file_progress = int((i / total_files) * 30)  # 30% of progress for this source
                self.progress.emit(
                    self.current_progress + file_progress,
                    f"Reading file {i+1}/{total_files}: {os.path.basename(csv_file)}"
                )
                
                df = pd.read_csv(csv_file, encoding='utf-8')
                df['_source_file'] = os.path.basename(csv_file)
                dataframes.append(df)
                
            except Exception as e:
                logger.error(f"Error reading {csv_file}: {str(e)}")
                continue
        
        if not dataframes:
            raise ValueError("No CSV files could be successfully read")
        
        # Merge DataFrames
        self.progress.emit(
            self.current_progress + 25,
            f"Merging {len(dataframes)} files for {source_name}..."
        )
        
        merged_df = pd.concat(dataframes, ignore_index=True, sort=False)
        
        # Save to output file
        self.progress.emit(
            self.current_progress + 28,
            f"Saving merged data for {source_name}..."
        )
        
        merged_df.to_csv(output_file, index=False, encoding='utf-8')
        
        return merged_df
    
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
                
                self.current_progress = int(i * source_progress_step)
                
                self.progress.emit(
                    self.current_progress,
                    f"Processing source {i+1}/{total_sources}: {source_config['table_name']}"
                )
                
                # Merge CSV files from this source
                temp_output = f"temp_{source_config['table_name']}.csv"
                
                try:
                    # Use our progress-enabled CSV merger
                    df = self.append_csv_files_with_progress(
                        input_folder=source_config['folder_path'],
                        output_file=temp_output,
                        file_pattern=source_config['file_pattern'],
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
                    self.connection.execute(f"DROP TABLE IF EXISTS {table_name}")
                    self.connection.execute(f"CREATE TABLE {table_name} AS SELECT * FROM read_csv_auto('{temp_output}')")
                    
                    # Clean up temp file
                    if os.path.exists(temp_output):
                        os.remove(temp_output)
                    
                    results['sources_processed'] += 1
                    results['total_rows'] += len(df)
                    results['tables_created'].append(table_name)
                    
                    # Complete progress for this source
                    self.progress.emit(
                        int((i + 1) * source_progress_step),
                        f"Completed {source_config['table_name']} - {len(df):,} rows loaded"
                    )
                    
                except Exception as e:
                    logger.error(f"Error processing source {source_config['table_name']}: {e}")
                    self.error.emit(f"Error processing {source_config['table_name']}: {str(e)}")
                    return
            
            # Execute SQL query if provided
            if self.sql_query and self.output_config['table_name']:
                self.progress.emit(75, "Executing SQL transformation...")
                
                try:
                    output_table = self.output_config['table_name']
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
            
            self.finished.emit(True, "CSV automation completed successfully!", results)
            
        except Exception as e:
            logger.error(f"Unexpected error in CSV automation: {e}")
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
        sources_layout = QVBoxLayout(sources_tab)
        
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
        
        self.tab_widget.addTab(sources_tab, "1. CSV Sources")
        
        # Tab 2: SQL Query
        sql_tab = QWidget()
        sql_layout = QVBoxLayout(sql_tab)
        
        sql_title = QLabel("SQL Query (Optional)")
        sql_title.setFont(QFont("Arial", 14, QFont.Weight.Bold))
        sql_layout.addWidget(sql_title)
        
        sql_description = QLabel(
            "Write an SQL query to combine or transform your CSV data.\n"
            "Reference tables by the names specified in the Sources tab."
        )
        sql_description.setStyleSheet("color: gray;")
        sql_layout.addWidget(sql_description)
        
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
        
        # Add initial CSV source
        self.add_csv_source()
    
    def add_csv_source(self):
        """Add a new CSV source widget"""
        source_widget = CSVSourceWidget(self.sources_widget, len(self.csv_sources), self)
        self.csv_sources.append(source_widget)
        self.sources_layout.addWidget(source_widget)
    
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
    
    def execute_automation(self):
        """Execute the CSV automation process"""
        # Validate sources
        valid_sources = [s for s in self.csv_sources if s.is_valid()]
        
        if not valid_sources:
            QMessageBox.warning(self, "Warning", "Please configure at least one valid CSV source.")
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