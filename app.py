#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
SQL Editor Application

A modern SQL editor with support for multiple database types including SQLite and DuckDB.
Features include syntax highlighting, query execution, result visualization, and schema browsing.
"""

import sys
import os
import sqlite3
import duckdb
import pandas as pd
from datetime import datetime
import json
import re

from PyQt6.QtWidgets import (
    QApplication, QMainWindow, QTabWidget, QWidget, QVBoxLayout, QHBoxLayout,
    QSplitter, QTreeWidget, QTreeWidgetItem, QTextEdit, QTableView, QHeaderView,
    QLabel, QPushButton, QComboBox, QFileDialog, QMessageBox, QDialog, QLineEdit,
    QFormLayout, QDialogButtonBox, QToolBar, QStatusBar, QMenu, QInputDialog,
    QSizePolicy, QFrame, QToolButton, QGroupBox, QRadioButton, QCheckBox, QListWidget
)
from PyQt6.QtGui import (
    QAction, QFont, QColor, QSyntaxHighlighter, QTextCharFormat, QIcon,
    QTextCursor, QPalette, QKeySequence, QShortcut
)
from PyQt6.QtCore import (
    Qt, QAbstractTableModel, QModelIndex, QSize, QThread, pyqtSignal,
    QRegularExpression, QSettings, QTimer
)
import qtawesome as qta

# Set application style
QApplication.setStyle('Fusion')

# Define color scheme
class ColorScheme:
    # Dark theme colors
    BACKGROUND = QColor(45, 45, 45)
    SIDEBAR_BG = QColor(35, 35, 35)
    TEXT = QColor(240, 240, 240)
    HIGHLIGHT = QColor(58, 150, 221)
    ACCENT = QColor(75, 160, 240)
    SUCCESS = QColor(95, 200, 115)
    WARNING = QColor(255, 170, 0)
    ERROR = QColor(255, 85, 85)
    
    # Enhanced SQL syntax highlighting colors
    COMMENT = QColor(128, 128, 128)          # Gray for comments
    STRING = QColor(152, 195, 121)           # Green for strings
    KEYWORD = QColor(198, 120, 221)          # Purple for SQL keywords
    NUMBER = QColor(229, 192, 123)           # Orange for numbers
    FUNCTION = QColor(97, 175, 239)          # Blue for functions
    OPERATOR = QColor(86, 182, 194)          # Cyan for operators
    TABLE_NAME = QColor(224, 108, 117)       # Red for table names
    COLUMN_NAME = QColor(152, 195, 121)      # Light green for column names
    DATA_TYPE = QColor(209, 154, 102)        # Tan for data types

# SQL Syntax Highlighter
class SQLHighlighter(QSyntaxHighlighter):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.highlighting_rules = []

        # SQL Keywords (Primary commands)
        keyword_format = QTextCharFormat()
        keyword_format.setForeground(ColorScheme.KEYWORD)
        keyword_format.setFontWeight(QFont.Weight.Bold)
        primary_keywords = [
            "\bSELECT\b", "\bFROM\b", "\bWHERE\b", "\bINSERT\b", "\bUPDATE\b", "\bDELETE\b",
            "\bCREATE\b", "\bALTER\b", "\bDROP\b", "\bTRUNCATE\b", "\bBEGIN\b", "\bCOMMIT\b", "\bROLLBACK\b"
        ]
        for pattern in primary_keywords:
            self.highlighting_rules.append((QRegularExpression(pattern, QRegularExpression.CaseInsensitiveOption), keyword_format))

        # SQL Operators and Logic
        operator_format = QTextCharFormat()
        operator_format.setForeground(ColorScheme.OPERATOR)
        operator_format.setFontWeight(QFont.Weight.Bold)
        operators = [
            "\bAND\b", "\bOR\b", "\bNOT\b", "\bIN\b", "\bLIKE\b", "\bBETWEEN\b", "\bEXISTS\b",
            "\bIS\b", "\bNULL\b", "\bIS NULL\b", "\bIS NOT NULL\b", "\bALL\b", "\bANY\b", "\bSOME\b",
            "=", "!=", "<>", "<", ">", "<=", ">=", "\+", "-", "\*", "/", "%"
        ]
        for pattern in operators:
            self.highlighting_rules.append((QRegularExpression(pattern, QRegularExpression.CaseInsensitiveOption), operator_format))

        # SQL Functions (Built-in functions)
        function_format = QTextCharFormat()
        function_format.setForeground(ColorScheme.FUNCTION)
        function_format.setFontWeight(QFont.Weight.Bold)
        functions = [
            "\bCOUNT\b", "\bSUM\b", "\bAVG\b", "\bMAX\b", "\bMIN\b", "\bGROUP_CONCAT\b",
            "\bCOALESCE\b", "\bNULLIF\b", "\bCAST\b", "\bCONVERT\b", "\bSUBSTR\b", "\bLENGTH\b",
            "\bUPPER\b", "\bLOWER\b", "\bTRIM\b", "\bLTRIM\b", "\bRTRIM\b", "\bREPLACE\b",
            "\bROW_NUMBER\b", "\bRANK\b", "\bDENSE_RANK\b", "\bLEAD\b", "\bLAG\b",
            "\bFIRST_VALUE\b", "\bLAST_VALUE\b", "\bNTILE\b", "\bDATE\b", "\bTIME\b",
            "\bDATETIME\b", "\bNOW\b", "\bCURDATE\b", "\bCURTIME\b"
        ]
        for pattern in functions:
            self.highlighting_rules.append((QRegularExpression(pattern, QRegularExpression.CaseInsensitiveOption), function_format))

        # Data Types
        datatype_format = QTextCharFormat()
        datatype_format.setForeground(ColorScheme.DATA_TYPE)
        datatype_format.setFontWeight(QFont.Weight.Bold)
        datatypes = [
            "\bINTEGER\b", "\bINT\b", "\bBIGINT\b", "\bSMALLINT\b", "\bTINYINT\b",
            "\bVARCHAR\b", "\bCHAR\b", "\bTEXT\b", "\bSTRING\b", "\bCLOB\b",
            "\bREAL\b", "\bFLOAT\b", "\bDOUBLE\b", "\bNUMERIC\b", "\bDECIMAL\b",
            "\bDATE\b", "\bTIME\b", "\bTIMESTAMP\b", "\bDATETIME\b",
            "\bBOOLEAN\b", "\bBOOL\b", "\bBLOB\b", "\bBINARY\b"
        ]
        for pattern in datatypes:
            self.highlighting_rules.append((QRegularExpression(pattern, QRegularExpression.CaseInsensitiveOption), datatype_format))

        # SQL Clauses and Modifiers
        clause_format = QTextCharFormat()
        clause_format.setForeground(ColorScheme.ACCENT)
        clause_format.setFontWeight(QFont.Weight.Bold)
        clauses = [
            "\bJOIN\b", "\bINNER JOIN\b", "\bLEFT JOIN\b", "\bRIGHT JOIN\b", "\bFULL JOIN\b",
            "\bGROUP BY\b", "\bORDER BY\b", "\bHAVING\b", "\bLIMIT\b", "\bOFFSET\b",
            "\bUNION\b", "\bUNION ALL\b", "\bINTERSECT\b", "\bEXCEPT\b",
            "\bCASE\b", "\bWHEN\b", "\bTHEN\b", "\bELSE\b", "\bEND\b",
            "\bDISTINCT\b", "\bAS\b", "\bON\b", "\bUSING\b", "\bINTO\b", "\bVALUES\b", "\bSET\b",
            "\bWITH\b", "\bRECURSIVE\b", "\bOVER\b", "\bPARTITION BY\b", "\bWINDOW\b"
        ]
        for pattern in clauses:
            self.highlighting_rules.append((QRegularExpression(pattern, QRegularExpression.CaseInsensitiveOption), clause_format))

        # Numbers (integers and decimals)
        number_format = QTextCharFormat()
        number_format.setForeground(ColorScheme.NUMBER)
        number_format.setFontWeight(QFont.Weight.Bold)
        self.highlighting_rules.append((QRegularExpression("\\b\\d+\\.\\d+\\b"), number_format))  # Decimals
        self.highlighting_rules.append((QRegularExpression("\\b\\d+\\b"), number_format))  # Integers

        # String literals (single and double quotes)
        string_format = QTextCharFormat()
        string_format.setForeground(ColorScheme.STRING)
        string_format.setFontItalic(True)
        self.highlighting_rules.append((QRegularExpression("'[^']*'"), string_format))
        self.highlighting_rules.append((QRegularExpression('"[^"]*"'), string_format))

        # Table names (after FROM, JOIN, UPDATE, INSERT INTO, etc.)
        table_format = QTextCharFormat()
        table_format.setForeground(ColorScheme.TABLE_NAME)
        table_format.setFontWeight(QFont.Weight.Bold)
        # This is a simplified pattern - in practice, table name detection is complex
        self.highlighting_rules.append((QRegularExpression("(?i)(?:FROM|JOIN|UPDATE|INSERT\\s+INTO)\\s+([a-zA-Z_][a-zA-Z0-9_]*)", QRegularExpression.CaseInsensitiveOption), table_format))

        # Comments (single line and multi-line)
        comment_format = QTextCharFormat()
        comment_format.setForeground(ColorScheme.COMMENT)
        comment_format.setFontItalic(True)
        self.highlighting_rules.append((QRegularExpression("--[^\n]*"), comment_format))
        
        # Multi-line comment setup
        self.multiline_comment_format = QTextCharFormat()
        self.multiline_comment_format.setForeground(ColorScheme.COMMENT)
        self.multiline_comment_format.setFontItalic(True)
        self.comment_start_expression = QRegularExpression("/\\*")
        self.comment_end_expression = QRegularExpression("\\*/")

    def highlightBlock(self, text):
        # Apply single-line highlighting rules
        for pattern, format in self.highlighting_rules:
            match_iterator = pattern.globalMatch(text)
            while match_iterator.hasNext():
                match = match_iterator.next()
                self.setFormat(match.capturedStart(), match.capturedLength(), format)

        # Handle multi-line comments
        self.setCurrentBlockState(0)
        start_index = 0
        if self.previousBlockState() != 1:
            start_index = self.comment_start_expression.match(text).capturedStart()

        while start_index >= 0:
            match_end = self.comment_end_expression.match(text, start_index)
            end_index = match_end.capturedStart()
            comment_length = 0
            if end_index == -1:
                self.setCurrentBlockState(1)
                comment_length = len(text) - start_index
            else:
                comment_length = end_index - start_index + match_end.capturedLength()
            
            self.setFormat(start_index, comment_length, self.multiline_comment_format)
            start_index = self.comment_start_expression.match(text, start_index + comment_length).capturedStart()

# Table model for displaying query results
class PandasTableModel(QAbstractTableModel):
    def __init__(self, data=None):
        super().__init__()
        self._data = pd.DataFrame() if data is None else data

    def rowCount(self, parent=QModelIndex()):
        return len(self._data)

    def columnCount(self, parent=QModelIndex()):
        return len(self._data.columns)

    def data(self, index, role=Qt.ItemDataRole.DisplayRole):
        if not index.isValid():
            return None
            
        if role == Qt.ItemDataRole.DisplayRole:
            value = self._data.iloc[index.row(), index.column()]
            if pd.isna(value):
                return "NULL"
            elif isinstance(value, (float, int)):
                return str(value)
            else:
                return str(value)
                
        elif role == Qt.ItemDataRole.TextAlignmentRole:
            value = self._data.iloc[index.row(), index.column()]
            if isinstance(value, (int, float)):
                return int(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
            return int(Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter)
                
        return None

    def headerData(self, section, orientation, role=Qt.ItemDataRole.DisplayRole):
        if role == Qt.ItemDataRole.DisplayRole:
            if orientation == Qt.Orientation.Horizontal:
                return str(self._data.columns[section])
            else:
                return str(section + 1)
        return None

    def sort(self, column, order):
        self.layoutAboutToBeChanged.emit()
        self._data = self._data.sort_values(
            self._data.columns[column],
            ascending=(order == Qt.SortOrder.AscendingOrder)
        )
        self.layoutChanged.emit()

# Worker thread for executing queries
class QueryWorker(QThread):
    finished = pyqtSignal(object, float)
    error = pyqtSignal(str)
    
    def __init__(self, connection, query):
        super().__init__()
        self.connection = connection
        self.query = query
        
    def run(self):
        try:
            start_time = datetime.now()
            if isinstance(self.connection, sqlite3.Connection) or isinstance(self.connection, duckdb.DuckDBPyConnection):
                df = pd.read_sql_query(self.query, self.connection)
            else:
                # Handle other database types if needed
                raise ValueError("Unsupported database connection type")
                
            execution_time = (datetime.now() - start_time).total_seconds()
            self.finished.emit(df, execution_time)
        except Exception as e:
            self.error.emit(str(e))

# Connection dialog
class ConnectionDialog(QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Connect to Database")
        self.resize(450, 250)
        
        self.layout = QVBoxLayout()
        
        # Database type selection
        self.db_type_layout = QHBoxLayout()
        self.db_type_label = QLabel("Database Type:")
        self.db_type_combo = QComboBox()
        self.db_type_combo.addItems(["SQLite", "DuckDB"])
        self.db_type_combo.currentIndexChanged.connect(self.update_form)
        self.db_type_layout.addWidget(self.db_type_label)
        self.db_type_layout.addWidget(self.db_type_combo)
        self.layout.addLayout(self.db_type_layout)
        
        # Form layout for connection details
        self.form_layout = QFormLayout()
        self.file_path_edit = QLineEdit()
        self.browse_button = QPushButton("Browse...")
        self.browse_button.clicked.connect(self.browse_file)
        self.create_button = QPushButton("Create New Database")
        self.create_button.clicked.connect(self.create_new_database)
        
        self.file_layout = QHBoxLayout()
        self.file_layout.addWidget(self.file_path_edit)
        self.file_layout.addWidget(self.browse_button)
        self.file_layout.addWidget(self.create_button)
        
        self.form_layout.addRow("Database File:", self.file_layout)
        self.layout.addLayout(self.form_layout)
        
        # Buttons
        self.button_box = QDialogButtonBox(QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel)
        self.button_box.accepted.connect(self.accept)
        self.button_box.rejected.connect(self.reject)
        self.layout.addWidget(self.button_box)
        
        self.setLayout(self.layout)
    
    def update_form(self):
        # Update form based on selected database type
        db_type = self.db_type_combo.currentText()
        # For now, both SQLite and DuckDB just need a file path
        # This can be expanded for other database types
    
    def browse_file(self):
        db_type = self.db_type_combo.currentText()
        file_filter = "SQLite Database (*.db *.sqlite);;All Files (*)" if db_type == "SQLite" else "DuckDB Database (*.duckdb);;All Files (*)"
        file_path, _ = QFileDialog.getOpenFileName(self, "Select Database File", "", file_filter)
        if file_path:
            self.file_path_edit.setText(file_path)
    
    def create_new_database(self):
        db_type = self.db_type_combo.currentText()
        
        # Get database name from user
        name, ok = QInputDialog.getText(self, "Create New Database", "Enter database name:")
        if not ok or not name.strip():
            return
        
        # Clean the name (remove invalid characters)
        name = re.sub(r'[<>:"/\\|?*]', '', name.strip())
        if not name:
            QMessageBox.warning(self, "Invalid Name", "Please enter a valid database name.")
            return
        
        # Set file extension based on database type
        if db_type == "SQLite":
            extension = ".db"
            file_filter = "SQLite Database (*.db)"
        else:  # DuckDB
            extension = ".duckdb"
            file_filter = "DuckDB Database (*.duckdb)"
        
        # Get save location
        default_name = name + extension
        file_path, _ = QFileDialog.getSaveFileName(
            self, 
            f"Create New {db_type} Database", 
            default_name, 
            file_filter
        )
        
        if not file_path:
            return
        
        # Ensure correct extension
        if not file_path.lower().endswith(extension):
            file_path += extension
        
        try:
            # Create the database file
            if db_type == "SQLite":
                # Create SQLite database
                conn = sqlite3.connect(file_path)
                conn.execute("CREATE TABLE IF NOT EXISTS _metadata (created_at TEXT)")
                conn.execute("INSERT INTO _metadata (created_at) VALUES (?)", (datetime.now().isoformat(),))
                conn.commit()
                conn.close()
            else:  # DuckDB
                # Create DuckDB database
                conn = duckdb.connect(file_path)
                conn.execute("CREATE TABLE IF NOT EXISTS _metadata (created_at TEXT)")
                conn.execute("INSERT INTO _metadata (created_at) VALUES (?)", (datetime.now().isoformat(),))
                conn.commit()
                conn.close()
            
            # Set the file path in the dialog
            self.file_path_edit.setText(file_path)
            
            QMessageBox.information(
                self, 
                "Database Created", 
                f"Successfully created {db_type} database:\n{file_path}"
            )
            
        except Exception as e:
            QMessageBox.critical(
                self, 
                "Creation Error", 
                f"Failed to create database:\n{str(e)}"
            )
    
    def get_connection_info(self):
        return {
            "type": self.db_type_combo.currentText(),
            "file_path": self.file_path_edit.text()
        }

# Create Database dialog
class CreateDatabaseDialog(QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Create New Database")
        self.resize(400, 180)
        self.created_file_path = None
        
        self.layout = QVBoxLayout()
        
        # Database type selection
        self.db_type_layout = QHBoxLayout()
        self.db_type_label = QLabel("Database Type:")
        self.db_type_combo = QComboBox()
        self.db_type_combo.addItems(["SQLite", "DuckDB"])
        self.db_type_layout.addWidget(self.db_type_label)
        self.db_type_layout.addWidget(self.db_type_combo)
        self.layout.addLayout(self.db_type_layout)
        
        # Database name input
        self.name_layout = QHBoxLayout()
        self.name_label = QLabel("Database Name:")
        self.name_edit = QLineEdit()
        self.name_edit.setPlaceholderText("Enter database name...")
        self.name_layout.addWidget(self.name_label)
        self.name_layout.addWidget(self.name_edit)
        self.layout.addLayout(self.name_layout)
        
        # Location selection
        self.location_layout = QHBoxLayout()
        self.location_label = QLabel("Save Location:")
        self.location_edit = QLineEdit()
        self.location_edit.setPlaceholderText("Choose location...")
        self.location_edit.setReadOnly(True)
        self.browse_location_button = QPushButton("Browse...")
        self.browse_location_button.clicked.connect(self.browse_location)
        self.location_layout.addWidget(self.location_label)
        self.location_layout.addWidget(self.location_edit)
        self.location_layout.addWidget(self.browse_location_button)
        self.layout.addLayout(self.location_layout)
        
        # Buttons
        self.button_layout = QHBoxLayout()
        self.create_button = QPushButton("Create Database")
        self.create_button.clicked.connect(self.create_database)
        self.cancel_button = QPushButton("Cancel")
        self.cancel_button.clicked.connect(self.reject)
        self.button_layout.addWidget(self.create_button)
        self.button_layout.addWidget(self.cancel_button)
        self.layout.addLayout(self.button_layout)
        
        self.setLayout(self.layout)
        
        # Set default location to user's Documents folder
        import os
        default_location = os.path.expanduser("~/Documents")
        if os.path.exists(default_location):
            self.location_edit.setText(default_location)
    
    def browse_location(self):
        directory = QFileDialog.getExistingDirectory(self, "Select Directory")
        if directory:
            self.location_edit.setText(directory)
    
    def create_database(self):
        name = self.name_edit.text().strip()
        location = self.location_edit.text().strip()
        db_type = self.db_type_combo.currentText()
        
        if not name:
            QMessageBox.warning(self, "Invalid Input", "Please enter a database name.")
            return
        
        if not location or not os.path.exists(location):
            QMessageBox.warning(self, "Invalid Location", "Please select a valid location.")
            return
        
        # Clean the name (remove invalid characters)
        name = re.sub(r'[<>:"/\\|?*]', '', name)
        if not name:
            QMessageBox.warning(self, "Invalid Name", "Please enter a valid database name.")
            return
        
        # Set file extension based on database type
        extension = ".db" if db_type == "SQLite" else ".duckdb"
        file_path = os.path.join(location, name + extension)
        
        # Check if file already exists
        if os.path.exists(file_path):
            reply = QMessageBox.question(
                self, 
                "File Exists", 
                f"A database with this name already exists:\n{file_path}\n\nDo you want to overwrite it?",
                QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
                QMessageBox.StandardButton.No
            )
            if reply == QMessageBox.StandardButton.No:
                return
        
        try:
            # Create the database file
            if db_type == "SQLite":
                conn = sqlite3.connect(file_path)
                conn.execute("CREATE TABLE IF NOT EXISTS _metadata (created_at TEXT, created_by TEXT)")
                conn.execute(
                    "INSERT INTO _metadata (created_at, created_by) VALUES (?, ?)", 
                    (datetime.now().isoformat(), "SQL Editor Application")
                )
                conn.commit()
                conn.close()
            else:  # DuckDB
                conn = duckdb.connect(file_path)
                conn.execute("CREATE TABLE IF NOT EXISTS _metadata (created_at TEXT, created_by TEXT)")
                conn.execute(
                    "INSERT INTO _metadata (created_at, created_by) VALUES (?, ?)", 
                    (datetime.now().isoformat(), "SQL Editor Application")
                )
                conn.commit()
                conn.close()
            
            self.created_file_path = file_path
            
            QMessageBox.information(
                self, 
                "Database Created", 
                f"Successfully created {db_type} database:\n{file_path}\n\nThe database will now be opened."
            )
            
            self.accept()
            
        except Exception as e:
            QMessageBox.critical(
                self, 
                "Creation Error", 
                f"Failed to create database:\n{str(e)}"
            )
    
    def get_connection_info(self):
        if self.created_file_path:
            return {
                "type": self.db_type_combo.currentText(),
                "file_path": self.created_file_path
            }
        return None


class DataImportDialog(QDialog):
    """Dialog for importing data from various file formats"""
    
    def __init__(self, parent=None, connection=None, connection_info=None):
        super().__init__(parent)
        self.connection = connection
        self.connection_info = connection_info
        self.setWindowTitle("Import Data")
        self.setModal(True)
        self.setFixedSize(600, 550)
        self.init_ui()
    
    def init_ui(self):
        layout = QVBoxLayout(self)
        layout.setSpacing(15)
        layout.setContentsMargins(20, 20, 20, 20)
        
        # Title
        title_label = QLabel("📁 Import Data to Database")
        title_label.setStyleSheet("font-size: 14pt; font-weight: bold; color: #0078d4; margin-bottom: 10px;")
        layout.addWidget(title_label)
        
        # File selection
        file_group = QGroupBox("Select Data File")
        file_layout = QVBoxLayout(file_group)
        
        file_path_layout = QHBoxLayout()
        self.file_path_edit = QLineEdit()
        self.file_path_edit.setPlaceholderText("Select a data file...")
        self.file_path_edit.setReadOnly(True)
        
        self.browse_button = QPushButton("📁 Browse")
        self.browse_button.clicked.connect(self.browse_file)
        self.browse_button.setStyleSheet("""
            QPushButton {
                background-color: #0078d4;
                color: white;
                border: none;
                padding: 8px 16px;
                border-radius: 4px;
                font-weight: bold;
            }
            QPushButton:hover { background-color: #106ebe; }
        """)
        
        file_path_layout.addWidget(self.file_path_edit)
        file_path_layout.addWidget(self.browse_button)
        file_layout.addLayout(file_path_layout)
        
        # File info
        self.file_info_label = QLabel("No file selected")
        self.file_info_label.setStyleSheet("color: #666; font-style: italic;")
        file_layout.addWidget(self.file_info_label)
        
        layout.addWidget(file_group)
        
        # Table options
        table_group = QGroupBox("Table Options")
        table_layout = QVBoxLayout(table_group)
        
        # Import mode
        mode_label = QLabel("Import Mode:")
        mode_label.setStyleSheet("font-weight: bold; margin-bottom: 5px;")
        table_layout.addWidget(mode_label)
        
        self.create_new_radio = QRadioButton("🆕 Create new table")
        self.create_new_radio.setChecked(True)
        self.create_new_radio.setToolTip("Create a new table (fails if table already exists)")
        
        self.append_radio = QRadioButton("➕ Append to existing table")
        self.append_radio.setToolTip("Add data to existing table (table must exist)")
        
        self.replace_radio = QRadioButton("🔄 Replace existing table")
        self.replace_radio.setToolTip("Drop existing table and create new one with imported data")
        
        table_layout.addWidget(self.create_new_radio)
        table_layout.addWidget(self.append_radio)
        table_layout.addWidget(self.replace_radio)
        
        # Table name input (for create mode)
        self.table_name_widget = QWidget()
        table_name_layout = QHBoxLayout(self.table_name_widget)
        table_name_layout.setContentsMargins(0, 5, 0, 0)
        table_name_layout.addWidget(QLabel("Table Name:"))
        self.table_name_edit = QLineEdit()
        self.table_name_edit.setPlaceholderText("Enter table name...")
        table_name_layout.addWidget(self.table_name_edit)
        table_layout.addWidget(self.table_name_widget)
        
        # Table selection dropdown (for append/replace modes)
        self.table_select_widget = QWidget()
        table_select_layout = QHBoxLayout(self.table_select_widget)
        table_select_layout.setContentsMargins(0, 5, 0, 0)
        table_select_layout.addWidget(QLabel("Select Table:"))
        self.table_select_combo = QComboBox()
        self.table_select_combo.setStyleSheet("""
            QComboBox {
                background-color: #f8f9fa;
                border: 1px solid #ced4da;
                border-radius: 4px;
                padding: 5px;
                min-height: 20px;
            }
            QComboBox:focus {
                border-color: #0078d4;
            }
            QComboBox::drop-down {
                border: none;
                width: 20px;
            }
            QComboBox::down-arrow {
                image: none;
                border-left: 5px solid transparent;
                border-right: 5px solid transparent;
                border-top: 5px solid #666;
                margin-right: 5px;
            }
        """)
        table_select_layout.addWidget(self.table_select_combo)
        table_layout.addWidget(self.table_select_widget)
        self.table_select_widget.hide()  # Initially hidden
        
        layout.addWidget(table_group)
        
        # Excel sheet selection (initially hidden)
        self.sheet_group = QGroupBox("Excel Sheet Selection")
        sheet_layout = QVBoxLayout(self.sheet_group)
        
        self.sheet_combo = QComboBox()
        sheet_layout.addWidget(self.sheet_combo)
        
        layout.addWidget(self.sheet_group)
        self.sheet_group.hide()
        
        # Advanced options
        advanced_group = QGroupBox("Advanced Options")
        advanced_layout = QVBoxLayout(advanced_group)
        
        # CSV options
        self.csv_options_widget = QWidget()
        csv_options_layout = QFormLayout(self.csv_options_widget)
        
        self.delimiter_edit = QLineEdit(",")
        self.delimiter_edit.setMaximumWidth(50)
        csv_options_layout.addRow("Delimiter:", self.delimiter_edit)
        
        self.encoding_combo = QComboBox()
        self.encoding_combo.addItems(["utf-8", "latin-1", "cp1252", "utf-16"])
        csv_options_layout.addRow("Encoding:", self.encoding_combo)
        
        self.header_checkbox = QCheckBox("First row contains headers")
        self.header_checkbox.setChecked(True)
        csv_options_layout.addRow("", self.header_checkbox)
        
        advanced_layout.addWidget(self.csv_options_widget)
        layout.addWidget(advanced_group)
        
        # Buttons
        button_layout = QHBoxLayout()
        button_layout.addStretch()
        
        self.cancel_button = QPushButton("❌ Cancel")
        self.cancel_button.clicked.connect(self.reject)
        self.cancel_button.setStyleSheet("""
            QPushButton {
                background-color: #6c757d;
                color: white;
                border: none;
                padding: 8px 16px;
                border-radius: 4px;
                font-weight: bold;
            }
            QPushButton:hover { background-color: #5a6268; }
        """)
        
        self.import_button = QPushButton("📥 Import Data")
        self.import_button.clicked.connect(self.accept)
        self.import_button.setEnabled(False)
        self.import_button.setStyleSheet("""
            QPushButton {
                background-color: #28a745;
                color: white;
                border: none;
                padding: 8px 16px;
                border-radius: 4px;
                font-weight: bold;
            }
            QPushButton:hover { background-color: #218838; }
            QPushButton:disabled { background-color: #6c757d; opacity: 0.6; }
        """)
        
        button_layout.addWidget(self.cancel_button)
        button_layout.addWidget(self.import_button)
        layout.addLayout(button_layout)
        
        # Connect signals
        self.file_path_edit.textChanged.connect(self.update_ui)
        self.table_name_edit.textChanged.connect(self.update_ui)
        self.table_select_combo.currentTextChanged.connect(self.update_ui)
        
        # Connect radio button signals to update table selection UI
        self.create_new_radio.toggled.connect(self.update_table_selection_ui)
        self.append_radio.toggled.connect(self.update_table_selection_ui)
        self.replace_radio.toggled.connect(self.update_table_selection_ui)
        
        # Load existing tables if connection is available
        self.load_existing_tables()
    
    def browse_file(self):
        file_path, _ = QFileDialog.getOpenFileName(
            self,
            "Select Data File",
            "",
            "All Supported (*.csv *.xlsx *.xls *.parquet *.json *.tsv *.txt);;"
            "CSV Files (*.csv);;"
            "Excel Files (*.xlsx *.xls);;"
            "Parquet Files (*.parquet);;"
            "JSON Files (*.json);;"
            "TSV Files (*.tsv);;"
            "Text Files (*.txt);;"
            "All Files (*)"
        )
        
        if file_path:
            self.file_path_edit.setText(file_path)
            self.analyze_file(file_path)
    
    def analyze_file(self, file_path):
        try:
            file_size = os.path.getsize(file_path) / 1024 / 1024  # Size in MB
            file_ext = os.path.splitext(file_path)[1].lower()
            
            # Update file info
            self.file_info_label.setText(f"File: {os.path.basename(file_path)} | Size: {file_size:.2f} MB | Type: {file_ext}")
            
            # Auto-suggest table name only if field is empty
            if not self.table_name_edit.text().strip():
                base_name = os.path.splitext(os.path.basename(file_path))[0]
                # Clean table name (remove special characters, replace with underscores)
                clean_name = re.sub(r'[^a-zA-Z0-9_]', '_', base_name).lower()
                self.table_name_edit.setText(clean_name)
            
            # Handle Excel files - show sheet selection
            if file_ext in ['.xlsx', '.xls']:
                self.load_excel_sheets(file_path)
                self.sheet_group.show()
                self.csv_options_widget.hide()
            else:
                self.sheet_group.hide()
                if file_ext in ['.csv', '.tsv', '.txt']:
                    self.csv_options_widget.show()
                    if file_ext == '.tsv':
                        self.delimiter_edit.setText('\t')
                else:
                    self.csv_options_widget.hide()
            
        except Exception as e:
            self.file_info_label.setText(f"Error analyzing file: {str(e)}")
    
    def load_excel_sheets(self, file_path):
        try:
            # Read Excel file to get sheet names
            excel_file = pd.ExcelFile(file_path)
            self.sheet_combo.clear()
            self.sheet_combo.addItems(excel_file.sheet_names)
        except Exception as e:
            QMessageBox.warning(self, "Excel Error", f"Could not read Excel sheets: {str(e)}")
    
    def load_existing_tables(self):
        """Load existing tables from the database into the dropdown"""
        self.table_select_combo.clear()
        
        if not self.connection or not self.connection_info:
            return
            
        try:
            if self.connection_info['type'].lower() == 'duckdb':
                tables_df = self.connection.execute("SHOW TABLES").fetchdf()
                existing_tables = tables_df['name'].tolist() if not tables_df.empty else []
            else:  # SQLite
                cursor = self.connection.cursor()
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
                existing_tables = [row[0] for row in cursor.fetchall()]
            
            if existing_tables:
                # Add tables with icons
                for table in existing_tables:
                    self.table_select_combo.addItem(f"📊 {table}")
            else:
                self.table_select_combo.addItem("(No tables found)")
                self.table_select_combo.setEnabled(False)
                
        except Exception as e:
            self.table_select_combo.addItem(f"Error loading tables: {str(e)}")
            self.table_select_combo.setEnabled(False)
    
    def update_table_selection_ui(self):
        """Update the table selection UI based on the selected import mode"""
        if self.create_new_radio.isChecked():
            self.table_name_widget.show()
            self.table_select_widget.hide()
        else:  # append or replace mode
            self.table_name_widget.hide()
            self.table_select_widget.show()
            
            # Check if we have tables available
            if self.table_select_combo.count() == 0 or not self.table_select_combo.isEnabled():
                # Show warning if no tables available for append/replace
                if self.append_radio.isChecked():
                    QMessageBox.warning(
                        self, 
                        "No Tables Available", 
                        "No existing tables found in the database.\nPlease create a table first or use 'Create new table' mode."
                    )
                    self.create_new_radio.setChecked(True)
                    return
                elif self.replace_radio.isChecked() and self.table_select_combo.count() == 0:
                    QMessageBox.information(
                        self, 
                        "No Tables to Replace", 
                        "No existing tables found to replace.\nYou can use 'Create new table' mode instead."
                    )
                    self.create_new_radio.setChecked(True)
                    return
        
        self.update_ui()
    
    def update_ui(self):
        # Enable import button only if file and appropriate table selection are provided
        has_file = bool(self.file_path_edit.text().strip())
        
        if self.create_new_radio.isChecked():
            has_table_info = bool(self.table_name_edit.text().strip())
        else:  # append or replace mode
            current_text = self.table_select_combo.currentText()
            has_table_info = (current_text and 
                            current_text != "(No tables found)" and
                            not current_text.startswith("Error") and
                            (current_text.startswith("📊 ") or current_text not in ["(No tables found)"]))
        
        self.import_button.setEnabled(has_file and has_table_info)
    
    def get_import_info(self):
        file_path = self.file_path_edit.text()
        file_ext = os.path.splitext(file_path)[1].lower()
        
        # Get table name based on mode
        if self.create_new_radio.isChecked():
            table_name = self.table_name_edit.text().strip()
        else:  # append or replace mode
            table_name = self.table_select_combo.currentText()
            # Remove icon prefix if present
            if table_name.startswith("📊 "):
                table_name = table_name[2:]  # Remove "📊 " prefix
        
        import_info = {
            'file_path': file_path,
            'table_name': table_name,
            'file_type': file_ext,
            'mode': 'create' if self.create_new_radio.isChecked() else 
                   'append' if self.append_radio.isChecked() else 'replace'
        }
        
        # Add Excel sheet if applicable
        if file_ext in ['.xlsx', '.xls'] and self.sheet_combo.currentText():
            import_info['sheet_name'] = self.sheet_combo.currentText()
        
        # Add CSV options if applicable
        if file_ext in ['.csv', '.tsv', '.txt']:
            import_info['delimiter'] = self.delimiter_edit.text() or ','
            import_info['encoding'] = self.encoding_combo.currentText()
            import_info['header'] = self.header_checkbox.isChecked()
        
        return import_info


# Tab widget for query editors
class QueryTab(QWidget):
    schema_changed = pyqtSignal()  # Signal to notify when schema might have changed
    
    def __init__(self, parent=None, connection=None, connection_info=None):
        super().__init__(parent)
        self.connection = connection
        self.connection_info = connection_info
        self.query_worker = None
        
        # Layout
        self.layout = QVBoxLayout(self)
        self.layout.setContentsMargins(0, 0, 0, 0)
        
        # Splitter for editor and results
        self.splitter = QSplitter(Qt.Orientation.Vertical)
        
        # Query editor
        self.editor = QTextEdit()
        self.editor.setFont(QFont("Consolas", 11))
        self.editor.setLineWrapMode(QTextEdit.LineWrapMode.NoWrap)
        self.highlighter = SQLHighlighter(self.editor.document())
        
        # Set editor colors
        editor_palette = self.editor.palette()
        editor_palette.setColor(QPalette.ColorRole.Base, ColorScheme.BACKGROUND)
        editor_palette.setColor(QPalette.ColorRole.Text, ColorScheme.TEXT)
        self.editor.setPalette(editor_palette)
        
        # Results area
        self.results_widget = QWidget()
        self.results_layout = QVBoxLayout(self.results_widget)
        self.results_layout.setContentsMargins(0, 0, 0, 0)
        
        # Results header
        self.results_header = QWidget()
        self.results_header_layout = QHBoxLayout(self.results_header)
        self.results_header_layout.setContentsMargins(5, 5, 5, 5)
        
        self.results_label = QLabel("Results")
        self.results_label.setFont(QFont("Segoe UI", 10, QFont.Weight.Bold))
        self.results_info = QLabel("")
        
        self.results_header_layout.addWidget(self.results_label)
        self.results_header_layout.addWidget(self.results_info, 1)
        
        # Results table
        self.results_table = QTableView()
        self.results_table.setAlternatingRowColors(True)
        self.results_table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Interactive)
        self.results_table.horizontalHeader().setStretchLastSection(True)
        self.results_table.verticalHeader().setVisible(True)
        self.results_table.setSortingEnabled(True)
        
        # Set table colors
        table_palette = self.results_table.palette()
        table_palette.setColor(QPalette.ColorRole.Base, QColor(55, 55, 55))
        table_palette.setColor(QPalette.ColorRole.AlternateBase, QColor(45, 45, 45))
        table_palette.setColor(QPalette.ColorRole.Text, ColorScheme.TEXT)
        self.results_table.setPalette(table_palette)
        
        # Add widgets to layouts
        self.results_layout.addWidget(self.results_header)
        self.results_layout.addWidget(self.results_table)
        
        self.splitter.addWidget(self.editor)
        self.splitter.addWidget(self.results_widget)
        self.splitter.setSizes([200, 300])
        
        self.layout.addWidget(self.splitter)
        
    def execute_query(self):
        if not self.connection:
            QMessageBox.warning(self, "No Connection", "Please connect to a database first.")
            return
            
        query = self.editor.toPlainText().strip()
        if not query:
            return
            
        # Disable editor during execution
        self.editor.setReadOnly(True)
        self.results_info.setText("Executing query...")
        
        # Execute query in a separate thread
        self.query_worker = QueryWorker(self.connection, query)
        self.query_worker.finished.connect(self.handle_query_results)
        self.query_worker.error.connect(self.handle_query_error)
        self.query_worker.start()
    
    def handle_query_results(self, df, execution_time):
        # Update table model with results
        self.model = PandasTableModel(df)
        self.results_table.setModel(self.model)
        
        # Auto-resize columns for better visibility
        for i in range(self.model.columnCount()):
            self.results_table.setColumnWidth(i, 200)
        
        # Update results info
        row_count = len(df)
        self.results_info.setText(f"{row_count} {'row' if row_count == 1 else 'rows'} returned in {execution_time:.3f} seconds")
        
        # Check if this was a DDL statement that might have changed the schema
        query = self.editor.toPlainText().strip().upper()
        ddl_keywords = ['CREATE TABLE', 'DROP TABLE', 'ALTER TABLE', 'CREATE VIEW', 'DROP VIEW', 'CREATE INDEX', 'DROP INDEX']
        if any(keyword in query for keyword in ddl_keywords):
            self.schema_changed.emit()
        
        # Re-enable editor
        self.editor.setReadOnly(False)
    
    def handle_query_error(self, error_message):
        self.results_info.setText(f"Error: {error_message}")
        self.editor.setReadOnly(False)

# Database schema browser
class SchemaBrowser(QTreeWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setHeaderLabels(["Database Objects"])
        self.setColumnCount(1)
        self.setAlternatingRowColors(True)
        self.setAnimated(True)
        
        # Set colors
        palette = self.palette()
        palette.setColor(QPalette.ColorRole.Base, ColorScheme.SIDEBAR_BG)
        palette.setColor(QPalette.ColorRole.AlternateBase, QColor(40, 40, 40))
        palette.setColor(QPalette.ColorRole.Text, ColorScheme.TEXT)
        self.setPalette(palette)
        
        # Icons
        self.table_icon = qta.icon('fa5s.table', color=ColorScheme.ACCENT)
        self.view_icon = qta.icon('fa5s.eye', color=ColorScheme.ACCENT)
        self.index_icon = qta.icon('fa5s.key', color=ColorScheme.ACCENT)
        self.column_icon = qta.icon('fa5s.columns', color=ColorScheme.TEXT)
        self.pk_icon = qta.icon('fa5s.key', color=ColorScheme.SUCCESS)
        self.fk_icon = qta.icon('fa5s.link', color=ColorScheme.WARNING)
    
    def load_schema(self, connection, connection_info):
        self.clear()
        
        db_type = connection_info["type"].lower()
        if db_type in ["sqlite", "sqlite3"]:
            self.load_sqlite_schema(connection)
        elif db_type == "duckdb":
            self.load_duckdb_schema(connection)
        else:
            print(f"Unknown database type: {connection_info['type']}")
    
    def load_sqlite_schema(self, connection):
        # Create root item for database
        db_item = QTreeWidgetItem(self, [os.path.basename(connection.database)])
        db_item.setIcon(0, qta.icon('fa5s.database', color=ColorScheme.ACCENT))
        
        # Get list of tables
        cursor = connection.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
        tables = cursor.fetchall()
        
        # Tables group
        tables_item = QTreeWidgetItem(db_item, ["Tables"])
        tables_item.setIcon(0, qta.icon('fa5s.folder', color=ColorScheme.ACCENT))
        
        if not tables:
            # Show "No tables" message when database is empty
            no_tables_item = QTreeWidgetItem(tables_item, ["(No tables)"])
            no_tables_item.setIcon(0, qta.icon('fa5s.info-circle', color=ColorScheme.COMMENT))
        else:
            for table in tables:
                table_name = table[0]
                table_item = QTreeWidgetItem(tables_item, [table_name])
                table_item.setIcon(0, self.table_icon)
                
                # Get columns for this table
                cursor.execute(f"PRAGMA table_info({table_name})")
                columns = cursor.fetchall()
                
                for column in columns:
                    col_name = column[1]
                    col_type = column[2]
                    is_pk = column[5] == 1  # Primary key flag
                    
                    column_text = f"{col_name} ({col_type})"
                    column_item = QTreeWidgetItem(table_item, [column_text])
                    column_item.setIcon(0, self.pk_icon if is_pk else self.column_icon)
        
        # Views group
        cursor.execute("SELECT name FROM sqlite_master WHERE type='view' ORDER BY name")
        views = cursor.fetchall()
        
        if views:
            views_item = QTreeWidgetItem(db_item, ["Views"])
            views_item.setIcon(0, qta.icon('fa5s.folder', color=ColorScheme.ACCENT))
            
            for view in views:
                view_name = view[0]
                view_item = QTreeWidgetItem(views_item, [view_name])
                view_item.setIcon(0, self.view_icon)
        
        # Indexes group
        cursor.execute("SELECT name, tbl_name FROM sqlite_master WHERE type='index' AND name NOT LIKE 'sqlite_%' ORDER BY name")
        indexes = cursor.fetchall()
        
        if indexes:
            indexes_item = QTreeWidgetItem(db_item, ["Indexes"])
            indexes_item.setIcon(0, qta.icon('fa5s.folder', color=ColorScheme.ACCENT))
            
            for index in indexes:
                index_name = index[0]
                table_name = index[1]
                index_item = QTreeWidgetItem(indexes_item, [f"{index_name} (on {table_name})"])
                index_item.setIcon(0, self.index_icon)
        
        # Expand the database item
        db_item.setExpanded(True)
        tables_item.setExpanded(True)
    
    def load_duckdb_schema(self, connection):
        # Create root item for database
        db_item = QTreeWidgetItem(self, ["DuckDB Database"])
        db_item.setIcon(0, qta.icon('fa5s.database', color=ColorScheme.ACCENT))
        
        # Get list of tables
        tables_df = connection.execute("SHOW TABLES").fetchdf()
        
        # Tables group
        tables_item = QTreeWidgetItem(db_item, ["Tables"])
        tables_item.setIcon(0, qta.icon('fa5s.folder', color=ColorScheme.ACCENT))
        
        if tables_df.empty:
            # Show "No tables" message when database is empty
            no_tables_item = QTreeWidgetItem(tables_item, ["(No tables)"])
            no_tables_item.setIcon(0, qta.icon('fa5s.info-circle', color=ColorScheme.COMMENT))
        else:
            for _, row in tables_df.iterrows():
                table_name = row['name']
                table_item = QTreeWidgetItem(tables_item, [table_name])
                table_item.setIcon(0, self.table_icon)
                
                # Get columns for this table
                try:
                    columns_df = connection.execute(f"PRAGMA table_info('{table_name}')").fetchdf()
                    
                    for _, col_row in columns_df.iterrows():
                        col_name = col_row['name']
                        col_type = col_row['type']
                        is_pk = col_row['pk'] == 1  # Primary key flag
                        
                        column_text = f"{col_name} ({col_type})"
                        column_item = QTreeWidgetItem(table_item, [column_text])
                        column_item.setIcon(0, self.pk_icon if is_pk else self.column_icon)
                except Exception as e:
                    print(f"Error loading columns for table {table_name}: {e}")
        
        # Views group - DuckDB also supports views
        try:
            views_df = connection.execute("SELECT view_name FROM information_schema.views").fetchdf()
            
            if not views_df.empty:
                views_item = QTreeWidgetItem(db_item, ["Views"])
                views_item.setIcon(0, qta.icon('fa5s.folder', color=ColorScheme.ACCENT))
                
                for _, row in views_df.iterrows():
                    view_name = row['view_name']
                    view_item = QTreeWidgetItem(views_item, [view_name])
                    view_item.setIcon(0, self.view_icon)
        except:
            # Some versions of DuckDB might not have this view
            pass
        
        # Expand the database item
        db_item.setExpanded(True)
        tables_item.setExpanded(True)

# Main application window
class SQLEditorApp(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("SQL Editor")
        self.resize(1200, 800)
        self.connections = {}
        self.current_connection = None
        self.current_connection_info = None
        
        # Set application style
        self.setup_style()
        
        # Create central widget and layout
        self.central_widget = QWidget()
        self.setCentralWidget(self.central_widget)
        self.main_layout = QVBoxLayout(self.central_widget)
        self.main_layout.setContentsMargins(0, 0, 0, 0)
        
        # Create main splitter
        self.main_splitter = QSplitter(Qt.Orientation.Horizontal)
        
        # Create schema browser
        self.schema_browser = SchemaBrowser()
        self.schema_browser.setMinimumWidth(250)
        
        # Create tab widget for query editors
        self.tab_widget = QTabWidget()
        self.tab_widget.setTabsClosable(True)
        self.tab_widget.tabCloseRequested.connect(self.close_tab)
        
        # Add widgets to splitter
        self.main_splitter.addWidget(self.schema_browser)
        self.main_splitter.addWidget(self.tab_widget)
        self.main_splitter.setSizes([250, 950])
        
        # Add splitter to main layout
        self.main_layout.addWidget(self.main_splitter)
        
        # Create toolbar
        self.create_toolbar()
        
        # Create status bar
        self.status_bar = QStatusBar()
        self.setStatusBar(self.status_bar)
        self.connection_label = QLabel("Not connected")
        self.status_bar.addPermanentWidget(self.connection_label)
        
        # Create actions
        self.create_actions()
        
        # Create menus
        self.create_menus()
        
        # Add initial tab
        self.add_tab()
        
        # Auto-connect to main DuckDB database
        self.auto_connect_main_database()
        
        # Setup schema monitoring timer
        self.schema_timer = QTimer()
        self.schema_timer.timeout.connect(self.check_schema_changes)
        self.schema_timer.start(2000)  # Check every 2 seconds
        self.last_schema_hash = None
    
    def auto_connect_main_database(self):
        """Automatically connect to main.duckdb in the project folder"""
        try:
            # Get the current directory (where the SQL editor is running)
            current_dir = os.path.dirname(os.path.abspath(__file__))
            db_path = os.path.join(current_dir, "main.duckdb")
            
            # Create the database if it doesn't exist
            if not os.path.exists(db_path):
                # Create an empty DuckDB database
                temp_conn = duckdb.connect(db_path)
                temp_conn.close()
                self.status_bar.showMessage(f"Created new database: {db_path}")
            
            # Connection info for the main database
            connection_info = {
                'type': 'duckdb',
                'path': db_path,
                'name': 'main.duckdb'
            }
            
            # Connect to the database
            self.connect_to_database(connection_info)
            
            # Force an immediate schema refresh after connection
            QTimer.singleShot(100, self.refresh_schema_browser)
            
            self.status_bar.showMessage(f"Auto-connected to main.duckdb")
            
        except Exception as e:
            self.status_bar.showMessage(f"Failed to auto-connect: {str(e)}")
            print(f"Auto-connect error: {e}")
    
    def refresh_schema_browser(self):
        """Refresh the schema browser immediately"""
        if self.current_connection and self.current_connection_info:
            self.schema_browser.load_schema(self.current_connection, self.current_connection_info)
    
    def check_schema_changes(self):
        """Check for schema changes and refresh if needed"""
        if not self.current_connection or not self.current_connection_info:
            return
        
        try:
            # Get current schema hash
            if self.current_connection_info['type'].lower() == 'duckdb':
                # Get table list for DuckDB
                result = self.current_connection.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'").fetchall()
                schema_data = str(sorted([row[0] for row in result]))
            else:
                # Get table list for SQLite
                cursor = self.current_connection.cursor()
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
                schema_data = str(sorted([row[0] for row in cursor.fetchall()]))
            
            # Calculate hash
            import hashlib
            current_hash = hashlib.md5(schema_data.encode()).hexdigest()
            
            # Check if schema changed
            if self.last_schema_hash is not None and current_hash != self.last_schema_hash:
                self.refresh_schema_browser()
            
            self.last_schema_hash = current_hash
            
        except Exception as e:
            # Silently ignore errors in schema monitoring
            pass
    
    def setup_style(self):
        # Set dark theme
        palette = QPalette()
        palette.setColor(QPalette.ColorRole.Window, ColorScheme.BACKGROUND)
        palette.setColor(QPalette.ColorRole.WindowText, ColorScheme.TEXT)
        palette.setColor(QPalette.ColorRole.Base, ColorScheme.BACKGROUND)
        palette.setColor(QPalette.ColorRole.AlternateBase, QColor(53, 53, 53))
        palette.setColor(QPalette.ColorRole.ToolTipBase, ColorScheme.BACKGROUND)
        palette.setColor(QPalette.ColorRole.ToolTipText, ColorScheme.TEXT)
        palette.setColor(QPalette.ColorRole.Text, ColorScheme.TEXT)
        palette.setColor(QPalette.ColorRole.Button, ColorScheme.BACKGROUND)
        palette.setColor(QPalette.ColorRole.ButtonText, ColorScheme.TEXT)
        palette.setColor(QPalette.ColorRole.BrightText, Qt.GlobalColor.red)
        palette.setColor(QPalette.ColorRole.Link, ColorScheme.ACCENT)
        palette.setColor(QPalette.ColorRole.Highlight, ColorScheme.HIGHLIGHT)
        palette.setColor(QPalette.ColorRole.HighlightedText, Qt.GlobalColor.black)
        
        QApplication.setPalette(palette)
    
    def create_toolbar(self):
        self.toolbar = QToolBar("Main Toolbar")
        self.toolbar.setIconSize(QSize(20, 20))
        self.toolbar.setMovable(False)
        self.addToolBar(self.toolbar)
        
        # Create Database button
        self.create_db_button = QToolButton()
        self.create_db_button.setIcon(qta.icon('fa5s.plus-circle', color=ColorScheme.SUCCESS))
        self.create_db_button.setText("Create DB")
        self.create_db_button.setToolButtonStyle(Qt.ToolButtonStyle.ToolButtonTextBesideIcon)
        self.create_db_button.clicked.connect(self.show_create_database_dialog)
        self.toolbar.addWidget(self.create_db_button)
        
        # Connect button
        self.connect_button = QToolButton()
        self.connect_button.setIcon(qta.icon('fa5s.plug', color=ColorScheme.TEXT))
        self.connect_button.setText("Connect")
        self.connect_button.setToolButtonStyle(Qt.ToolButtonStyle.ToolButtonTextBesideIcon)
        self.connect_button.clicked.connect(self.show_connection_dialog)
        self.toolbar.addWidget(self.connect_button)
        
        # Reconnect to Main DB button
        self.reconnect_main_button = QToolButton()
        self.reconnect_main_button.setIcon(qta.icon('fa5s.home', color=ColorScheme.ACCENT))
        self.reconnect_main_button.setText("Main DB")
        self.reconnect_main_button.setToolButtonStyle(Qt.ToolButtonStyle.ToolButtonTextBesideIcon)
        self.reconnect_main_button.setToolTip("Reconnect to main.duckdb")
        self.reconnect_main_button.clicked.connect(self.auto_connect_main_database)
        self.toolbar.addWidget(self.reconnect_main_button)
        
        self.toolbar.addSeparator()
        
        # Execute button
        self.execute_button = QToolButton()
        self.execute_button.setIcon(qta.icon('fa5s.play', color=ColorScheme.SUCCESS))
        self.execute_button.setText("Execute")
        self.execute_button.setToolButtonStyle(Qt.ToolButtonStyle.ToolButtonTextBesideIcon)
        self.execute_button.clicked.connect(self.execute_current_query)
        self.toolbar.addWidget(self.execute_button)
        
        # New tab button
        self.new_tab_button = QToolButton()
        self.new_tab_button.setIcon(qta.icon('fa5s.plus', color=ColorScheme.TEXT))
        self.new_tab_button.setText("New Tab")
        self.new_tab_button.setToolButtonStyle(Qt.ToolButtonStyle.ToolButtonTextBesideIcon)
        self.new_tab_button.clicked.connect(self.add_tab)
        self.toolbar.addWidget(self.new_tab_button)
        
        self.toolbar.addSeparator()
        
        # Save button
        self.save_button = QToolButton()
        self.save_button.setIcon(qta.icon('fa5s.save', color=ColorScheme.TEXT))
        self.save_button.setText("Save")
        self.save_button.setToolButtonStyle(Qt.ToolButtonStyle.ToolButtonTextBesideIcon)
        self.save_button.clicked.connect(self.save_query)
        self.toolbar.addWidget(self.save_button)
        
        # Open button
        self.open_button = QToolButton()
        self.open_button.setIcon(qta.icon('fa5s.folder-open', color=ColorScheme.TEXT))
        self.open_button.setText("Open")
        self.open_button.setToolButtonStyle(Qt.ToolButtonStyle.ToolButtonTextBesideIcon)
        self.open_button.clicked.connect(self.open_query)
        self.toolbar.addWidget(self.open_button)
        
        self.toolbar.addSeparator()
        
        # Import Data button
        self.import_data_button = QToolButton()
        self.import_data_button.setIcon(qta.icon('fa5s.file-import', color=ColorScheme.SUCCESS))
        self.import_data_button.setText("Import Data")
        self.import_data_button.setToolButtonStyle(Qt.ToolButtonStyle.ToolButtonTextBesideIcon)
        self.import_data_button.setToolTip("Import data from CSV, Excel, Parquet, JSON files")
        self.import_data_button.clicked.connect(self.show_import_dialog)
        self.toolbar.addWidget(self.import_data_button)
    
    def create_actions(self):
        # File actions
        self.new_action = QAction(qta.icon('fa5s.file', color=ColorScheme.TEXT), "&New Query", self)
        self.new_action.setShortcut(QKeySequence.StandardKey.New)
        self.new_action.triggered.connect(self.add_tab)
        
        self.open_action = QAction(qta.icon('fa5s.folder-open', color=ColorScheme.TEXT), "&Open Query...", self)
        self.open_action.setShortcut(QKeySequence.StandardKey.Open)
        self.open_action.triggered.connect(self.open_query)
        
        self.save_action = QAction(qta.icon('fa5s.save', color=ColorScheme.TEXT), "&Save Query", self)
        self.save_action.setShortcut(QKeySequence.StandardKey.Save)
        self.save_action.triggered.connect(self.save_query)
        
        self.save_as_action = QAction("Save Query &As...", self)
        self.save_as_action.setShortcut(QKeySequence.StandardKey.SaveAs)
        self.save_as_action.triggered.connect(self.save_query_as)
        
        self.exit_action = QAction("E&xit", self)
        self.exit_action.setShortcut(QKeySequence.StandardKey.Quit)
        self.exit_action.triggered.connect(self.close)
        
        # Database actions
        self.connect_action = QAction(qta.icon('fa5s.plug', color=ColorScheme.TEXT), "&Connect to Database...", self)
        self.connect_action.triggered.connect(self.show_connection_dialog)
        
        self.disconnect_action = QAction(qta.icon('fa5s.power-off', color=ColorScheme.ERROR), "&Disconnect", self)
        self.disconnect_action.triggered.connect(self.disconnect_database)
        self.disconnect_action.setEnabled(False)
        
        self.reconnect_main_action = QAction(qta.icon('fa5s.home', color=ColorScheme.ACCENT), "Reconnect to &Main Database", self)
        self.reconnect_main_action.setShortcut("Ctrl+M")
        self.reconnect_main_action.triggered.connect(self.auto_connect_main_database)
        
        self.import_data_action = QAction(qta.icon('fa5s.file-import', color=ColorScheme.SUCCESS), "&Import Data...", self)
        self.import_data_action.setShortcut("Ctrl+I")
        self.import_data_action.triggered.connect(self.show_import_dialog)
        
        # Query actions
        self.execute_action = QAction(qta.icon('fa5s.play', color=ColorScheme.SUCCESS), "&Execute Query", self)
        self.execute_action.setShortcut("F5")
        self.execute_action.triggered.connect(self.execute_current_query)
        
        # Create keyboard shortcut for executing query with Ctrl+Enter
        self.execute_shortcut = QShortcut(QKeySequence("Ctrl+Return"), self)
        self.execute_shortcut.activated.connect(self.execute_current_query)
    
    def create_menus(self):
        # File menu
        self.file_menu = self.menuBar().addMenu("&File")
        self.file_menu.addAction(self.new_action)
        self.file_menu.addAction(self.open_action)
        self.file_menu.addSeparator()
        self.file_menu.addAction(self.save_action)
        self.file_menu.addAction(self.save_as_action)
        self.file_menu.addSeparator()
        self.file_menu.addAction(self.exit_action)
        
        # Database menu
        self.db_menu = self.menuBar().addMenu("&Database")
        
        # Create new database action
        self.create_db_action = QAction(qta.icon('fa5s.plus-circle'), "&Create New Database", self)
        self.create_db_action.setShortcut(QKeySequence("Ctrl+N"))
        self.create_db_action.triggered.connect(self.show_create_database_dialog)
        
        self.db_menu.addAction(self.create_db_action)
        self.db_menu.addSeparator()
        self.db_menu.addAction(self.connect_action)
        self.db_menu.addAction(self.reconnect_main_action)
        self.db_menu.addAction(self.disconnect_action)
        self.db_menu.addSeparator()
        self.db_menu.addAction(self.import_data_action)
        
        # Query menu
        self.query_menu = self.menuBar().addMenu("&Query")
        self.query_menu.addAction(self.execute_action)
    
    def add_tab(self):
        # Create new query tab
        tab = QueryTab(connection=self.current_connection, connection_info=self.current_connection_info)
        # Connect schema change signal
        tab.schema_changed.connect(self.refresh_schema_browser)
        tab_index = self.tab_widget.addTab(tab, f"Query {self.tab_widget.count() + 1}")
        self.tab_widget.setCurrentIndex(tab_index)
        tab.editor.setFocus()
    
    def close_tab(self, index):
        if self.tab_widget.count() > 1:
            self.tab_widget.removeTab(index)
        else:
            # If it's the last tab, clear it instead of closing
            tab = self.tab_widget.widget(0)
            tab.editor.clear()
    
    def show_connection_dialog(self):
        dialog = ConnectionDialog(self)
        if dialog.exec():
            connection_info = dialog.get_connection_info()
            self.connect_to_database(connection_info)
    
    def show_create_database_dialog(self):
        dialog = CreateDatabaseDialog(self)
        if dialog.exec():
            connection_info = dialog.get_connection_info()
            if connection_info and connection_info.get('file_path'):
                self.connect_to_database(connection_info)
    
    def show_import_dialog(self):
        if not self.current_connection:
            QMessageBox.warning(self, "No Connection", "Please connect to a database first.")
            return
        
        dialog = DataImportDialog(self, self.current_connection, self.current_connection_info)
        if dialog.exec():
            import_info = dialog.get_import_info()
            
            # For create mode, still ask for table name confirmation
            if import_info['mode'] == 'create':
                suggested_name = import_info['table_name'] or self.suggest_table_name(import_info['file_path'])
                table_name = self.show_table_name_dialog(suggested_name, import_info['file_path'], 'create')
                if table_name:
                    import_info['table_name'] = table_name
                else:
                    return  # User cancelled or didn't provide a name
            
            # For append and replace modes, table name is already selected from dropdown
            self.import_data(import_info)
    
    def show_table_name_dialog(self, suggested_name, file_path, mode='create'):
        """Show a custom dialog for table name input with better visibility"""
        dialog = QDialog(self)
        dialog.setWindowTitle("Create New Table")
        dialog.setModal(True)
        dialog.resize(450, 200)
        
        # Set dialog style
        dialog.setStyleSheet(f"""
            QDialog {{
                background-color: {ColorScheme.BACKGROUND.name()};
                color: {ColorScheme.TEXT.name()};
            }}
            QLabel {{
                color: {ColorScheme.TEXT.name()};
                font-size: 12px;
                margin: 5px;
            }}
            QLineEdit {{
                background-color: {ColorScheme.SIDEBAR_BG.name()};
                color: {ColorScheme.TEXT.name()};
                border: 2px solid {ColorScheme.ACCENT.name()};
                border-radius: 5px;
                padding: 8px;
                font-size: 14px;
                font-weight: bold;
            }}
            QLineEdit:focus {{
                border-color: {ColorScheme.SUCCESS.name()};
            }}
            QPushButton {{
                background-color: {ColorScheme.ACCENT.name()};
                color: white;
                border: none;
                border-radius: 5px;
                padding: 8px 16px;
                font-size: 12px;
                font-weight: bold;
                min-width: 80px;
            }}
            QPushButton:hover {{
                background-color: {ColorScheme.SUCCESS.name()};
            }}
            QPushButton:pressed {{
                background-color: {ColorScheme.HIGHLIGHT.name()};
            }}
        """)
        
        layout = QVBoxLayout(dialog)
        layout.setSpacing(15)
        layout.setContentsMargins(20, 20, 20, 20)
        
        # Title label
        title_label = QLabel("🆕 Create New Table")
        title_label.setStyleSheet("font-size: 16px; font-weight: bold; color: #4FC3F7; margin-bottom: 10px;")
        layout.addWidget(title_label)
        
        # File info label
        file_info = QLabel(f"📁 Importing from: {os.path.basename(file_path)}")
        file_info.setStyleSheet("font-size: 11px; color: #B0BEC5; margin-bottom: 5px;")
        layout.addWidget(file_info)
        
        # Instruction label
        instruction_label = QLabel("Please enter a name for the new table:")
        instruction_label.setStyleSheet("font-size: 12px; margin-bottom: 5px;")
        layout.addWidget(instruction_label)
        
        # Table name input
        table_name_input = QLineEdit()
        table_name_input.setText(suggested_name)
        table_name_input.selectAll()  # Select all text for easy editing
        table_name_input.setPlaceholderText("Enter table name...")
        layout.addWidget(table_name_input)
        
        # Validation label
        validation_label = QLabel("✓ Table name looks good!")
        validation_label.setStyleSheet("font-size: 10px; color: #4CAF50; margin-top: 5px;")
        layout.addWidget(validation_label)
        
        # Button layout
        button_layout = QHBoxLayout()
        button_layout.addStretch()
        
        cancel_button = QPushButton("Cancel")
        cancel_button.setStyleSheet("""
            QPushButton {
                background-color: #757575;
            }
            QPushButton:hover {
                background-color: #9E9E9E;
            }
        """)
        create_button = QPushButton("Create Table")
        
        button_layout.addWidget(cancel_button)
        button_layout.addWidget(create_button)
        layout.addLayout(button_layout)
        
        # Validation function
        def validate_table_name():
            name = table_name_input.text().strip()
            if not name:
                validation_label.setText("⚠️ Please enter a table name")
                validation_label.setStyleSheet("font-size: 10px; color: #FF9800;")
                create_button.setEnabled(False)
            elif not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', name):
                validation_label.setText("⚠️ Use only letters, numbers, and underscores. Must start with letter or underscore.")
                validation_label.setStyleSheet("font-size: 10px; color: #FF5722;")
                create_button.setEnabled(False)
            else:
                validation_label.setText("✓ Table name looks good!")
                validation_label.setStyleSheet("font-size: 10px; color: #4CAF50;")
                create_button.setEnabled(True)
        
        # Connect validation
        table_name_input.textChanged.connect(validate_table_name)
        
        # Connect buttons
        cancel_button.clicked.connect(dialog.reject)
        create_button.clicked.connect(dialog.accept)
        
        # Allow Enter key to create table
        table_name_input.returnPressed.connect(lambda: dialog.accept() if create_button.isEnabled() else None)
        
        # Set focus and validate initially
        table_name_input.setFocus()
        validate_table_name()
        
        # Show dialog and return result
        if dialog.exec() == QDialog.DialogCode.Accepted:
            return table_name_input.text().strip()
        return None
    

    
    def suggest_table_name(self, file_path):
        """Suggest a table name based on the file path"""
        base_name = os.path.splitext(os.path.basename(file_path))[0]
        # Clean table name (remove special characters, replace with underscores)
        clean_name = re.sub(r'[^a-zA-Z0-9_]', '_', base_name).lower()
        # Remove consecutive underscores
        clean_name = re.sub(r'_+', '_', clean_name)
        # Remove leading/trailing underscores
        clean_name = clean_name.strip('_')
        # Ensure it starts with a letter or underscore
        if clean_name and not clean_name[0].isalpha() and clean_name[0] != '_':
            clean_name = f"table_{clean_name}"
        # Handle empty names
        if not clean_name:
            clean_name = "new_table"
        return clean_name
    
    def connect_to_database(self, connection_info):
        try:
            db_type = connection_info["type"]
            # Handle both 'file_path' and 'path' keys for compatibility
            file_path = connection_info.get("file_path") or connection_info.get("path")
            
            if not file_path:
                QMessageBox.warning(self, "Connection Error", "Please specify a database file.")
                return
            
            # Check if we already have this connection
            connection_key = f"{db_type}:{file_path}"
            if connection_key in self.connections:
                self.current_connection = self.connections[connection_key]
                self.current_connection_info = connection_info
            else:
                # Create new connection
                if db_type.lower() in ["sqlite", "sqlite3"]:
                    connection = sqlite3.connect(file_path)
                elif db_type.lower() == "duckdb":
                    connection = duckdb.connect(file_path)
                else:
                    raise ValueError(f"Unsupported database type: {db_type}")
                
                self.connections[connection_key] = connection
                self.current_connection = connection
                self.current_connection_info = connection_info
            
            # Update UI - show special indicator for main database
            db_name = connection_info.get("name", os.path.basename(file_path))
            if db_name == "main.duckdb":
                self.connection_label.setText(f"🏠 {db_name} (Main Database)")
            else:
                self.connection_label.setText(f"Connected to {db_name} ({db_type})")
            
            self.disconnect_action.setEnabled(True)
            
            # Update schema browser
            self.schema_browser.load_schema(self.current_connection, self.current_connection_info)
            
            # Update all tabs with the new connection
            for i in range(self.tab_widget.count()):
                tab = self.tab_widget.widget(i)
                tab.connection = self.current_connection
                tab.connection_info = self.current_connection_info
                # Connect schema change signal if not already connected
                try:
                    tab.schema_changed.connect(self.refresh_schema_browser)
                except:
                    pass  # Signal might already be connected
            
            self.statusBar().showMessage(f"Connected to {file_path}", 3000)
            
        except Exception as e:
            QMessageBox.critical(self, "Connection Error", f"Failed to connect to database: {str(e)}")
    
    def disconnect_database(self):
        if self.current_connection:
            # Close the connection
            file_path = self.current_connection_info.get("file_path") or self.current_connection_info.get("path")
            connection_key = f"{self.current_connection_info['type']}:{file_path}"
            if connection_key in self.connections:
                try:
                    self.connections[connection_key].close()
                    del self.connections[connection_key]
                except:
                    pass
            
            # Update UI
            self.current_connection = None
            self.current_connection_info = None
            self.connection_label.setText("Not connected")
            self.disconnect_action.setEnabled(False)
            self.schema_browser.clear()
            
            # Update all tabs
            for i in range(self.tab_widget.count()):
                tab = self.tab_widget.widget(i)
                tab.connection = None
                tab.connection_info = None
            
            self.statusBar().showMessage("Disconnected from database", 3000)
    
    def import_data(self, import_info):
        """Import data from file to database"""
        try:
            file_path = import_info['file_path']
            table_name = import_info['table_name']
            file_type = import_info['file_type']
            mode = import_info['mode']
            
            # Show progress
            self.statusBar().showMessage(f"Importing {os.path.basename(file_path)}...")
            
            # Load data based on file type
            df = None
            
            if file_type == '.csv':
                df = pd.read_csv(
                    file_path,
                    delimiter=import_info.get('delimiter', ','),
                    encoding=import_info.get('encoding', 'utf-8'),
                    header=0 if import_info.get('header', True) else None
                )
            
            elif file_type == '.tsv':
                df = pd.read_csv(
                    file_path,
                    delimiter=import_info.get('delimiter', '\t'),
                    encoding=import_info.get('encoding', 'utf-8'),
                    header=0 if import_info.get('header', True) else None
                )
            
            elif file_type == '.txt':
                df = pd.read_csv(
                    file_path,
                    delimiter=import_info.get('delimiter', ','),
                    encoding=import_info.get('encoding', 'utf-8'),
                    header=0 if import_info.get('header', True) else None
                )
            
            elif file_type in ['.xlsx', '.xls']:
                sheet_name = import_info.get('sheet_name', 0)
                df = pd.read_excel(file_path, sheet_name=sheet_name)
            
            elif file_type == '.parquet':
                df = pd.read_parquet(file_path)
            
            elif file_type == '.json':
                df = pd.read_json(file_path)
            
            else:
                raise ValueError(f"Unsupported file type: {file_type}")
            
            if df is None or df.empty:
                QMessageBox.warning(self, "Import Error", "No data found in the file.")
                return
            
            # Clean column names for database compatibility
            df.columns = [self.clean_column_name(col) for col in df.columns]
            
            # Handle import mode
            if mode == 'replace':
                # Drop table if exists
                try:
                    if self.current_connection_info['type'].lower() == 'duckdb':
                        self.current_connection.execute(f"DROP TABLE IF EXISTS {table_name}")
                    else:  # SQLite
                        self.current_connection.execute(f"DROP TABLE IF EXISTS {table_name}")
                        self.current_connection.commit()
                except:
                    pass  # Table might not exist
            
            # Import data to database
            if self.current_connection_info['type'].lower() == 'duckdb':
                # DuckDB import
                if mode == 'create':
                    # Check if table exists for create mode
                    try:
                        result = self.current_connection.execute(f"SELECT 1 FROM {table_name} LIMIT 1").fetchone()
                        if result is not None:
                            raise ValueError(f"Table '{table_name}' already exists. Use 'Replace' mode to overwrite or 'Append' to add data.")
                    except:
                        pass  # Table doesn't exist, which is what we want for create mode
                    df.to_sql(table_name, self.current_connection, if_exists='fail', index=False)
                elif mode == 'append':
                    self.flexible_append_data(df, table_name, 'duckdb')
                else:  # replace
                    df.to_sql(table_name, self.current_connection, if_exists='replace', index=False)
            else:
                # SQLite import
                if mode == 'create':
                    # Check if table exists for create mode
                    cursor = self.current_connection.cursor()
                    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table_name,))
                    if cursor.fetchone():
                        raise ValueError(f"Table '{table_name}' already exists. Use 'Replace' mode to overwrite or 'Append' to add data.")
                    df.to_sql(table_name, self.current_connection, if_exists='fail', index=False)
                elif mode == 'append':
                    self.flexible_append_data(df, table_name, 'sqlite')
                else:  # replace
                    df.to_sql(table_name, self.current_connection, if_exists='replace', index=False)
                self.current_connection.commit()
            
            # Update schema browser immediately
            self.refresh_schema_browser()
            
            # Show success message
            rows, cols = df.shape
            mode_text = {
                'create': 'created',
                'append': 'appended to',
                'replace': 'replaced'
            }[mode]
            
            QMessageBox.information(
                self, 
                "Import Successful", 
                f"Successfully {mode_text} table '{table_name}'!\n\n"
                f"📊 {rows:,} rows × {cols} columns imported\n"
                f"📁 Source: {os.path.basename(file_path)}"
            )
            
            self.statusBar().showMessage(f"Import completed: {rows:,} rows imported to '{table_name}'", 5000)
            
            # Add a query to the current tab to show the imported data
            current_tab = self.tab_widget.currentWidget()
            if current_tab and not current_tab.editor.toPlainText().strip():
                current_tab.editor.setPlainText(f"SELECT * FROM {table_name} LIMIT 100;")
            
        except Exception as e:
            QMessageBox.critical(self, "Import Error", f"Failed to import data: {str(e)}")
            self.statusBar().showMessage(f"Import failed: {str(e)}", 5000)
    
    def flexible_append_data(self, df, table_name, db_type):
        """Append data with flexible column handling - adds missing columns automatically"""
        try:
            # Get existing table schema
            if db_type == 'duckdb':
                try:
                    existing_columns_df = self.current_connection.execute(f"PRAGMA table_info('{table_name}')").fetchdf()
                    existing_columns = set(existing_columns_df['name'].tolist())
                except:
                    # Table doesn't exist, create it
                    df.to_sql(table_name, self.current_connection, if_exists='fail', index=False)
                    return
            else:  # sqlite
                try:
                    cursor = self.current_connection.cursor()
                    cursor.execute(f"PRAGMA table_info({table_name})")
                    existing_columns = set([row[1] for row in cursor.fetchall()])
                except:
                    # Table doesn't exist, create it
                    df.to_sql(table_name, self.current_connection, if_exists='fail', index=False)
                    self.current_connection.commit()
                    return
            
            # Get new columns from the dataframe
            new_columns = set(df.columns)
            
            # Find columns that need to be added to the existing table
            missing_columns = new_columns - existing_columns
            
            # Add missing columns to the existing table
            if missing_columns:
                print(f"Adding new columns to table '{table_name}': {', '.join(missing_columns)}")
                for col in missing_columns:
                    # Determine column type based on data
                    sample_value = df[col].dropna().iloc[0] if not df[col].dropna().empty else None
                    
                    if sample_value is None:
                        col_type = "TEXT"
                    elif isinstance(sample_value, (int, pd.Int64Dtype)):
                        col_type = "INTEGER"
                    elif isinstance(sample_value, (float, pd.Float64Dtype)):
                        col_type = "REAL"
                    elif isinstance(sample_value, bool):
                        col_type = "BOOLEAN"
                    else:
                        col_type = "TEXT"
                    
                    # Add the column
                    alter_sql = f"ALTER TABLE {table_name} ADD COLUMN {col} {col_type}"
                    self.current_connection.execute(alter_sql)
                    
                if db_type == 'sqlite':
                    self.current_connection.commit()
            
            # Find columns in the table that are not in the new data
            extra_table_columns = existing_columns - new_columns
            
            # Add missing columns to the dataframe with NULL values
            for col in extra_table_columns:
                df[col] = None
            
            # Reorder dataframe columns to match table schema (existing + new)
            all_columns = list(existing_columns) + list(missing_columns)
            df = df.reindex(columns=all_columns)
            
            # Now append the data
            df.to_sql(table_name, self.current_connection, if_exists='append', index=False)
            
            if db_type == 'sqlite':
                self.current_connection.commit()
                
            # Show info about column changes
            if missing_columns:
                self.statusBar().showMessage(
                    f"Added {len(missing_columns)} new columns: {', '.join(list(missing_columns)[:3])}{'...' if len(missing_columns) > 3 else ''}", 
                    5000
                )
                
        except Exception as e:
            # Fallback to regular append if flexible append fails
            print(f"Flexible append failed, falling back to regular append: {e}")
            df.to_sql(table_name, self.current_connection, if_exists='append', index=False)
            if db_type == 'sqlite':
                self.current_connection.commit()
    
    def clean_column_name(self, column_name):
        """Clean column name for database compatibility"""
        # Convert to string and strip whitespace
        clean_name = str(column_name).strip()
        
        # Replace spaces and special characters with underscores
        clean_name = re.sub(r'[^a-zA-Z0-9_]', '_', clean_name)
        
        # Remove consecutive underscores
        clean_name = re.sub(r'_+', '_', clean_name)
        
        # Remove leading/trailing underscores
        clean_name = clean_name.strip('_')
        
        # Ensure it starts with a letter or underscore
        if clean_name and not clean_name[0].isalpha() and clean_name[0] != '_':
            clean_name = f"col_{clean_name}"
        
        # Handle empty names
        if not clean_name:
            clean_name = "unnamed_column"
        
        return clean_name
    
    def execute_current_query(self):
        current_tab = self.tab_widget.currentWidget()
        if current_tab:
            current_tab.execute_query()
    
    def save_query(self):
        current_tab = self.tab_widget.currentWidget()
        if not current_tab:
            return
            
        # Get the query text
        query_text = current_tab.editor.toPlainText()
        if not query_text.strip():
            return
            
        # Get file path
        file_path, _ = QFileDialog.getSaveFileName(
            self, "Save Query", "", "SQL Files (*.sql);;All Files (*)"
        )
        
        if file_path:
            try:
                with open(file_path, 'w') as f:
                    f.write(query_text)
                self.statusBar().showMessage(f"Query saved to {file_path}", 3000)
                
                # Update tab title
                self.tab_widget.setTabText(
                    self.tab_widget.currentIndex(), os.path.basename(file_path)
                )
            except Exception as e:
                QMessageBox.critical(self, "Save Error", f"Failed to save query: {str(e)}")
    
    def save_query_as(self):
        self.save_query()
    
    def open_query(self):
        file_path, _ = QFileDialog.getOpenFileName(
            self, "Open Query", "", "SQL Files (*.sql);;All Files (*)"
        )
        
        if file_path:
            try:
                with open(file_path, 'r') as f:
                    query_text = f.read()
                
                # Create new tab or use current if empty
                current_tab = self.tab_widget.currentWidget()
                if current_tab and not current_tab.editor.toPlainText().strip():
                    tab = current_tab
                    tab_index = self.tab_widget.currentIndex()
                else:
                    tab = QueryTab(connection=self.current_connection, connection_info=self.current_connection_info)
                    tab_index = self.tab_widget.addTab(tab, os.path.basename(file_path))
                    self.tab_widget.setCurrentIndex(tab_index)
                
                # Set query text
                tab.editor.setPlainText(query_text)
                self.tab_widget.setTabText(tab_index, os.path.basename(file_path))
                self.statusBar().showMessage(f"Opened {file_path}", 3000)
            except Exception as e:
                QMessageBox.critical(self, "Open Error", f"Failed to open query: {str(e)}")

# Main entry point
def main():
    app = QApplication(sys.argv)
    window = SQLEditorApp()
    window.show()
    sys.exit(app.exec())

if __name__ == "__main__":
    main()
