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
import warnings

# Suppress pandas warnings about DuckDB connections
warnings.filterwarnings('ignore', message='pandas only supports SQLAlchemy connectable.*')

# Additional imports for export functionality
try:
    import openpyxl
except ImportError:
    openpyxl = None

# Import for CSV delimiter detection
try:
    import csv
except ImportError:
    csv = None

from PyQt6.QtWidgets import (
    QApplication, QMainWindow, QTabWidget, QWidget, QVBoxLayout, QHBoxLayout,
    QSplitter, QTreeWidget, QTreeWidgetItem, QTextEdit, QTableView, QHeaderView,
    QLabel, QPushButton, QComboBox, QFileDialog, QMessageBox, QDialog, QLineEdit,
    QFormLayout, QDialogButtonBox, QToolBar, QStatusBar, QMenu, QInputDialog,
    QSizePolicy, QFrame, QToolButton, QGroupBox, QRadioButton, QCheckBox, QListWidget,
    QCompleter, QListWidgetItem, QProgressDialog, QGridLayout, QScrollArea
)
from PyQt6.QtGui import (
    QAction, QFont, QColor, QSyntaxHighlighter, QTextCharFormat, QIcon,
    QTextCursor, QPalette, QKeySequence, QShortcut, QStandardItemModel, QStandardItem
)
from PyQt6.QtCore import (
    Qt, QAbstractTableModel, QModelIndex, QSize, QThread, pyqtSignal,
    QRegularExpression, QSettings, QTimer, QStringListModel, pyqtSlot
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

# SQL Auto-completion and suggestions
class SQLCompleter(QCompleter):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setup_completions()
        self.setCaseSensitivity(Qt.CaseSensitivity.CaseInsensitive)
        self.setCompletionMode(QCompleter.CompletionMode.PopupCompletion)
        self.setWrapAround(False)
        
        # Customize the popup appearance
        popup = self.popup()
        popup.setStyleSheet("""
            QListView {
                background-color: #2d2d2d;
                color: #f0f0f0;
                border: 1px solid #4a4a4a;
                selection-background-color: #3a7bd5;
                selection-color: white;
                outline: none;
            }
            QListView::item {
                padding: 5px;
                border-bottom: 1px solid #3a3a3a;
            }
            QListView::item:hover {
                background-color: #3a3a3a;
            }
            QListView::item:selected {
                background-color: #3a7bd5;
            }
        """)
        
    def setup_completions(self):
        # SQL Keywords and commands
        self.sql_keywords = [
            # Core SQL commands
            'SELECT', 'FROM', 'WHERE', 'INSERT', 'UPDATE', 'DELETE', 'CREATE', 'ALTER', 'DROP',
            'TRUNCATE', 'BEGIN', 'COMMIT', 'ROLLBACK', 'SAVEPOINT', 'RELEASE',
            
            # Clauses and modifiers
            'DISTINCT', 'ALL', 'AS', 'INTO', 'VALUES', 'SET', 'ON', 'USING',
            'GROUP BY', 'ORDER BY', 'HAVING', 'LIMIT', 'OFFSET', 'FETCH',
            'UNION', 'UNION ALL', 'INTERSECT', 'EXCEPT', 'MINUS',
            
            # Joins
            'JOIN', 'INNER JOIN', 'LEFT JOIN', 'RIGHT JOIN', 'FULL JOIN',
            'CROSS JOIN', 'NATURAL JOIN', 'LEFT OUTER JOIN', 'RIGHT OUTER JOIN',
            'FULL OUTER JOIN',
            
            # Conditional logic
            'CASE', 'WHEN', 'THEN', 'ELSE', 'END', 'IF', 'IFNULL', 'NULLIF',
            
            # Operators
            'AND', 'OR', 'NOT', 'IN', 'NOT IN', 'LIKE', 'NOT LIKE', 'ILIKE',
            'BETWEEN', 'NOT BETWEEN', 'EXISTS', 'NOT EXISTS', 'IS', 'IS NOT',
            'IS NULL', 'IS NOT NULL', 'REGEXP', 'RLIKE',
            
            # Window functions
            'OVER', 'PARTITION BY', 'WINDOW', 'ROWS', 'RANGE', 'PRECEDING',
            'FOLLOWING', 'UNBOUNDED', 'CURRENT ROW',
            
            # CTEs and subqueries
            'WITH', 'RECURSIVE', 'LATERAL',
            
            # Data types
            'INTEGER', 'INT', 'BIGINT', 'SMALLINT', 'TINYINT', 'SERIAL',
            'VARCHAR', 'CHAR', 'TEXT', 'STRING', 'CLOB', 'NVARCHAR', 'NCHAR',
            'REAL', 'FLOAT', 'DOUBLE', 'NUMERIC', 'DECIMAL', 'MONEY',
            'DATE', 'TIME', 'TIMESTAMP', 'DATETIME', 'YEAR', 'INTERVAL',
            'BOOLEAN', 'BOOL', 'BIT', 'BLOB', 'BINARY', 'VARBINARY',
            'JSON', 'JSONB', 'XML', 'UUID', 'ARRAY',
            
            # Constraints and table operations
            'PRIMARY KEY', 'FOREIGN KEY', 'UNIQUE', 'NOT NULL', 'DEFAULT',
            'CHECK', 'REFERENCES', 'CASCADE', 'RESTRICT', 'SET NULL', 'SET DEFAULT',
            'AUTO_INCREMENT', 'IDENTITY', 'GENERATED', 'ALWAYS', 'BY DEFAULT',
            
            # Database objects
            'TABLE', 'VIEW', 'INDEX', 'TRIGGER', 'PROCEDURE', 'FUNCTION',
            'SCHEMA', 'DATABASE', 'CATALOG', 'SEQUENCE', 'DOMAIN', 'TYPE',
            
            # Permissions and security
            'GRANT', 'REVOKE', 'DENY', 'ROLE', 'USER', 'LOGIN', 'PASSWORD',
            'PRIVILEGES', 'USAGE', 'EXECUTE', 'REFERENCES', 'TEMPORARY',
            
            # Optimization and hints
            'EXPLAIN', 'ANALYZE', 'VERBOSE', 'COSTS', 'BUFFERS', 'TIMING',
            'PLAN', 'EXECUTION', 'STATISTICS', 'HINT', 'USE INDEX', 'FORCE INDEX'
        ]
        
        # SQL Functions
        self.sql_functions = [
            # Aggregate functions
            'COUNT', 'SUM', 'AVG', 'MIN', 'MAX', 'STDDEV', 'VARIANCE',
            'GROUP_CONCAT', 'STRING_AGG', 'ARRAY_AGG', 'JSON_AGG',
            
            # String functions
            'CONCAT', 'SUBSTRING', 'SUBSTR', 'LENGTH', 'CHAR_LENGTH',
            'UPPER', 'LOWER', 'TRIM', 'LTRIM', 'RTRIM', 'REPLACE',
            'REGEXP_REPLACE', 'SPLIT_PART', 'POSITION', 'INSTR',
            'LEFT', 'RIGHT', 'REVERSE', 'REPEAT', 'LPAD', 'RPAD',
            'ASCII', 'CHR', 'INITCAP', 'TRANSLATE', 'SOUNDEX',
            
            # Mathematical functions
            'ABS', 'CEIL', 'CEILING', 'FLOOR', 'ROUND', 'TRUNC', 'TRUNCATE',
            'POWER', 'POW', 'SQRT', 'EXP', 'LN', 'LOG', 'LOG10',
            'SIN', 'COS', 'TAN', 'ASIN', 'ACOS', 'ATAN', 'ATAN2',
            'DEGREES', 'RADIANS', 'PI', 'RAND', 'RANDOM', 'SIGN',
            'MOD', 'GREATEST', 'LEAST',
            
            # Date/Time functions
            'NOW', 'CURRENT_DATE', 'CURRENT_TIME', 'CURRENT_TIMESTAMP',
            'TODAY', 'YESTERDAY', 'TOMORROW', 'DATE', 'TIME', 'DATETIME',
            'EXTRACT', 'DATE_PART', 'DATE_TRUNC', 'DATE_ADD', 'DATE_SUB',
            'DATEDIFF', 'DATEADD', 'YEAR', 'MONTH', 'DAY', 'HOUR',
            'MINUTE', 'SECOND', 'MICROSECOND', 'DAYOFWEEK', 'DAYOFYEAR',
            'WEEK', 'WEEKDAY', 'QUARTER', 'LAST_DAY', 'NEXT_DAY',
            'AGE', 'TIMEZONE', 'TO_TIMESTAMP', 'FROM_UNIXTIME',
            
            # Type conversion functions
            'CAST', 'CONVERT', 'TRY_CAST', 'TRY_CONVERT', 'SAFE_CAST',
            'TO_CHAR', 'TO_NUMBER', 'TO_DATE', 'PARSE_DATE', 'PARSE_DATETIME',
            
            # Conditional functions
            'COALESCE', 'ISNULL', 'IFNULL', 'NULLIF', 'NVL', 'NVL2',
            'DECODE', 'CHOOSE', 'IIF',
            
            # Window functions
            'ROW_NUMBER', 'RANK', 'DENSE_RANK', 'PERCENT_RANK', 'CUME_DIST',
            'NTILE', 'LAG', 'LEAD', 'FIRST_VALUE', 'LAST_VALUE', 'NTH_VALUE',
            
            # JSON functions
            'JSON_EXTRACT', 'JSON_UNQUOTE', 'JSON_ARRAY', 'JSON_OBJECT',
            'JSON_KEYS', 'JSON_LENGTH', 'JSON_VALID', 'JSON_TYPE',
            
            # Array functions
            'ARRAY_LENGTH', 'ARRAY_POSITION', 'ARRAY_REMOVE', 'ARRAY_REPLACE',
            'ARRAY_APPEND', 'ARRAY_PREPEND', 'ARRAY_CONTAINS', 'UNNEST',
            
            # System functions
            'VERSION', 'USER', 'CURRENT_USER', 'SESSION_USER', 'SYSTEM_USER',
            'DATABASE', 'SCHEMA', 'CONNECTION_ID', 'LAST_INSERT_ID',
            'ROW_COUNT', 'FOUND_ROWS'
        ]
        
        # Combine all completions
        all_completions = self.sql_keywords + self.sql_functions
        
        # Create model
        self.model = QStringListModel(all_completions)
        self.setModel(self.model)
        
        # Store for dynamic updates
        self.table_names = []
        self.column_names = []
        
    def update_table_names(self, table_names):
        """Update the list of available table names"""
        self.table_names = table_names
        self.refresh_completions()
        
    def update_column_names(self, column_names):
        """Update the list of available column names"""
        self.column_names = column_names
        self.refresh_completions()
        
    def refresh_completions(self):
        """Refresh the completion model with current keywords, functions, tables, and columns"""
        all_completions = (self.sql_keywords + self.sql_functions + 
                         self.table_names + self.column_names)
        
        # Remove duplicates while preserving order
        seen = set()
        unique_completions = []
        for item in all_completions:
            if item.upper() not in seen:
                unique_completions.append(item)
                seen.add(item.upper())
                
        self.model.setStringList(unique_completions)

# Enhanced SQL Text Editor with auto-completion
class SQLTextEdit(QTextEdit):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.completer = SQLCompleter(self)
        self.completer.setWidget(self)
        self.completer.activated.connect(self.insert_completion)
        
        # Set up the text editor
        self.setFont(QFont("Consolas", 11))
        self.setLineWrapMode(QTextEdit.LineWrapMode.NoWrap)
        self.setTabStopDistance(40)  # 4 spaces for tab
        
        # Connect to text change for auto-completion
        self.textChanged.connect(self.on_text_changed)
        
        # Track cursor position for better completion
        self.cursorPositionChanged.connect(self.on_cursor_changed)
        
    def insert_completion(self, completion):
        """Insert the selected completion into the text"""
        cursor = self.textCursor()
        
        # Find the start of the current word
        current_word = self.get_current_word()
        if current_word:
            # Replace the current word with the completion
            cursor.movePosition(QTextCursor.MoveOperation.Left, 
                              QTextCursor.MoveMode.KeepAnchor, len(current_word))
            cursor.removeSelectedText()
            
        cursor.insertText(completion)
        self.setTextCursor(cursor)
        
    def get_current_word(self):
        """Get the word currently being typed"""
        cursor = self.textCursor()
        cursor.select(QTextCursor.SelectionType.WordUnderCursor)
        return cursor.selectedText()
        
    def on_text_changed(self):
        """Handle text changes for auto-completion"""
        current_word = self.get_current_word()
        
        # Only show completions if we have at least 2 characters
        if len(current_word) >= 2:
            self.completer.setCompletionPrefix(current_word)
            
            # Position the completion popup
            cursor_rect = self.cursorRect()
            cursor_rect.setWidth(self.completer.popup().sizeHintForColumn(0) + 
                               self.completer.popup().verticalScrollBar().sizeHint().width())
            self.completer.complete(cursor_rect)
        else:
            self.completer.popup().hide()
            
    def on_cursor_changed(self):
        """Handle cursor position changes"""
        # Hide completion popup if cursor moves away from the word being completed
        if not self.get_current_word():
            self.completer.popup().hide()
            
    def keyPressEvent(self, event):
        """Handle key press events"""
        # Handle special keys for completion
        if self.completer.popup().isVisible():
            if event.key() in (Qt.Key.Key_Enter, Qt.Key.Key_Return, Qt.Key.Key_Tab):
                event.ignore()
                return
            elif event.key() == Qt.Key.Key_Escape:
                self.completer.popup().hide()
                return
                
        # Auto-indentation for new lines
        if event.key() in (Qt.Key.Key_Enter, Qt.Key.Key_Return):
            cursor = self.textCursor()
            cursor.insertText('\n')
            
            # Get the indentation of the current line
            cursor.movePosition(QTextCursor.MoveOperation.Up)
            cursor.movePosition(QTextCursor.MoveOperation.StartOfLine)
            cursor.movePosition(QTextCursor.MoveOperation.EndOfLine, 
                              QTextCursor.MoveMode.KeepAnchor)
            line_text = cursor.selectedText()
            
            # Calculate indentation
            indent = ''
            for char in line_text:
                if char in ' \t':
                    indent += char
                else:
                    break
                    
            # Insert the same indentation on the new line
            cursor.movePosition(QTextCursor.MoveOperation.Down)
            cursor.insertText(indent)
            
            self.setTextCursor(cursor)
            return
            
        # Auto-completion for parentheses, quotes, etc.
        if event.text() == '(':
            cursor = self.textCursor()
            cursor.insertText('()')
            cursor.movePosition(QTextCursor.MoveOperation.Left)
            self.setTextCursor(cursor)
            return
        elif event.text() == '[':
            cursor = self.textCursor()
            cursor.insertText('[]')
            cursor.movePosition(QTextCursor.MoveOperation.Left)
            self.setTextCursor(cursor)
            return
        elif event.text() in ['"', "'"]:
            cursor = self.textCursor()
            quote = event.text()
            cursor.insertText(quote + quote)
            cursor.movePosition(QTextCursor.MoveOperation.Left)
            self.setTextCursor(cursor)
            return
            
        super().keyPressEvent(event)
        
    def update_completions(self, table_names=None, column_names=None):
        """Update the completer with new table and column names"""
        if table_names is not None:
            self.completer.update_table_names(table_names)
        if column_names is not None:
            self.completer.update_column_names(column_names)

# SQL Syntax Highlighter
class SQLHighlighter(QSyntaxHighlighter):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.highlighting_rules = []
        self.setup_highlighting_rules()

    def setup_highlighting_rules(self):
        # Clear existing rules
        self.highlighting_rules = []

        # SQL Keywords (Primary commands) - Most important, put first
        keyword_format = QTextCharFormat()
        keyword_format.setForeground(QColor(198, 120, 221))  # Purple - explicit color
        keyword_format.setFontWeight(QFont.Weight.Bold)
        
        sql_keywords = [
            'SELECT', 'FROM', 'WHERE', 'INSERT', 'UPDATE', 'DELETE', 'CREATE', 'ALTER', 'DROP',
            'TRUNCATE', 'BEGIN', 'COMMIT', 'ROLLBACK', 'DISTINCT', 'AS', 'INTO', 'VALUES', 'SET',
            'GROUP BY', 'ORDER BY', 'HAVING', 'LIMIT', 'OFFSET', 'UNION', 'UNION ALL',
            'INTERSECT', 'EXCEPT', 'WITH', 'RECURSIVE'
        ]
        
        for keyword in sql_keywords:
            # Use word boundaries to match whole words only
            pattern = f"\\b{keyword}\\b"
            regex = QRegularExpression(pattern, QRegularExpression.CaseInsensitiveOption)
            self.highlighting_rules.append((regex, keyword_format))

        # SQL Operators and Logic
        operator_format = QTextCharFormat()
        operator_format.setForeground(QColor(86, 182, 194))  # Cyan - explicit color
        operator_format.setFontWeight(QFont.Weight.Bold)
        
        operators = [
            'AND', 'OR', 'NOT', 'IN', 'LIKE', 'BETWEEN', 'EXISTS', 'IS', 'NULL',
            'IS NULL', 'IS NOT NULL', 'ALL', 'ANY', 'SOME'
        ]
        
        for operator in operators:
            pattern = f"\\b{operator}\\b"
            regex = QRegularExpression(pattern, QRegularExpression.CaseInsensitiveOption)
            self.highlighting_rules.append((regex, operator_format))

        # Operator symbols
        operator_symbols = ['=', '!=', '<>', '<', '>', '<=', '>=', '\\+', '-', '\\*', '/', '%']
        for symbol in operator_symbols:
            regex = QRegularExpression(symbol)
            self.highlighting_rules.append((regex, operator_format))

        # SQL Functions
        function_format = QTextCharFormat()
        function_format.setForeground(QColor(97, 175, 239))  # Blue - explicit color
        function_format.setFontWeight(QFont.Weight.Bold)
        
        functions = [
            'COUNT', 'SUM', 'AVG', 'MAX', 'MIN', 'GROUP_CONCAT', 'COALESCE', 'NULLIF',
            'CAST', 'CONVERT', 'SUBSTRING', 'SUBSTR', 'LENGTH', 'UPPER', 'LOWER',
            'TRIM', 'LTRIM', 'RTRIM', 'REPLACE', 'NOW', 'CURRENT_DATE', 'CURRENT_TIME'
        ]
        
        for function in functions:
            pattern = f"\\b{function}\\b"
            regex = QRegularExpression(pattern, QRegularExpression.CaseInsensitiveOption)
            self.highlighting_rules.append((regex, function_format))

        # JOIN keywords
        join_format = QTextCharFormat()
        join_format.setForeground(QColor(75, 160, 240))  # Accent blue - explicit color
        join_format.setFontWeight(QFont.Weight.Bold)
        
        joins = ['JOIN', 'INNER JOIN', 'LEFT JOIN', 'RIGHT JOIN', 'FULL JOIN', 'CROSS JOIN', 'ON', 'USING']
        for join in joins:
            pattern = f"\\b{join}\\b"
            regex = QRegularExpression(pattern, QRegularExpression.CaseInsensitiveOption)
            self.highlighting_rules.append((regex, join_format))

        # Data Types
        datatype_format = QTextCharFormat()
        datatype_format.setForeground(QColor(209, 154, 102))  # Tan - explicit color
        datatype_format.setFontWeight(QFont.Weight.Bold)
        
        datatypes = [
            'INTEGER', 'INT', 'BIGINT', 'SMALLINT', 'TINYINT', 'VARCHAR', 'CHAR', 'TEXT',
            'REAL', 'FLOAT', 'DOUBLE', 'NUMERIC', 'DECIMAL', 'DATE', 'TIME', 'TIMESTAMP',
            'DATETIME', 'BOOLEAN', 'BOOL', 'BLOB', 'BINARY'
        ]
        
        for datatype in datatypes:
            pattern = f"\\b{datatype}\\b"
            regex = QRegularExpression(pattern, QRegularExpression.CaseInsensitiveOption)
            self.highlighting_rules.append((regex, datatype_format))

        # String literals (quoted strings)
        string_format = QTextCharFormat()
        string_format.setForeground(QColor(152, 195, 121))  # Green - explicit color
        string_format.setFontItalic(True)
        
        # Single quoted strings
        self.highlighting_rules.append((QRegularExpression("'[^']*'"), string_format))
        # Double quoted strings
        self.highlighting_rules.append((QRegularExpression('"[^"]*"'), string_format))

        # Numbers
        number_format = QTextCharFormat()
        number_format.setForeground(QColor(229, 192, 123))  # Orange - explicit color
        number_format.setFontWeight(QFont.Weight.Bold)
        
        # Decimal numbers
        self.highlighting_rules.append((QRegularExpression("\\b\\d+\\.\\d+\\b"), number_format))
        # Integer numbers
        self.highlighting_rules.append((QRegularExpression("\\b\\d+\\b"), number_format))

        # Comments
        comment_format = QTextCharFormat()
        comment_format.setForeground(QColor(128, 128, 128))  # Gray - explicit color
        comment_format.setFontItalic(True)
        
        # Single line comments
        self.highlighting_rules.append((QRegularExpression("--[^\n]*"), comment_format))
        
        # Multi-line comment setup
        self.multiline_comment_format = QTextCharFormat()
        self.multiline_comment_format.setForeground(QColor(128, 128, 128))  # Gray - explicit color
        self.multiline_comment_format.setFontItalic(True)
        self.comment_start_expression = QRegularExpression("/\\*")
        self.comment_end_expression = QRegularExpression("\\*/")

    def highlightBlock(self, text):
        # Debug: Print what we're highlighting (comment out in production)
        # print(f"Highlighting text: '{text}'")
        
        # Apply single-line highlighting rules
        for pattern, format_obj in self.highlighting_rules:
            match_iterator = pattern.globalMatch(text)
            while match_iterator.hasNext():
                match = match_iterator.next()
                start = match.capturedStart()
                length = match.capturedLength()
                matched_text = text[start:start+length]
                # Debug: Print matches (comment out in production)
                # print(f"  Matched: '{matched_text}' at {start}-{start+length}")
                self.setFormat(start, length, format_obj)

        # Handle multi-line comments
        self.setCurrentBlockState(0)
        start_index = 0
        if self.previousBlockState() != 1:
            match = self.comment_start_expression.match(text)
            start_index = match.capturedStart() if match.hasMatch() else -1

        while start_index >= 0:
            match_end = self.comment_end_expression.match(text, start_index)
            end_index = match_end.capturedStart() if match_end.hasMatch() else -1
            comment_length = 0
            
            if end_index == -1:
                self.setCurrentBlockState(1)
                comment_length = len(text) - start_index
            else:
                comment_length = end_index - start_index + match_end.capturedLength()
            
            self.setFormat(start_index, comment_length, self.multiline_comment_format)
            
            if end_index != -1:
                next_match = self.comment_start_expression.match(text, start_index + comment_length)
                start_index = next_match.capturedStart() if next_match.hasMatch() else -1
            else:
                start_index = -1

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

class ImportWorker(QThread):
    """Worker thread for non-blocking data imports"""
    progress = pyqtSignal(int, str)  # progress percentage, status message
    finished = pyqtSignal(bool, str)  # success, message
    error = pyqtSignal(str)
    
    def __init__(self, main_app, import_info, is_folder=False):
        super().__init__()
        self.main_app = main_app
        self.import_info = import_info
        self.is_folder = is_folder
    
    def run(self):
        try:
            if self.is_folder:
                self.progress.emit(10, "Starting folder import...")
                success = self.main_app.import_folder_data(self.import_info)
            else:
                self.progress.emit(10, "Starting file import...")
                success = self.main_app.import_data(self.import_info)
            
            if success:
                self.progress.emit(100, "Import completed successfully!")
                self.finished.emit(True, "Data imported successfully!")
            else:
                self.finished.emit(False, "Import failed. Check console for details.")
                
        except Exception as e:
            self.error.emit(f"Import error: {str(e)}")

class ProgressDialog(QDialog):
    """Progress dialog for long-running operations"""
    
    def __init__(self, parent=None, title="Processing..."):
        super().__init__(parent)
        self.setWindowTitle(title)
        self.setModal(True)
        self.setFixedSize(400, 150)
        self.setWindowFlags(Qt.WindowType.Dialog | Qt.WindowType.CustomizeWindowHint | Qt.WindowType.WindowTitleHint)
        
        layout = QVBoxLayout(self)
        layout.setSpacing(15)
        layout.setContentsMargins(20, 20, 20, 20)
        
        # Status label
        self.status_label = QLabel("Initializing...")
        self.status_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        layout.addWidget(self.status_label)
        
        # Progress bar
        self.progress_bar = QProgressBar()
        self.progress_bar.setRange(0, 100)
        self.progress_bar.setValue(0)
        layout.addWidget(self.progress_bar)
        
        # Cancel button
        self.cancel_button = QPushButton("Cancel")
        self.cancel_button.clicked.connect(self.reject)
        self.cancel_button.setStyleSheet("""
            QPushButton {
                background-color: #dc3545;
                color: white;
                border: none;
                padding: 8px 16px;
                border-radius: 4px;
                font-weight: bold;
            }
            QPushButton:hover { background-color: #c82333; }
        """)
        
        button_layout = QHBoxLayout()
        button_layout.addStretch()
        button_layout.addWidget(self.cancel_button)
        layout.addLayout(button_layout)
    
    def update_progress(self, value, message):
        """Update progress bar and status message"""
        self.progress_bar.setValue(value)
        self.status_label.setText(message)
        
        # Auto-close when complete
        if value >= 100:
            QTimer.singleShot(1000, self.accept)  # Close after 1 second

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
        self.resize(850, 750)  # Made larger and resizable
        self.setMinimumSize(700, 600)  # Set minimum size
        self.init_ui()
    
    def init_ui(self):
        # Create main layout
        main_layout = QVBoxLayout(self)
        main_layout.setContentsMargins(10, 10, 10, 10)
        
        # Create scroll area
        scroll_area = QScrollArea()
        scroll_area.setWidgetResizable(True)
        scroll_area.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAsNeeded)
        scroll_area.setVerticalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAsNeeded)
        
        # Create content widget for scroll area
        content_widget = QWidget()
        layout = QVBoxLayout(content_widget)
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
        
        delimiter_layout = QHBoxLayout()
        self.delimiter_edit = QLineEdit(",")
        self.delimiter_edit.setMaximumWidth(80)
        
        self.auto_detect_button = QPushButton("🔍 Auto")
        self.auto_detect_button.setMaximumWidth(60)
        self.auto_detect_button.setToolTip("Auto-detect delimiter from file")
        self.auto_detect_button.clicked.connect(self.auto_detect_delimiter)
        self.auto_detect_button.setStyleSheet("""
            QPushButton {
                background-color: #17a2b8;
                color: white;
                border: none;
                padding: 4px 8px;
                border-radius: 3px;
                font-size: 10px;
                font-weight: bold;
            }
            QPushButton:hover { background-color: #138496; }
        """)
        
        delimiter_layout.addWidget(self.delimiter_edit)
        delimiter_layout.addWidget(self.auto_detect_button)
        delimiter_layout.addStretch()
        
        csv_options_layout.addRow("Delimiter:", delimiter_layout)
        
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
        
        # Add content widget to scroll area and scroll area to main layout
        scroll_area.setWidget(content_widget)
        main_layout.addWidget(scroll_area)
    
    def auto_detect_delimiter(self):
        """Auto-detect delimiter for the selected file"""
        file_path = self.file_path_edit.text()
        if not file_path or not os.path.exists(file_path):
            QMessageBox.warning(self, "No File", "Please select a file first.")
            return
        
        file_ext = os.path.splitext(file_path)[1].lower()
        if file_ext not in ['.csv', '.txt', '.tsv']:
            QMessageBox.information(self, "Not Applicable", "Auto-detection only works for CSV, TSV, and TXT files.")
            return
        
        # Get the main app instance to use its detect_csv_delimiter method
        main_app = self.parent()
        while main_app and not hasattr(main_app, 'detect_csv_delimiter'):
            main_app = main_app.parent()
        
        if main_app and hasattr(main_app, 'detect_csv_delimiter'):
            detected_delimiter = main_app.detect_csv_delimiter(file_path)
            
            # Convert tab to visible representation
            if detected_delimiter == '\t':
                display_delimiter = '\\t'
            else:
                display_delimiter = detected_delimiter
            
            self.delimiter_edit.setText(display_delimiter)
            
            # Show confirmation
            delimiter_name = {
                ',': 'comma',
                ';': 'semicolon', 
                '\t': 'tab',
                '|': 'pipe'
            }.get(detected_delimiter, f"'{detected_delimiter}'")
            
            QMessageBox.information(self, "Delimiter Detected", 
                                  f"Detected delimiter: {delimiter_name} ({display_delimiter})")
        else:
            QMessageBox.warning(self, "Error", "Could not access delimiter detection functionality.")
    
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
                    
                    # Auto-detect delimiter for CSV/TSV/TXT files
                    main_app = self.parent()
                    while main_app and not hasattr(main_app, 'detect_csv_delimiter'):
                        main_app = main_app.parent()
                    
                    if main_app and hasattr(main_app, 'detect_csv_delimiter'):
                        detected_delimiter = main_app.detect_csv_delimiter(file_path)
                        # Convert tab to visible representation
                        if detected_delimiter == '\t':
                            self.delimiter_edit.setText('\\t')
                        else:
                            self.delimiter_edit.setText(detected_delimiter)
                    elif file_ext == '.tsv':
                        self.delimiter_edit.setText('\\t')
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


class FolderImportDialog(QDialog):
    """Dialog for importing multiple files from a folder"""
    
    def __init__(self, parent=None, connection=None, connection_info=None):
        super().__init__(parent)
        self.connection = connection
        self.connection_info = connection_info
        self.setWindowTitle("Import Folder")
        self.setModal(True)
        self.resize(950, 1100)  # Made even larger
        self.setMinimumSize(800, 900)  # Set larger minimum size
        
        # Add window flags to allow maximize, minimize, and close buttons
        self.setWindowFlags(Qt.WindowType.Dialog | Qt.WindowType.WindowMaximizeButtonHint | Qt.WindowType.WindowMinimizeButtonHint | Qt.WindowType.WindowCloseButtonHint)
        

        self.found_files = []
        self.init_ui()
    
    def init_ui(self):
        # Create main layout for the dialog
        main_layout = QVBoxLayout(self)
        main_layout.setContentsMargins(5, 5, 5, 5)
        
        # Create scroll area
        scroll_area = QScrollArea()
        scroll_area.setWidgetResizable(True)
        scroll_area.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAsNeeded)
        scroll_area.setVerticalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAsNeeded)
        
        # Create content widget for the scroll area
        content_widget = QWidget()
        layout = QVBoxLayout(content_widget)
        layout.setSpacing(15)
        layout.setContentsMargins(20, 20, 20, 20)
        
        # Title
        title_label = QLabel("📁 Import All Files from Folder")
        title_label.setStyleSheet("font-size: 14pt; font-weight: bold; color: #0078d4; margin-bottom: 10px;")
        layout.addWidget(title_label)
        
        # Folder selection
        folder_group = QGroupBox("Select Folder")
        folder_layout = QVBoxLayout(folder_group)
        
        folder_path_layout = QHBoxLayout()
        self.folder_path_edit = QLineEdit()
        self.folder_path_edit.setPlaceholderText("Select a folder containing data files...")
        self.folder_path_edit.setReadOnly(True)
        
        self.browse_folder_button = QPushButton("📁 Browse Folder")
        self.browse_folder_button.clicked.connect(self.browse_folder)
        self.browse_folder_button.setStyleSheet("""
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
        
        folder_path_layout.addWidget(self.folder_path_edit)
        folder_path_layout.addWidget(self.browse_folder_button)
        folder_layout.addLayout(folder_path_layout)
        
        # Folder info
        self.folder_info_label = QLabel("No folder selected")
        self.folder_info_label.setStyleSheet("color: #666; font-style: italic;")
        folder_layout.addWidget(self.folder_info_label)
        
        layout.addWidget(folder_group)
        
        # File type filter
        filter_group = QGroupBox("File Type Filter")
        filter_layout = QVBoxLayout(filter_group)
        
        # Checkboxes for file types
        self.file_type_checkboxes = {}
        file_types = [
            ("csv", "CSV Files (*.csv)", False),
            ("xlsx", "Excel Files (*.xlsx, *.xls)", False),
            ("json", "JSON Files (*.json)", False),
            ("parquet", "Parquet Files (*.parquet)", False),
            ("tsv", "TSV Files (*.tsv)", False),
            ("txt", "Text Files (*.txt)", False)
        ]
        
        checkbox_layout = QGridLayout()
        for i, (ext, label, checked) in enumerate(file_types):
            checkbox = QCheckBox(label)
            checkbox.setChecked(checked)
            checkbox.stateChanged.connect(self.update_file_scan)
            self.file_type_checkboxes[ext] = checkbox
            checkbox_layout.addWidget(checkbox, i // 2, i % 2)
        
        filter_layout.addLayout(checkbox_layout)
        layout.addWidget(filter_group)
        
        # Found files display
        files_group = QGroupBox("Found Files")
        files_layout = QVBoxLayout(files_group)
        
        self.files_list = QListWidget()
        self.files_list.setMaximumHeight(100)
        files_layout.addWidget(self.files_list)
        
        self.files_info_label = QLabel("No files found")
        self.files_info_label.setStyleSheet("color: #666; font-style: italic;")
        files_layout.addWidget(self.files_info_label)
        
        layout.addWidget(files_group)
        
        # Import options
        options_group = QGroupBox("Import Options")
        options_layout = QVBoxLayout(options_group)
        
        # Table name input
        table_name_layout = QHBoxLayout()
        table_name_layout.addWidget(QLabel("Table Name:"))
        self.table_name_edit = QLineEdit()
        self.table_name_edit.setPlaceholderText("Enter table name for combined data...")
        table_name_layout.addWidget(self.table_name_edit)
        options_layout.addLayout(table_name_layout)
        
        # Import mode
        mode_label = QLabel("Import Mode:")
        mode_label.setStyleSheet("font-weight: bold; margin-top: 10px;")
        options_layout.addWidget(mode_label)
        
        self.create_new_radio = QRadioButton("🆕 Create new table")
        self.create_new_radio.setChecked(True)
        self.create_new_radio.setToolTip("Create a new table with combined data from all files")
        
        self.append_radio = QRadioButton("➕ Append to existing table")
        self.append_radio.setToolTip("Add combined data to existing table")
        
        self.replace_radio = QRadioButton("🔄 Replace existing table")
        self.replace_radio.setToolTip("Replace existing table with combined data")
        
        options_layout.addWidget(self.create_new_radio)
        options_layout.addWidget(self.append_radio)
        options_layout.addWidget(self.replace_radio)
        
        # Table selection dropdown (for append/replace modes)
        self.table_select_widget = QWidget()
        table_select_layout = QHBoxLayout(self.table_select_widget)
        table_select_layout.setContentsMargins(0, 5, 0, 0)
        table_select_layout.addWidget(QLabel("Select Table:"))
        self.table_select_combo = QComboBox()
        table_select_layout.addWidget(self.table_select_combo)
        self.table_select_widget.hide()  # Initially hidden
        options_layout.addWidget(self.table_select_widget)
        
        # Advanced options
        advanced_label = QLabel("Advanced Options:")
        advanced_label.setStyleSheet("font-weight: bold; margin-top: 10px;")
        options_layout.addWidget(advanced_label)
        
        self.add_filename_column = QCheckBox("Add filename column")
        self.add_filename_column.setChecked(True)
        self.add_filename_column.setToolTip("Add a column with the source filename for each row")
        options_layout.addWidget(self.add_filename_column)
        
        self.recursive_scan = QCheckBox("Include subfolders")
        self.recursive_scan.setChecked(False)
        self.recursive_scan.setToolTip("Scan subfolders for files")
        self.recursive_scan.stateChanged.connect(self.update_file_scan)
        options_layout.addWidget(self.recursive_scan)
        
        layout.addWidget(options_group)
        
        # File-specific options
        file_config_group = QGroupBox("File-Specific Options")
        file_config_layout = QVBoxLayout(file_config_group)
        
        # CSV/TSV/TXT Options
        self.csv_options_widget = QWidget()
        csv_options_layout = QFormLayout(self.csv_options_widget)
        
        delimiter_layout = QHBoxLayout()
        self.delimiter_edit = QLineEdit()
        self.delimiter_edit.setText(",")
        self.delimiter_edit.setPlaceholderText("e.g., , or ; or |")
        self.delimiter_edit.setMaximumWidth(80)
        
        self.auto_detect_folder_button = QPushButton("🔍 Auto")
        self.auto_detect_folder_button.setMaximumWidth(60)
        self.auto_detect_folder_button.setToolTip("Auto-detect delimiter from CSV files in folder")
        self.auto_detect_folder_button.clicked.connect(self.auto_detect_folder_delimiter)
        self.auto_detect_folder_button.setStyleSheet("""
            QPushButton {
                background-color: #17a2b8;
                color: white;
                border: none;
                padding: 4px 8px;
                border-radius: 3px;
                font-size: 10px;
                font-weight: bold;
            }
            QPushButton:hover { background-color: #138496; }
        """)
        
        delimiter_layout.addWidget(self.delimiter_edit)
        delimiter_layout.addWidget(self.auto_detect_folder_button)
        delimiter_layout.addStretch()
        
        csv_options_layout.addRow("Delimiter:", delimiter_layout)
        
        self.encoding_combo = QComboBox()
        self.encoding_combo.addItems(["utf-8", "latin-1", "cp1252", "utf-16"])
        csv_options_layout.addRow("Encoding:", self.encoding_combo)
        
        self.header_checkbox = QCheckBox("First row contains headers")
        self.header_checkbox.setChecked(True)
        csv_options_layout.addRow("", self.header_checkbox)
        
        file_config_layout.addWidget(QLabel("CSV/TSV/TXT Files:"))
        file_config_layout.addWidget(self.csv_options_widget)
        
        # Excel Options
        self.excel_options_widget = QWidget()
        excel_options_layout = QFormLayout(self.excel_options_widget)
        
        self.sheet_behavior_combo = QComboBox()
        self.sheet_behavior_combo.addItems([
            "Import all sheets (combine)", 
            "Import first sheet only", 
            "Use specific sheet name for all files",
            "Ask for each file"
        ])
        self.sheet_behavior_combo.setCurrentIndex(1)  # Default to first sheet
        self.sheet_behavior_combo.currentTextChanged.connect(self.on_excel_mode_changed)
        excel_options_layout.addRow("Sheet Handling:", self.sheet_behavior_combo)
        
        # Default sheet name selection (initially hidden)
        self.sheet_name_label = QLabel("Default Sheet Name:")
        
        # Create a horizontal layout for the sheet selection
        sheet_selection_widget = QWidget()
        sheet_selection_layout = QHBoxLayout(sheet_selection_widget)
        sheet_selection_layout.setContentsMargins(0, 0, 0, 0)
        
        self.sheet_name_combo = QComboBox()
        self.sheet_name_combo.setEditable(True)  # Allow custom input
        self.sheet_name_combo.setToolTip("Select a sheet name from the first Excel file, or type a custom name")
        
        self.refresh_sheets_button = QPushButton("🔄")
        self.refresh_sheets_button.setMaximumWidth(30)
        self.refresh_sheets_button.setToolTip("Refresh sheet list from first Excel file")
        self.refresh_sheets_button.clicked.connect(self.refresh_excel_sheets)
        
        sheet_selection_layout.addWidget(self.sheet_name_combo)
        sheet_selection_layout.addWidget(self.refresh_sheets_button)
        
        # Initially hide both label and input
        self.sheet_name_label.hide()
        sheet_selection_widget.hide()
        self.sheet_selection_widget = sheet_selection_widget  # Store reference
        
        excel_options_layout.addRow(self.sheet_name_label, sheet_selection_widget)
        
        file_config_layout.addWidget(QLabel("Excel Files:"))
        file_config_layout.addWidget(self.excel_options_widget)
        
        # JSON Options
        self.json_options_widget = QWidget()
        json_options_layout = QFormLayout(self.json_options_widget)
        
        self.json_normalize_checkbox = QCheckBox("Normalize nested JSON")
        self.json_normalize_checkbox.setChecked(True)
        self.json_normalize_checkbox.setToolTip("Flatten nested JSON objects into columns")
        json_options_layout.addRow("", self.json_normalize_checkbox)
        
        file_config_layout.addWidget(QLabel("JSON Files:"))
        file_config_layout.addWidget(self.json_options_widget)
        
        layout.addWidget(file_config_group)
        
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
        
        self.import_button = QPushButton("📥 Import Folder")
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
        
        # Set the content widget to the scroll area
        scroll_area.setWidget(content_widget)
        
        # Add scroll area to main layout
        main_layout.addWidget(scroll_area)
        
        # Connect signals
        self.folder_path_edit.textChanged.connect(self.update_ui)
        self.table_name_edit.textChanged.connect(self.update_ui)
        self.table_select_combo.currentTextChanged.connect(self.update_ui)
        
        # Connect radio button signals
        self.create_new_radio.toggled.connect(self.update_table_selection_ui)
        self.append_radio.toggled.connect(self.update_table_selection_ui)
        self.replace_radio.toggled.connect(self.update_table_selection_ui)
        
        # Load existing tables
        self.load_existing_tables()
    
    def auto_detect_folder_delimiter(self):
        """Auto-detect delimiter from CSV files in the selected folder"""
        if not self.found_files:
            QMessageBox.warning(self, "No Files", "Please select a folder and scan for files first.")
            return
        
        # Find CSV/TSV/TXT files in the found files
        csv_files = [f for f in self.found_files if f.lower().endswith(('.csv', '.txt', '.tsv'))]
        
        if not csv_files:
            QMessageBox.information(self, "No CSV Files", "No CSV, TSV, or TXT files found in the selected folder.")
            return
        
        # Get the main app instance to use its detect_csv_delimiter method
        main_app = self.parent()
        while main_app and not hasattr(main_app, 'detect_csv_delimiter'):
            main_app = main_app.parent()
        
        if not main_app or not hasattr(main_app, 'detect_csv_delimiter'):
            QMessageBox.warning(self, "Error", "Could not access delimiter detection functionality.")
            return
        
        # Try to detect delimiter from the first few CSV files
        delimiter_counts = {}
        files_checked = 0
        
        for file_path in csv_files[:5]:  # Check first 5 CSV files
            try:
                detected_delimiter = main_app.detect_csv_delimiter(file_path)
                delimiter_counts[detected_delimiter] = delimiter_counts.get(detected_delimiter, 0) + 1
                files_checked += 1
            except Exception as e:
                print(f"Error detecting delimiter for {file_path}: {e}")
                continue
        
        if not delimiter_counts:
            QMessageBox.warning(self, "Detection Failed", "Could not detect delimiter from any CSV files.")
            return
        
        # Choose the most common delimiter
        best_delimiter = max(delimiter_counts, key=delimiter_counts.get)
        
        # Convert tab to visible representation
        if best_delimiter == '\t':
            display_delimiter = '\\t'
        else:
            display_delimiter = best_delimiter
        
        self.delimiter_edit.setText(display_delimiter)
        
        # Show confirmation
        delimiter_name = {
            ',': 'comma',
            ';': 'semicolon', 
            '\t': 'tab',
            '|': 'pipe'
        }.get(best_delimiter, f"'{best_delimiter}'")
        
        QMessageBox.information(self, "Delimiter Detected", 
                              f"Detected delimiter: {delimiter_name} ({display_delimiter})\n"
                              f"Based on {files_checked} CSV file(s)")
    
    def on_excel_mode_changed(self):
        """Show/hide the default sheet name input based on Excel mode selection"""
        current_mode = self.sheet_behavior_combo.currentText()
        
        if "Use specific sheet name" in current_mode:
            # Show the default sheet name selection and label
            self.sheet_name_label.show()
            self.sheet_selection_widget.show()
            # Auto-refresh sheets when first shown
            self.refresh_excel_sheets()
        else:
            # Hide the default sheet name selection and label
            self.sheet_name_label.hide()
            self.sheet_selection_widget.hide()
    
    def refresh_excel_sheets(self):
        """Refresh the sheet names dropdown from the first Excel file found"""
        try:
            # Clear existing items
            self.sheet_name_combo.clear()
            
            # Find first Excel file in the current file list
            first_excel_file = None
            for file_path in self.found_files:
                if file_path.lower().endswith(('.xlsx', '.xls')):
                    first_excel_file = file_path
                    break
            
            if not first_excel_file:
                # No Excel files found, add default options
                self.sheet_name_combo.addItems(['Sheet1', 'Data', 'Sheet'])
                self.sheet_name_combo.setCurrentText('Sheet1')
                return
            
            # Try to read sheet names from the first Excel file
            try:
                import pandas as pd
                excel_file = pd.ExcelFile(first_excel_file)
                sheet_names = excel_file.sheet_names
                
                if sheet_names:
                    self.sheet_name_combo.addItems(sheet_names)
                    self.sheet_name_combo.setCurrentText(sheet_names[0])  # Select first sheet by default
                    
                    # Update tooltip with file info
                    file_name = os.path.basename(first_excel_file)
                    self.sheet_name_combo.setToolTip(f"Sheet names from: {file_name}\nSelect a sheet or type a custom name")
                else:
                    # File has no sheets, add defaults
                    self.sheet_name_combo.addItems(['Sheet1', 'Data', 'Sheet'])
                    self.sheet_name_combo.setCurrentText('Sheet1')
                    
            except Exception as e:
                # Error reading Excel file, add default options
                self.sheet_name_combo.addItems(['Sheet1', 'Data', 'Sheet'])
                self.sheet_name_combo.setCurrentText('Sheet1')
                print(f"Could not read sheets from {first_excel_file}: {e}")
                
        except Exception as e:
            # General error, add default options
            self.sheet_name_combo.addItems(['Sheet1', 'Data', 'Sheet'])
            self.sheet_name_combo.setCurrentText('Sheet1')
            print(f"Error refreshing Excel sheets: {e}")
    
    def browse_folder(self):
        folder_path = QFileDialog.getExistingDirectory(
            self,
            "Select Folder Containing Data Files",
            "",
            QFileDialog.Option.ShowDirsOnly
        )
        
        if folder_path:
            self.folder_path_edit.setText(folder_path)
            self.analyze_folder(folder_path)
    
    def analyze_folder(self, folder_path):
        try:
            # Auto-suggest table name only if field is empty
            if not self.table_name_edit.text().strip():
                folder_name = os.path.basename(folder_path)
                # Clean table name (remove special characters, replace with underscores)
                clean_name = re.sub(r'[^a-zA-Z0-9_]', '_', folder_name).lower()
                if clean_name:
                    self.table_name_edit.setText(clean_name)
                else:
                    self.table_name_edit.setText("folder_import")
            
            self.update_file_scan()
            
        except Exception as e:
            self.folder_info_label.setText(f"Error analyzing folder: {str(e)}")
    
    def update_file_scan(self):
        folder_path = self.folder_path_edit.text()
        if not folder_path or not os.path.exists(folder_path):
            self.found_files = []
            self.files_list.clear()
            self.folder_info_label.setText("No folder selected")
            self.files_info_label.setText("No files found")
            return
        
        try:
            # Get selected file types
            selected_extensions = []
            for ext, checkbox in self.file_type_checkboxes.items():
                if checkbox.isChecked():
                    if ext == "xlsx":
                        selected_extensions.extend([".xlsx", ".xls"])
                    else:
                        selected_extensions.append(f".{ext}")
            
            # Auto-update delimiter for TSV files
            if self.file_type_checkboxes.get("tsv", QCheckBox()).isChecked():
                if self.delimiter_edit.text() in [",", ""]:
                    self.delimiter_edit.setText("\\t")
            
            # Scan folder for files
            self.found_files = []
            if self.recursive_scan.isChecked():
                # Recursive scan
                for root, dirs, files in os.walk(folder_path):
                    for file in files:
                        if any(file.lower().endswith(ext) for ext in selected_extensions):
                            self.found_files.append(os.path.join(root, file))
            else:
                # Non-recursive scan
                for file in os.listdir(folder_path):
                    file_path = os.path.join(folder_path, file)
                    if os.path.isfile(file_path) and any(file.lower().endswith(ext) for ext in selected_extensions):
                        self.found_files.append(file_path)
            
            # Update UI
            self.files_list.clear()
            total_size = 0
            for file_path in self.found_files:
                try:
                    file_size = os.path.getsize(file_path)
                    total_size += file_size
                    file_info = f"{os.path.basename(file_path)} ({file_size / 1024 / 1024:.2f} MB)"
                    self.files_list.addItem(file_info)
                except:
                    self.files_list.addItem(os.path.basename(file_path))
            
            # Update info labels
            if self.found_files:
                self.folder_info_label.setText(f"Folder: {os.path.basename(folder_path)} | Found {len(self.found_files)} files | Total size: {total_size / 1024 / 1024:.2f} MB")
                self.files_info_label.setText(f"{len(self.found_files)} files found")
                
                # Auto-refresh Excel sheets if the specific sheet option is selected
                if "Use specific sheet name" in self.sheet_behavior_combo.currentText():
                    self.refresh_excel_sheets()
            else:
                self.folder_info_label.setText(f"Folder: {os.path.basename(folder_path)} | No supported files found")
                self.files_info_label.setText("No files found with selected file types")
            
        except Exception as e:
            self.folder_info_label.setText(f"Error scanning folder: {str(e)}")
            self.files_info_label.setText("Error scanning files")
    
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
            self.table_select_widget.hide()
        else:  # append or replace mode
            self.table_select_widget.show()
        self.update_ui()
    
    def update_ui(self):
        # Enable import button only if folder, files, and appropriate table selection are provided
        has_folder = bool(self.folder_path_edit.text().strip())
        has_files = len(self.found_files) > 0
        
        # Check if any file types are selected
        has_file_types = any(checkbox.isChecked() for checkbox in self.file_type_checkboxes.values())
        
        if self.create_new_radio.isChecked():
            has_table_info = bool(self.table_name_edit.text().strip())
        else:  # append or replace mode
            current_text = self.table_select_combo.currentText()
            has_table_info = (current_text and 
                            current_text != "(No tables found)" and
                            not current_text.startswith("Error") and
                            current_text.startswith("📊 "))
        
        # Check if Excel files are selected and if default sheet name is required
        has_excel_files = self.file_type_checkboxes.get("xlsx", QCheckBox()).isChecked()
        excel_sheet_behavior = self.sheet_behavior_combo.currentText()
        needs_sheet_name = has_excel_files and "Use specific sheet name" in excel_sheet_behavior
        has_sheet_name = bool(self.sheet_name_combo.currentText().strip()) if needs_sheet_name else True
        
        # Update button state and tooltip
        can_import = has_folder and has_files and has_table_info and has_file_types and has_sheet_name
        self.import_button.setEnabled(can_import)
        
        # Update button tooltip with helpful information
        if not has_folder:
            self.import_button.setToolTip("Please select a folder first")
        elif not has_file_types:
            self.import_button.setToolTip("Please select at least one file type to import")
        elif not has_files:
            self.import_button.setToolTip("No files found with selected types in the folder")
        elif not has_table_info:
            if self.create_new_radio.isChecked():
                self.import_button.setToolTip("Please enter a table name")
            else:
                self.import_button.setToolTip("Please select an existing table")
        elif not has_sheet_name:
            self.import_button.setToolTip("Please enter a default sheet name for Excel files")
        else:
            self.import_button.setToolTip(f"Ready to import {len(self.found_files)} files")
    
    def get_import_info(self):
        # Get table name based on mode
        if self.create_new_radio.isChecked():
            table_name = self.table_name_edit.text().strip()
        else:  # append or replace mode
            table_name = self.table_select_combo.currentText()
            # Remove icon prefix if present
            if table_name.startswith("📊 "):
                table_name = table_name[2:]  # Remove "📊 " prefix
        
        import_info = {
            'folder_path': self.folder_path_edit.text(),
            'file_paths': self.found_files.copy(),
            'table_name': table_name,
            'mode': 'create' if self.create_new_radio.isChecked() else 
                   'append' if self.append_radio.isChecked() else 'replace',
            'add_filename_column': self.add_filename_column.isChecked(),
            'recursive_scan': self.recursive_scan.isChecked(),
            # File-specific options
            'csv_delimiter': self.delimiter_edit.text() or ',',
            'csv_encoding': self.encoding_combo.currentText(),
            'csv_header': self.header_checkbox.isChecked(),
            'excel_sheet_behavior': self.sheet_behavior_combo.currentText(),
            'excel_default_sheet': self.sheet_name_combo.currentText().strip(),
            'json_normalize': self.json_normalize_checkbox.isChecked()
        }
        
        return import_info


# Tab widget for query editors
class QueryTab(QWidget):
    schema_changed = pyqtSignal()  # Signal to notify when schema might have changed
    
    def __init__(self, parent=None, connection=None, connection_info=None):
        super().__init__(parent)
        self.connection = connection
        self.connection_info = connection_info
        self.query_worker = None
        self.table_names = []
        self.column_names = []
        
        # Fullscreen state
        self.is_fullscreen = False
        self.normal_geometry = None
        self.main_window = None
        
        # Layout
        self.layout = QVBoxLayout(self)
        self.layout.setContentsMargins(0, 0, 0, 0)
        
        # Splitter for editor and results
        self.splitter = QSplitter(Qt.Orientation.Vertical)
        
        # Query editor with auto-completion
        self.editor = SQLTextEdit()
        
        # Set editor colors first
        editor_palette = self.editor.palette()
        editor_palette.setColor(QPalette.ColorRole.Base, ColorScheme.BACKGROUND)
        editor_palette.setColor(QPalette.ColorRole.Text, ColorScheme.TEXT)
        self.editor.setPalette(editor_palette)
        
        # Apply syntax highlighting after setting up the editor
        self.highlighter = SQLHighlighter(self.editor.document())
        
        # Force a refresh of syntax highlighting
        QTimer.singleShot(100, self.highlighter.rehighlight)
        
        # Results area
        self.results_widget = QWidget()
        self.results_layout = QVBoxLayout(self.results_widget)
        self.results_layout.setContentsMargins(0, 0, 0, 0)
        
        # Editor header with fullscreen button
        self.editor_header = QWidget()
        self.editor_header_layout = QHBoxLayout(self.editor_header)
        self.editor_header_layout.setContentsMargins(5, 5, 5, 5)
        
        self.editor_label = QLabel("Query Editor")
        self.editor_label.setFont(QFont("Segoe UI", 10, QFont.Weight.Bold))
        
        self.fullscreen_button = QPushButton("⛶")
        self.fullscreen_button.setMaximumSize(30, 25)
        self.fullscreen_button.setToolTip("Toggle fullscreen editor (F11)")
        self.fullscreen_button.clicked.connect(self.toggle_fullscreen)
        self.fullscreen_button.setStyleSheet("""
            QPushButton {
                background-color: #404040;
                color: #ffffff;
                border: 1px solid #606060;
                border-radius: 3px;
                font-weight: bold;
            }
            QPushButton:hover {
                background-color: #505050;
                border-color: #0078d4;
            }
            QPushButton:pressed {
                background-color: #0078d4;
            }
        """)
        
        self.editor_header_layout.addWidget(self.editor_label)
        self.editor_header_layout.addStretch()
        self.editor_header_layout.addWidget(self.fullscreen_button)
        
        # Results header
        self.results_header = QWidget()
        self.results_header_layout = QHBoxLayout(self.results_header)
        self.results_header_layout.setContentsMargins(5, 5, 5, 5)
        
        self.results_label = QLabel("Results")
        self.results_label.setFont(QFont("Segoe UI", 10, QFont.Weight.Bold))
        self.results_info = QLabel("")
        
        # Export button
        self.export_button = QPushButton("📤 Export")
        self.export_button.setMaximumSize(80, 25)
        self.export_button.setToolTip("Export results to file")
        self.export_button.setEnabled(False)  # Initially disabled
        self.export_button.clicked.connect(self.show_export_menu)
        self.export_button.setStyleSheet("""
            QPushButton {
                background-color: #0078d4;
                color: #ffffff;
                border: 1px solid #106ebe;
                border-radius: 3px;
                font-weight: bold;
                padding: 2px 8px;
            }
            QPushButton:hover {
                background-color: #106ebe;
                border-color: #005a9e;
            }
            QPushButton:pressed {
                background-color: #005a9e;
            }
            QPushButton:disabled {
                background-color: #404040;
                color: #808080;
                border-color: #606060;
            }
        """)
        
        self.results_header_layout.addWidget(self.results_label)
        self.results_header_layout.addWidget(self.results_info, 1)
        self.results_header_layout.addWidget(self.export_button)
        
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
        
        # Create editor container with header
        self.editor_container = QWidget()
        self.editor_container_layout = QVBoxLayout(self.editor_container)
        self.editor_container_layout.setContentsMargins(0, 0, 0, 0)
        self.editor_container_layout.addWidget(self.editor_header)
        self.editor_container_layout.addWidget(self.editor)
        
        # Add widgets to layouts
        self.results_layout.addWidget(self.results_header)
        self.results_layout.addWidget(self.results_table)
        
        self.splitter.addWidget(self.editor_container)
        self.splitter.addWidget(self.results_widget)
        self.splitter.setSizes([200, 300])
        
        self.layout.addWidget(self.splitter)
        
    def execute_query(self):
        if not self.connection:
            QMessageBox.warning(self, "No Connection", "Please connect to a database first.")
            return
        
        # Check if there's a selection first
        cursor = self.editor.textCursor()
        if cursor.hasSelection():
            query = cursor.selectedText().strip()
            execution_type = "selection"
        else:
            query = self.editor.toPlainText().strip()
            execution_type = "full query"
            
        if not query:
            return
            
        # Show what's being executed
        query_preview = query[:100] + "..." if len(query) > 100 else query
        self.results_info.setText(f"Executing {execution_type}: {query_preview}")
        
        # Disable editor during execution
        self.editor.setReadOnly(True)
        
        # Execute query in a separate thread
        self.query_worker = QueryWorker(self.connection, query)
        self.query_worker.finished.connect(self.handle_query_results)
        self.query_worker.error.connect(self.handle_query_error)
        self.query_worker.start()
    
    def execute_selected_query(self):
        """Execute only the selected text as a query"""
        cursor = self.editor.textCursor()
        if cursor.hasSelection():
            self.execute_query()  # Will automatically detect selection
        else:
            QMessageBox.information(self, "No Selection", "Please select some text in the query editor first.")
    
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
        
        # Enable export button if we have results
        self.export_button.setEnabled(row_count > 0)
        
        # Check if this was a DDL statement that might have changed the schema
        query = self.editor.toPlainText().strip().upper()
        ddl_keywords = ['CREATE TABLE', 'DROP TABLE', 'ALTER TABLE', 'CREATE VIEW', 'DROP VIEW', 'CREATE INDEX', 'DROP INDEX']
        if any(keyword in query for keyword in ddl_keywords):
            self.schema_changed.emit()
        
        # Re-enable editor
        self.editor.setReadOnly(False)
    
    def handle_query_error(self, error_message):
        self.results_info.setText(f"Error: {error_message}")
        self.export_button.setEnabled(False)  # Disable export on error
        self.editor.setReadOnly(False)
        
    def update_schema_completions(self, table_names=None, column_names=None):
        """Update the auto-completion with table and column names from the schema"""
        if table_names is not None:
            self.table_names = table_names
        if column_names is not None:
            self.column_names = column_names
            
        # Update the editor's completer
        self.editor.update_completions(self.table_names, self.column_names)
    
    def toggle_fullscreen(self):
        """Toggle fullscreen mode for the query editor"""
        if not self.is_fullscreen:
            # Enter fullscreen mode
            self.enter_fullscreen()
        else:
            # Exit fullscreen mode
            self.exit_fullscreen()
    
    def enter_fullscreen(self):
        """Enter fullscreen mode"""
        try:
            # Find the main window
            self.main_window = self.window()
            
            # Create fullscreen window
            self.fullscreen_window = QWidget()
            self.fullscreen_window.setWindowTitle("SQL Editor - Fullscreen")
            self.fullscreen_window.setWindowFlags(Qt.WindowType.Window)
            
            # Set up fullscreen layout
            fullscreen_layout = QVBoxLayout(self.fullscreen_window)
            fullscreen_layout.setContentsMargins(10, 10, 10, 10)
            
            # Create fullscreen header
            header_widget = QWidget()
            header_layout = QHBoxLayout(header_widget)
            header_layout.setContentsMargins(0, 0, 0, 10)
            
            title_label = QLabel("SQL Editor - Fullscreen Mode")
            title_label.setFont(QFont("Segoe UI", 14, QFont.Weight.Bold))
            title_label.setStyleSheet(f"color: {ColorScheme.TEXT.name()};")
            
            exit_button = QPushButton("✕ Exit Fullscreen")
            exit_button.clicked.connect(self.exit_fullscreen)
            exit_button.setStyleSheet("""
                QPushButton {
                    background-color: #d32f2f;
                    color: white;
                    border: none;
                    padding: 8px 16px;
                    border-radius: 4px;
                    font-weight: bold;
                }
                QPushButton:hover { background-color: #b71c1c; }
            """)
            
            header_layout.addWidget(title_label)
            header_layout.addStretch()
            header_layout.addWidget(exit_button)
            
            # Move editor to fullscreen window
            self.editor.setParent(None)
            
            fullscreen_layout.addWidget(header_widget)
            fullscreen_layout.addWidget(self.editor)
            
            # Set dark theme for fullscreen window
            self.fullscreen_window.setStyleSheet(f"""
                QWidget {{
                    background-color: {ColorScheme.BACKGROUND.name()};
                    color: {ColorScheme.TEXT.name()};
                }}
            """)
            
            # Show fullscreen
            self.fullscreen_window.showFullScreen()
            self.editor.setFocus()
            
            # Update state
            self.is_fullscreen = True
            self.fullscreen_button.setText("⛶")
            self.fullscreen_button.setToolTip("Exit fullscreen editor (F11 or Esc)")
            
            # Install event filter for escape key
            self.fullscreen_window.installEventFilter(self)
            
        except Exception as e:
            print(f"Error entering fullscreen: {e}")
    
    def exit_fullscreen(self):
        """Exit fullscreen mode"""
        try:
            if hasattr(self, 'fullscreen_window') and self.fullscreen_window:
                # Move editor back to original container
                self.editor.setParent(None)
                self.editor_container_layout.addWidget(self.editor)
                
                # Close fullscreen window
                self.fullscreen_window.close()
                self.fullscreen_window = None
                
                # Update state
                self.is_fullscreen = False
                self.fullscreen_button.setText("⛶")
                self.fullscreen_button.setToolTip("Toggle fullscreen editor (F11)")
                
                # Focus back to editor
                self.editor.setFocus()
                
        except Exception as e:
            print(f"Error exiting fullscreen: {e}")
    
    def eventFilter(self, obj, event):
        """Handle events for fullscreen mode"""
        if (obj == self.fullscreen_window and 
            event.type() == event.Type.KeyPress):
            if event.key() == Qt.Key.Key_Escape:
                self.exit_fullscreen()
                return True
            elif event.key() == Qt.Key.Key_F11:
                self.exit_fullscreen()
                return True
        return super().eventFilter(obj, event)
    
    def keyPressEvent(self, event):
        """Handle key press events"""
        if event.key() == Qt.Key.Key_F11:
            self.toggle_fullscreen()
        else:
            super().keyPressEvent(event)
    
    def show_export_menu(self):
        """Show export options menu"""
        if not hasattr(self, 'model') or not self.model or self.model.rowCount() == 0:
            QMessageBox.information(self, "No Data", "No results to export.")
            return
        
        menu = QMenu(self)
        menu.setStyleSheet("""
            QMenu {
                background-color: #404040;
                color: #ffffff;
                border: 1px solid #606060;
                border-radius: 4px;
                padding: 4px;
            }
            QMenu::item {
                padding: 8px 16px;
                border-radius: 3px;
            }
            QMenu::item:selected {
                background-color: #0078d4;
            }
            QMenu::separator {
                height: 1px;
                background-color: #606060;
                margin: 4px 8px;
            }
        """)
        
        # Add export format actions
        csv_action = menu.addAction("📄 Export as CSV")
        csv_action.triggered.connect(lambda: self.export_data('csv'))
        
        excel_action = menu.addAction("📊 Export as Excel")
        excel_action.triggered.connect(lambda: self.export_data('excel'))
        
        json_action = menu.addAction("🔗 Export as JSON")
        json_action.triggered.connect(lambda: self.export_data('json'))
        
        parquet_action = menu.addAction("📦 Export as Parquet")
        parquet_action.triggered.connect(lambda: self.export_data('parquet'))
        
        menu.addSeparator()
        
        tsv_action = menu.addAction("📋 Export as TSV")
        tsv_action.triggered.connect(lambda: self.export_data('tsv'))
        
        html_action = menu.addAction("🌐 Export as HTML")
        html_action.triggered.connect(lambda: self.export_data('html'))
        
        xml_action = menu.addAction("📰 Export as XML")
        xml_action.triggered.connect(lambda: self.export_data('xml'))
        
        menu.addSeparator()
        
        clipboard_action = menu.addAction("📋 Copy to Clipboard")
        clipboard_action.triggered.connect(lambda: self.export_data('clipboard'))
        
        # Show menu at button position
        button_pos = self.export_button.mapToGlobal(self.export_button.rect().bottomLeft())
        menu.exec(button_pos)
    
    def export_data(self, format_type):
        """Export results data in the specified format"""
        try:
            if not hasattr(self, 'model') or not self.model or self.model.rowCount() == 0:
                QMessageBox.information(self, "No Data", "No results to export.")
                return
            
            # Get the dataframe from the model
            df = self.model._data.copy()
            
            if format_type == 'clipboard':
                # Copy to clipboard
                df.to_clipboard(index=False, sep='\t')
                QMessageBox.information(self, "Export Successful", 
                                      f"Results copied to clipboard!\n\n"
                                      f"📊 {len(df):,} rows × {len(df.columns)} columns")
                return
            
            # File export - get save location
            file_filters = {
                'csv': "CSV Files (*.csv)",
                'excel': "Excel Files (*.xlsx)",
                'json': "JSON Files (*.json)",
                'parquet': "Parquet Files (*.parquet)",
                'tsv': "TSV Files (*.tsv)",
                'html': "HTML Files (*.html)",
                'xml': "XML Files (*.xml)"
            }
            
            file_extensions = {
                'csv': '.csv',
                'excel': '.xlsx',
                'json': '.json',
                'parquet': '.parquet',
                'tsv': '.tsv',
                'html': '.html',
                'xml': '.xml'
            }
            
            default_name = f"query_results{file_extensions[format_type]}"
            
            file_path, _ = QFileDialog.getSaveFileName(
                self,
                f"Export Results as {format_type.upper()}",
                default_name,
                file_filters[format_type]
            )
            
            if not file_path:
                return  # User cancelled
            
            # Export based on format
            if format_type == 'csv':
                df.to_csv(file_path, index=False, encoding='utf-8')
                
            elif format_type == 'excel':
                if openpyxl is None:
                    QMessageBox.warning(self, "Excel Export Unavailable", 
                                      "Excel export requires the 'openpyxl' package.\n"
                                      "Please install it with: pip install openpyxl")
                    return
                
                with pd.ExcelWriter(file_path, engine='openpyxl') as writer:
                    df.to_excel(writer, sheet_name='Query Results', index=False)
                    
                    # Auto-adjust column widths
                    worksheet = writer.sheets['Query Results']
                    for column in worksheet.columns:
                        max_length = 0
                        column_letter = column[0].column_letter
                        for cell in column:
                            try:
                                if len(str(cell.value)) > max_length:
                                    max_length = len(str(cell.value))
                            except:
                                pass
                        adjusted_width = min(max_length + 2, 50)
                        worksheet.column_dimensions[column_letter].width = adjusted_width
                
            elif format_type == 'json':
                df.to_json(file_path, orient='records', indent=2, date_format='iso')
                
            elif format_type == 'parquet':
                df.to_parquet(file_path, index=False)
                
            elif format_type == 'tsv':
                df.to_csv(file_path, index=False, sep='\t', encoding='utf-8')
                
            elif format_type == 'html':
                html_content = f"""
<!DOCTYPE html>
<html>
<head>
    <title>Query Results</title>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 20px; }}
        table {{ border-collapse: collapse; width: 100%; }}
        th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
        th {{ background-color: #f2f2f2; font-weight: bold; }}
        tr:nth-child(even) {{ background-color: #f9f9f9; }}
        .info {{ margin-bottom: 20px; color: #666; }}
    </style>
</head>
<body>
    <h1>Query Results</h1>
    <div class="info">
        <p>Exported on: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
        <p>Rows: {len(df):,} | Columns: {len(df.columns)}</p>
    </div>
    {df.to_html(index=False, escape=False, classes='results-table')}
</body>
</html>
                """
                with open(file_path, 'w', encoding='utf-8') as f:
                    f.write(html_content)
                
            elif format_type == 'xml':
                xml_content = '<?xml version="1.0" encoding="UTF-8"?>\n'
                xml_content += '<query_results>\n'
                xml_content += f'  <metadata>\n'
                xml_content += f'    <export_date>{pd.Timestamp.now().isoformat()}</export_date>\n'
                xml_content += f'    <row_count>{len(df)}</row_count>\n'
                xml_content += f'    <column_count>{len(df.columns)}</column_count>\n'
                xml_content += f'  </metadata>\n'
                xml_content += '  <data>\n'
                
                for _, row in df.iterrows():
                    xml_content += '    <row>\n'
                    for col in df.columns:
                        # Clean column name for XML
                        clean_col = str(col).replace(' ', '_').replace('-', '_')
                        clean_col = ''.join(c for c in clean_col if c.isalnum() or c == '_')
                        value = str(row[col]) if pd.notna(row[col]) else ''
                        # Escape XML special characters
                        value = value.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')
                        xml_content += f'      <{clean_col}>{value}</{clean_col}>\n'
                    xml_content += '    </row>\n'
                
                xml_content += '  </data>\n'
                xml_content += '</query_results>'
                
                with open(file_path, 'w', encoding='utf-8') as f:
                    f.write(xml_content)
            
            # Show success message
            file_size = os.path.getsize(file_path) / 1024  # Size in KB
            size_text = f"{file_size:.1f} KB" if file_size < 1024 else f"{file_size/1024:.1f} MB"
            
            QMessageBox.information(self, "Export Successful", 
                                  f"Results exported successfully!\n\n"
                                  f"📁 File: {os.path.basename(file_path)}\n"
                                  f"📊 Data: {len(df):,} rows × {len(df.columns)} columns\n"
                                  f"💾 Size: {size_text}\n"
                                  f"📍 Location: {file_path}")
            
        except Exception as e:
            QMessageBox.critical(self, "Export Error", 
                               f"Failed to export results:\n\n{str(e)}")
            print(f"Export error: {e}")

# Database schema browser
class SchemaBrowser(QTreeWidget):
    # Signal to notify when schema data is updated (table_names, column_names)
    schema_data_updated = pyqtSignal(list, list)
    # Signal to notify when schema structure changes (table/column deleted)
    schema_changed = pyqtSignal()
    
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setHeaderLabels(["Database Objects"])
        self.setColumnCount(1)
        self.setAlternatingRowColors(True)
        self.setAnimated(True)
        
        # Store schema data for auto-completion
        self.table_names = []
        self.column_names = []
        
        # Store connection references for context menu operations
        self.connection = None
        self.connection_info = None
        
        # Enable context menu
        self.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
        self.customContextMenuRequested.connect(self.show_context_menu)
        
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
        
        # Store connection references for context menu operations
        self.connection = connection
        self.connection_info = connection_info
        
        db_type = connection_info["type"].lower()
        if db_type in ["sqlite", "sqlite3"]:
            self.load_sqlite_schema(connection)
        elif db_type == "duckdb":
            self.load_duckdb_schema(connection)
        else:
            print(f"Unknown database type: {connection_info['type']}")
    
    def load_sqlite_schema(self, connection):
        # Clear existing schema data
        self.table_names = []
        self.column_names = []
        
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
                self.table_names.append(table_name)  # Collect table name for auto-completion
                
                table_item = QTreeWidgetItem(tables_item, [table_name])
                table_item.setIcon(0, self.table_icon)
                # Store metadata for context menu
                table_item.setData(0, Qt.ItemDataRole.UserRole, {'type': 'table', 'name': table_name})
                
                # Get columns for this table
                cursor.execute(f"PRAGMA table_info({table_name})")
                columns = cursor.fetchall()
                
                for column in columns:
                    col_name = column[1]
                    col_type = column[2]
                    is_pk = column[5] == 1  # Primary key flag
                    
                    # Collect column name for auto-completion (avoid duplicates)
                    if col_name not in self.column_names:
                        self.column_names.append(col_name)
                    
                    column_text = f"{col_name} ({col_type})"
                    column_item = QTreeWidgetItem(table_item, [column_text])
                    column_item.setIcon(0, self.pk_icon if is_pk else self.column_icon)
                    # Store metadata for context menu
                    column_item.setData(0, Qt.ItemDataRole.UserRole, {'type': 'column', 'name': col_name, 'table': table_name, 'is_pk': is_pk})
        
        # Views group
        cursor.execute("SELECT name FROM sqlite_master WHERE type='view' ORDER BY name")
        views = cursor.fetchall()
        
        if views:
            views_item = QTreeWidgetItem(db_item, ["Views"])
            views_item.setIcon(0, qta.icon('fa5s.folder', color=ColorScheme.ACCENT))
            
            for view in views:
                view_name = view[0]
                self.table_names.append(view_name)  # Views can also be queried like tables
                view_item = QTreeWidgetItem(views_item, [view_name])
                view_item.setIcon(0, self.view_icon)
                # Store metadata for context menu
                view_item.setData(0, Qt.ItemDataRole.UserRole, {'type': 'view', 'name': view_name})
        
        # Emit signal with collected schema data
        self.schema_data_updated.emit(self.table_names, self.column_names)
        
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
        # Clear existing schema data
        self.table_names = []
        self.column_names = []
        
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
                self.table_names.append(table_name)  # Collect table name for auto-completion
                
                table_item = QTreeWidgetItem(tables_item, [table_name])
                table_item.setIcon(0, self.table_icon)
                # Store metadata for context menu
                table_item.setData(0, Qt.ItemDataRole.UserRole, {'type': 'table', 'name': table_name})
                
                # Get columns for this table
                try:
                    columns_df = connection.execute(f"PRAGMA table_info('{table_name}')").fetchdf()
                    
                    for _, col_row in columns_df.iterrows():
                        col_name = col_row['name']
                        col_type = col_row['type']
                        is_pk = col_row['pk'] == 1  # Primary key flag
                        
                        # Collect column name for auto-completion (avoid duplicates)
                        if col_name not in self.column_names:
                            self.column_names.append(col_name)
                        
                        column_text = f"{col_name} ({col_type})"
                        column_item = QTreeWidgetItem(table_item, [column_text])
                        column_item.setIcon(0, self.pk_icon if is_pk else self.column_icon)
                        # Store metadata for context menu
                        column_item.setData(0, Qt.ItemDataRole.UserRole, {'type': 'column', 'name': col_name, 'table': table_name, 'is_pk': is_pk})
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
                    self.table_names.append(view_name)  # Views can also be queried like tables
                    view_item = QTreeWidgetItem(views_item, [view_name])
                    view_item.setIcon(0, self.view_icon)
                    # Store metadata for context menu
                    view_item.setData(0, Qt.ItemDataRole.UserRole, {'type': 'view', 'name': view_name})
        except:
            # Some versions of DuckDB might not have this view
            pass
        
        # Emit signal with collected schema data
        self.schema_data_updated.emit(self.table_names, self.column_names)
        
        # Expand the database item
        db_item.setExpanded(True)
        tables_item.setExpanded(True)
    
    def show_context_menu(self, position):
        """Show context menu for the schema browser"""
        item = self.itemAt(position)
        if not item:
            return
        
        # Get item metadata
        item_data = item.data(0, Qt.ItemDataRole.UserRole)
        if not item_data:
            return
        
        # Create context menu
        menu = QMenu(self)
        
        # Add actions based on item type
        if item_data['type'] == 'table':
            delete_action = QAction(qta.icon('fa5s.trash', color=ColorScheme.ERROR), f"Delete Table '{item_data['name']}'", self)
            delete_action.triggered.connect(lambda: self.delete_table(item_data['name']))
            menu.addAction(delete_action)
            
        elif item_data['type'] == 'view':
            delete_action = QAction(qta.icon('fa5s.trash', color=ColorScheme.ERROR), f"Delete View '{item_data['name']}'", self)
            delete_action.triggered.connect(lambda: self.delete_view(item_data['name']))
            menu.addAction(delete_action)
            
        elif item_data['type'] == 'column':
            if not item_data.get('is_pk', False):  # Don't allow deleting primary keys
                delete_action = QAction(qta.icon('fa5s.trash', color=ColorScheme.ERROR), f"Delete Column '{item_data['name']}'", self)
                delete_action.triggered.connect(lambda: self.delete_column(item_data['table'], item_data['name']))
                menu.addAction(delete_action)
            else:
                info_action = QAction(qta.icon('fa5s.info-circle', color=ColorScheme.WARNING), "Cannot delete primary key column", self)
                info_action.setEnabled(False)
                menu.addAction(info_action)
        
        # Show the menu
        if menu.actions():
            menu.exec(self.mapToGlobal(position))
    
    def delete_table(self, table_name):
        """Delete a table with confirmation"""
        if not self.connection or not self.connection_info:
            QMessageBox.warning(self, "Error", "No database connection available.")
            return
        
        # Confirmation dialog
        reply = QMessageBox.question(
            self, 
            "Confirm Delete", 
            f"Are you sure you want to delete the table '{table_name}'?\n\nThis action cannot be undone and will permanently delete all data in the table.",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
            QMessageBox.StandardButton.No
        )
        
        if reply == QMessageBox.StandardButton.Yes:
            try:
                # Execute DROP TABLE with multiple fallback strategies
                success = False
                error_messages = []
                
                # Strategy 1: Try with double quotes
                try:
                    if self.connection_info['type'].lower() in ['sqlite', 'sqlite3']:
                        cursor = self.connection.cursor()
                        cursor.execute(f'DROP TABLE IF EXISTS "{table_name}"')
                        self.connection.commit()
                    else:  # DuckDB
                        self.connection.execute(f'DROP TABLE IF EXISTS "{table_name}"')
                    success = True
                except Exception as e1:
                    error_messages.append(f"Double quotes: {str(e1)}")
                
                # Strategy 2: Try without quotes if first attempt failed
                if not success:
                    try:
                        if self.connection_info['type'].lower() in ['sqlite', 'sqlite3']:
                            cursor = self.connection.cursor()
                            cursor.execute(f'DROP TABLE IF EXISTS {table_name}')
                            self.connection.commit()
                        else:  # DuckDB
                            self.connection.execute(f'DROP TABLE IF EXISTS {table_name}')
                        success = True
                    except Exception as e2:
                        error_messages.append(f"No quotes: {str(e2)}")
                
                # Strategy 3: Try with square brackets (for some SQL dialects)
                if not success:
                    try:
                        if self.connection_info['type'].lower() in ['sqlite', 'sqlite3']:
                            cursor = self.connection.cursor()
                            cursor.execute(f'DROP TABLE IF EXISTS [{table_name}]')
                            self.connection.commit()
                        else:  # DuckDB
                            self.connection.execute(f'DROP TABLE IF EXISTS [{table_name}]')
                        success = True
                    except Exception as e3:
                        error_messages.append(f"Square brackets: {str(e3)}")
                
                if success:
                    # Refresh schema browser
                    self.load_schema(self.connection, self.connection_info)
                    
                    # Emit schema changed signal
                    self.schema_changed.emit()
                    
                    # Show success message
                    QMessageBox.information(self, "Success", f"Table '{table_name}' has been deleted successfully.")
                else:
                    # All strategies failed
                    error_detail = "\n".join(error_messages)
                    QMessageBox.critical(self, "Error", f"Failed to delete table '{table_name}' using all strategies:\n\n{error_detail}")
                
            except Exception as e:
                QMessageBox.critical(self, "Error", f"Unexpected error while deleting table '{table_name}':\n{str(e)}")
    
    def delete_view(self, view_name):
        """Delete a view with confirmation"""
        if not self.connection or not self.connection_info:
            QMessageBox.warning(self, "Error", "No database connection available.")
            return
        
        # Confirmation dialog
        reply = QMessageBox.question(
            self, 
            "Confirm Delete", 
            f"Are you sure you want to delete the view '{view_name}'?\n\nThis action cannot be undone.",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
            QMessageBox.StandardButton.No
        )
        
        if reply == QMessageBox.StandardButton.Yes:
            try:
                # Execute DROP VIEW with multiple fallback strategies
                success = False
                error_messages = []
                
                # Strategy 1: Try with double quotes
                try:
                    if self.connection_info['type'].lower() in ['sqlite', 'sqlite3']:
                        cursor = self.connection.cursor()
                        cursor.execute(f'DROP VIEW IF EXISTS "{view_name}"')
                        self.connection.commit()
                    else:  # DuckDB
                        self.connection.execute(f'DROP VIEW IF EXISTS "{view_name}"')
                    success = True
                except Exception as e1:
                    error_messages.append(f"Double quotes: {str(e1)}")
                
                # Strategy 2: Try without quotes if first attempt failed
                if not success:
                    try:
                        if self.connection_info['type'].lower() in ['sqlite', 'sqlite3']:
                            cursor = self.connection.cursor()
                            cursor.execute(f'DROP VIEW IF EXISTS {view_name}')
                            self.connection.commit()
                        else:  # DuckDB
                            self.connection.execute(f'DROP VIEW IF EXISTS {view_name}')
                        success = True
                    except Exception as e2:
                        error_messages.append(f"No quotes: {str(e2)}")
                
                # Strategy 3: Try with square brackets
                if not success:
                    try:
                        if self.connection_info['type'].lower() in ['sqlite', 'sqlite3']:
                            cursor = self.connection.cursor()
                            cursor.execute(f'DROP VIEW IF EXISTS [{view_name}]')
                            self.connection.commit()
                        else:  # DuckDB
                            self.connection.execute(f'DROP VIEW IF EXISTS [{view_name}]')
                        success = True
                    except Exception as e3:
                        error_messages.append(f"Square brackets: {str(e3)}")
                
                if success:
                    # Refresh schema browser
                    self.load_schema(self.connection, self.connection_info)
                    
                    # Emit schema changed signal
                    self.schema_changed.emit()
                    
                    # Show success message
                    QMessageBox.information(self, "Success", f"View '{view_name}' has been deleted successfully.")
                else:
                    # All strategies failed
                    error_detail = "\n".join(error_messages)
                    QMessageBox.critical(self, "Error", f"Failed to delete view '{view_name}' using all strategies:\n\n{error_detail}")
                
            except Exception as e:
                QMessageBox.critical(self, "Error", f"Unexpected error while deleting view '{view_name}':\n{str(e)}")
    
    def delete_column(self, table_name, column_name):
        """Delete a column with confirmation"""
        if not self.connection or not self.connection_info:
            QMessageBox.warning(self, "Error", "No database connection available.")
            return
        
        # Confirmation dialog
        reply = QMessageBox.question(
            self, 
            "Confirm Delete", 
            f"Are you sure you want to delete the column '{column_name}' from table '{table_name}'?\n\nThis action cannot be undone and will permanently delete all data in this column.",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
            QMessageBox.StandardButton.No
        )
        
        if reply == QMessageBox.StandardButton.Yes:
            try:
                # Note: SQLite has limited ALTER TABLE support, so we need different approaches
                success = False
                error_messages = []
                
                if self.connection_info['type'].lower() in ['sqlite', 'sqlite3']:
                    # For SQLite, we need to recreate the table without the column
                    cursor = self.connection.cursor()
                    
                    # Get table schema with multiple strategies
                    columns = None
                    for quote_style in ['"', '', '`']:
                        try:
                            if quote_style:
                                cursor.execute(f'PRAGMA table_info({quote_style}{table_name}{quote_style})')
                            else:
                                cursor.execute(f'PRAGMA table_info({table_name})')
                            columns = cursor.fetchall()
                            break
                        except:
                            continue
                    
                    if not columns:
                        QMessageBox.critical(self, "Error", f"Could not retrieve table schema for '{table_name}'.")
                        return
                    
                    # Create new column list without the deleted column
                    remaining_columns = [col[1] for col in columns if col[1] != column_name]
                    
                    if len(remaining_columns) == 0:
                        QMessageBox.warning(self, "Error", "Cannot delete the last column of a table.")
                        return
                    
                    # Try multiple quoting strategies for SQLite
                    for quote_style in ['"', '', '`']:
                        try:
                            # Start transaction
                            cursor.execute("BEGIN TRANSACTION")
                            
                            try:
                                # Create temporary table with remaining columns
                                if quote_style:
                                    column_list = ", ".join([f'{quote_style}{col}{quote_style}' for col in remaining_columns])
                                    cursor.execute(f'CREATE TABLE {quote_style}{table_name}_temp{quote_style} AS SELECT {column_list} FROM {quote_style}{table_name}{quote_style}')
                                    
                                    # Drop original table
                                    cursor.execute(f'DROP TABLE {quote_style}{table_name}{quote_style}')
                                    
                                    # Rename temp table to original name
                                    cursor.execute(f'ALTER TABLE {quote_style}{table_name}_temp{quote_style} RENAME TO {quote_style}{table_name}{quote_style}')
                                else:
                                    column_list = ", ".join(remaining_columns)
                                    cursor.execute(f'CREATE TABLE {table_name}_temp AS SELECT {column_list} FROM {table_name}')
                                    
                                    # Drop original table
                                    cursor.execute(f'DROP TABLE {table_name}')
                                    
                                    # Rename temp table to original name
                                    cursor.execute(f'ALTER TABLE {table_name}_temp RENAME TO {table_name}')
                                
                                # Commit transaction
                                cursor.execute("COMMIT")
                                success = True
                                break
                                
                            except Exception as e:
                                # Rollback on error
                                cursor.execute("ROLLBACK")
                                error_messages.append(f"SQLite with {quote_style or 'no'} quotes: {str(e)}")
                                
                        except Exception as e:
                            error_messages.append(f"SQLite transaction with {quote_style or 'no'} quotes: {str(e)}")
                        
                else:  # DuckDB
                    # Try multiple quoting strategies for DuckDB
                    for quote_style in ['"', '', '`']:
                        try:
                            if quote_style:
                                self.connection.execute(f'ALTER TABLE {quote_style}{table_name}{quote_style} DROP COLUMN {quote_style}{column_name}{quote_style}')
                            else:
                                self.connection.execute(f'ALTER TABLE {table_name} DROP COLUMN {column_name}')
                            success = True
                            break
                        except Exception as e:
                            error_messages.append(f"DuckDB with {quote_style or 'no'} quotes: {str(e)}")
                
                if not success:
                    error_detail = "\n".join(error_messages)
                    QMessageBox.critical(self, "Error", f"Failed to delete column '{column_name}' from table '{table_name}' using all strategies:\n\n{error_detail}")
                    return
                
                # Only refresh if successful
                if success:
                    # Refresh schema browser
                    self.load_schema(self.connection, self.connection_info)
                    
                    # Emit schema changed signal
                    self.schema_changed.emit()
                    
                    # Show success message
                    QMessageBox.information(self, "Success", f"Column '{column_name}' has been deleted from table '{table_name}' successfully.")
                
            except Exception as e:
                QMessageBox.critical(self, "Error", f"Unexpected error while deleting column '{column_name}' from table '{table_name}':\n{str(e)}")

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
        
        # Connect schema browser signals
        self.schema_browser.schema_data_updated.connect(self.update_all_tabs_completions)
        self.schema_browser.schema_changed.connect(self.refresh_schema_browser)
        
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
            
    def update_all_tabs_completions(self, table_names, column_names):
        """Update auto-completion for all query tabs when schema changes"""
        for i in range(self.tab_widget.count()):
            tab = self.tab_widget.widget(i)
            if isinstance(tab, QueryTab):
                tab.update_schema_completions(table_names, column_names)
    
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
        self.execute_button.setToolTip("Execute entire query (F5)")
        self.execute_button.clicked.connect(self.execute_current_query)
        self.toolbar.addWidget(self.execute_button)
        
        # Execute Selection button
        self.execute_selection_button = QToolButton()
        self.execute_selection_button.setIcon(qta.icon('fa5s.play-circle', color=ColorScheme.ACCENT))
        self.execute_selection_button.setText("Execute Selection")
        self.execute_selection_button.setToolButtonStyle(Qt.ToolButtonStyle.ToolButtonTextBesideIcon)
        self.execute_selection_button.setToolTip("Execute selected text only (Ctrl+E)")
        self.execute_selection_button.clicked.connect(self.execute_selected_query)
        self.toolbar.addWidget(self.execute_selection_button)
        
        # Export Results button
        self.export_results_button = QToolButton()
        self.export_results_button.setIcon(qta.icon('fa5s.download', color='#2196F3'))
        self.export_results_button.setText("Export Results")
        self.export_results_button.setToolButtonStyle(Qt.ToolButtonStyle.ToolButtonTextBesideIcon)
        self.export_results_button.setToolTip("Export query results (Ctrl+Shift+E)")
        self.export_results_button.clicked.connect(self.export_current_results)
        self.toolbar.addWidget(self.export_results_button)
        
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
        
        # Folder Import button
        self.folder_import_button = QToolButton()
        self.folder_import_button.setIcon(qta.icon('fa5s.folder', color=ColorScheme.ACCENT))
        self.folder_import_button.setText("Import Folder")
        self.folder_import_button.setToolButtonStyle(Qt.ToolButtonStyle.ToolButtonTextBesideIcon)
        self.folder_import_button.setToolTip("Import and combine all files from a folder")
        self.folder_import_button.clicked.connect(self.show_folder_import_dialog)
        self.toolbar.addWidget(self.folder_import_button)
    
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
        
        self.folder_import_action = QAction(qta.icon('fa5s.folder', color=ColorScheme.ACCENT), "Import &Folder...", self)
        self.folder_import_action.setShortcut("Ctrl+Shift+I")
        self.folder_import_action.triggered.connect(self.show_folder_import_dialog)
        
        # Query actions
        self.execute_action = QAction(qta.icon('fa5s.play', color=ColorScheme.SUCCESS), "&Execute Query", self)
        self.execute_action.setShortcut("F5")
        self.execute_action.triggered.connect(self.execute_current_query)
        
        self.execute_selection_action = QAction(qta.icon('fa5s.play-circle', color=ColorScheme.ACCENT), "Execute &Selection", self)
        self.execute_selection_action.setShortcut("Ctrl+E")
        self.execute_selection_action.triggered.connect(self.execute_selected_query)
        
        # Export results action
        self.export_results_action = QAction(qta.icon('fa5s.download', color='#2196F3'), "Export &Results", self)
        self.export_results_action.setShortcut("Ctrl+Shift+E")
        self.export_results_action.triggered.connect(self.export_current_results)
        
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
        self.db_menu.addAction(self.folder_import_action)
        
        # Query menu
        self.query_menu = self.menuBar().addMenu("&Query")
        self.query_menu.addAction(self.execute_action)
        self.query_menu.addAction(self.execute_selection_action)
        self.query_menu.addSeparator()
        self.query_menu.addAction(self.export_results_action)
    
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
            self.start_import_worker(import_info, is_folder=False)
    
    def show_folder_import_dialog(self):
        if not self.current_connection:
            QMessageBox.warning(self, "No Connection", "Please connect to a database first.")
            return
        
        try:
            dialog = FolderImportDialog(self, self.current_connection, self.current_connection_info)
            result = dialog.exec()
            
            if result == QDialog.DialogCode.Accepted:
                folder_import_info = dialog.get_import_info()
                
                # Validate that we have the required information
                if not folder_import_info or not folder_import_info.get('file_paths'):
                    QMessageBox.warning(self, "Import Error", "No files selected for import.")
                    return
                
                # For create mode, still ask for table name confirmation
                if folder_import_info['mode'] == 'create':
                    suggested_name = folder_import_info['table_name'] or "folder_import"
                    table_name = self.show_table_name_dialog(suggested_name, folder_import_info['folder_path'], 'create')
                    if table_name:
                        folder_import_info['table_name'] = table_name
                    else:
                        return  # User cancelled or didn't provide a name
                
                # Import the folder
                self.start_import_worker(folder_import_info, is_folder=True)
            
        except Exception as e:
            QMessageBox.critical(self, "Dialog Error", f"Error opening folder import dialog:\n{str(e)}")
    
    def start_import_worker(self, import_info, is_folder=False):
        """Start the import worker thread with progress dialog"""
        # Create and show progress dialog
        title = "Importing Folder..." if is_folder else "Importing File..."
        self.progress_dialog = ProgressDialog(self, title)
        
        # Create and start worker thread
        self.import_worker = ImportWorker(self, import_info, is_folder)
        
        # Connect worker signals
        self.import_worker.progress.connect(self.progress_dialog.update_progress)
        self.import_worker.finished.connect(self.on_import_finished)
        self.import_worker.error.connect(self.on_import_error)
        
        # Connect progress dialog cancel to worker termination
        self.progress_dialog.rejected.connect(self.cancel_import)
        
        # Start the worker and show progress dialog
        self.import_worker.start()
        self.progress_dialog.exec()
    
    def on_import_finished(self, success, message):
        """Handle import completion"""
        if hasattr(self, 'progress_dialog'):
            self.progress_dialog.accept()
        
        if success:
            QMessageBox.information(self, "Import Successful", message)
            self.refresh_schema_browser()
            self.check_schema_changes()
        else:
            QMessageBox.warning(self, "Import Failed", message)
    
    def on_import_error(self, error_message):
        """Handle import error"""
        if hasattr(self, 'progress_dialog'):
            self.progress_dialog.reject()
        
        QMessageBox.critical(self, "Import Error", f"An error occurred during import:\n{error_message}")
    
    def cancel_import(self):
        """Cancel the running import"""
        if hasattr(self, 'import_worker') and self.import_worker.isRunning():
            self.import_worker.terminate()
            self.import_worker.wait()
            QMessageBox.information(self, "Import Cancelled", "Import operation was cancelled.")
    
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
        """Import data from file to database with bulletproof error handling"""
        file_path = None
        try:
            file_path = import_info['file_path']
            table_name = import_info['table_name']
            file_type = import_info['file_type']
            mode = import_info['mode']
            
            # Show progress
            self.statusBar().showMessage(f"Importing {os.path.basename(file_path)}...")
            
            # Load data based on file type with robust error handling
            df = self.safe_load_data(file_path, file_type, import_info)
            
            if df is None or df.empty:
                QMessageBox.warning(self, "Import Error", "No data found in the file or file could not be read.")
                return
            
            # Report original data info
            original_rows, original_cols = df.shape
            self.statusBar().showMessage(f"Processing {original_rows:,} rows × {original_cols} columns...")
            
            # Handle duplicate columns
            df = self.handle_duplicate_columns(df)
            
            # Clean column names for database compatibility
            df.columns = [self.clean_column_name(col) for col in df.columns]
            
            # Ensure unique table name
            safe_table_name = self.ensure_unique_table_name(table_name, mode)
            
            # Validate data before import
            errors, warnings = self.validate_import_data(df, safe_table_name, mode)
            
            # Show validation results if any
            if errors:
                error_msg = "Cannot proceed with import:\n\n" + "\n".join(f"• {error}" for error in errors)
                QMessageBox.critical(self, "Import Validation Failed", error_msg)
                return
            
            if warnings:
                warning_msg = "Import warnings:\n\n" + "\n".join(f"• {warning}" for warning in warnings)
                warning_msg += "\n\nDo you want to continue with the import?"
                
                reply = QMessageBox.question(
                    self, "Import Warnings", warning_msg,
                    QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
                    QMessageBox.StandardButton.Yes
                )
                
                if reply == QMessageBox.StandardButton.No:
                    return
            
            # Sanitize all data to prevent import errors
            df = self.sanitize_dataframe(df)
            
            # Handle import mode
            if mode == 'replace':
                # Drop table if exists
                try:
                    if self.current_connection_info['type'].lower() == 'duckdb':
                        self.current_connection.execute(f"DROP TABLE IF EXISTS {safe_table_name}")
                    else:  # SQLite
                        self.current_connection.execute(f"DROP TABLE IF EXISTS {safe_table_name}")
                        self.current_connection.commit()
                except:
                    pass  # Table might not exist
            
            # Import data to database with bulletproof error handling
            success = self.safe_import_to_database(df, safe_table_name, mode)
            
            if success:
                # Update schema browser immediately
                self.refresh_schema_browser()
                
                # Show success message
                rows, cols = df.shape
                mode_text = {
                    'create': 'created',
                    'append': 'appended to',
                    'replace': 'replaced'
                }[mode]
                
                # Show success message with any name changes
                success_msg = f"Successfully {mode_text} table '{safe_table_name}'!\n\n"
                success_msg += f"📊 {rows:,} rows × {cols} columns imported\n"
                success_msg += f"📁 Source: {os.path.basename(file_path)}\n\n"
                success_msg += f"✅ All data was automatically converted to compatible formats"
                
                if safe_table_name != table_name:
                    success_msg += f"\n\n⚠️ Table name was changed from '{table_name}' to '{safe_table_name}' to ensure uniqueness."
                
                QMessageBox.information(self, "Import Successful", success_msg)
                
                self.statusBar().showMessage(f"Import completed: {rows:,} rows imported to '{safe_table_name}'", 5000)
                
                # Add a query to the current tab to show the imported data
                current_tab = self.tab_widget.currentWidget()
                if current_tab and not current_tab.editor.toPlainText().strip():
                    current_tab.editor.setPlainText(f"SELECT * FROM {safe_table_name} LIMIT 100;")
            else:
                QMessageBox.warning(self, "Import Warning", "Import completed but some issues were encountered. Data was converted to text format for compatibility.")
            
        except Exception as e:
            error_msg = str(e)
            if file_path:
                file_info = f" (File: {os.path.basename(file_path)})"
            else:
                file_info = ""
            
            # Show user-friendly error messages
            if "permission denied" in error_msg.lower():
                user_msg = f"Cannot access the file{file_info}. Please check if:\n• The file is open in another program\n• You have permission to read the file\n• The file path is correct"
            elif "encoding" in error_msg.lower() or "codec" in error_msg.lower():
                user_msg = f"File encoding issue{file_info}. Try selecting a different encoding (utf-8, latin-1, cp1252)"
            elif "memory" in error_msg.lower():
                user_msg = f"File is too large to import{file_info}. Try importing a smaller file or contact support"
            elif "connection" in error_msg.lower():
                user_msg = "Database connection error. Please reconnect to the database and try again"
            else:
                user_msg = f"Import failed{file_info}.\n\nTechnical details: {error_msg}"
            
            QMessageBox.critical(self, "Import Error", user_msg)
            self.statusBar().showMessage(f"Import failed: {error_msg}", 5000)
    
    def import_folder_data(self, folder_import_info):
        """Import data from multiple files in a folder with column mismatch handling"""
        try:
            file_paths = folder_import_info['file_paths']
            table_name = folder_import_info['table_name']
            mode = folder_import_info['mode']
            add_filename_column = folder_import_info['add_filename_column']
            
            if not file_paths:
                QMessageBox.warning(self, "No Files", "No supported files found in the selected folder.")
                return
            
            # Show progress dialog
            progress_dialog = QProgressDialog("Processing files...", "Cancel", 0, len(file_paths), self)
            progress_dialog.setWindowModality(Qt.WindowModality.WindowModal)
            progress_dialog.setMinimumDuration(0)
            progress_dialog.show()
            
            # Process each file and collect dataframes
            all_dataframes = []
            all_columns = set()
            successful_files = []
            failed_files = []
            
            self.statusBar().showMessage(f"Processing {len(file_paths)} files...")
            
            for i, file_path in enumerate(file_paths):
                if progress_dialog.wasCanceled():
                    self.statusBar().showMessage("Import cancelled by user", 3000)
                    return
                
                progress_dialog.setLabelText(f"Processing {os.path.basename(file_path)}...")
                progress_dialog.setValue(i)
                QApplication.processEvents()  # Allow UI to update
                
                try:
                    # Determine file type and create import info
                    file_ext = os.path.splitext(file_path)[1].lower()
                    
                    # Create file-specific import info
                    file_import_info = {
                        'file_type': file_ext,
                        'encoding': folder_import_info.get('csv_encoding', 'utf-8'),
                        'delimiter': self.get_delimiter_for_file(file_ext, folder_import_info),
                        'header': folder_import_info.get('csv_header', True)
                    }
                    
                    # Handle Excel files specially
                    if file_ext in ['.xlsx', '.xls']:
                        sheet_behavior = folder_import_info.get('excel_sheet_behavior', 'Import first sheet only')
                        if sheet_behavior == 'Import all sheets (combine)':
                            df = self.load_excel_all_sheets(file_path)
                        elif sheet_behavior == 'Ask for each file':
                            df = self.load_excel_with_dialog(file_path)
                        elif 'Use specific sheet name' in sheet_behavior:
                            # Use the default sheet name specified by user
                            default_sheet = folder_import_info.get('excel_default_sheet', '')
                            if default_sheet:
                                file_import_info['sheet_name'] = default_sheet
                            else:
                                file_import_info['sheet_name'] = 0  # Fallback to first sheet
                            df = self.safe_load_data(file_path, file_ext, file_import_info)
                        else:  # Import first sheet only
                            file_import_info['sheet_name'] = 0
                            df = self.safe_load_data(file_path, file_ext, file_import_info)
                    else:
                        # Load file using existing safe_load_data method
                        df = self.safe_load_data(file_path, file_ext, file_import_info)
                    
                    if df is not None and not df.empty:
                        # Handle duplicate columns first
                        df = self.handle_duplicate_columns(df)
                        
                        # Clean column names
                        df.columns = [self.clean_column_name(col) for col in df.columns]
                        
                        # Add filename column if requested (after cleaning column names)
                        if add_filename_column:
                            # Ensure the source file column name is unique
                            source_col_name = '_source_file'
                            counter = 1
                            while source_col_name in df.columns:
                                source_col_name = f'_source_file_{counter}'
                                counter += 1
                            df[source_col_name] = os.path.basename(file_path)
                        
                        # Sanitize the dataframe
                        df = self.sanitize_dataframe(df)
                        
                        # Track all columns across files
                        all_columns.update(df.columns)
                        
                        all_dataframes.append(df)
                        successful_files.append(file_path)
                    else:
                        failed_files.append(f"{os.path.basename(file_path)}: No data found")
                        
                except Exception as e:
                    failed_files.append(f"{os.path.basename(file_path)}: {str(e)}")
                    continue
            
            progress_dialog.setValue(len(file_paths))
            
            if not all_dataframes:
                QMessageBox.warning(self, "Import Failed", "No data could be loaded from any of the files.")
                return
            
            # Create unified dataframe with all columns
            self.statusBar().showMessage("Combining data from all files...")
            
            # Ensure all dataframes have the same columns (fill missing with None)
            all_columns = sorted(list(all_columns))
            
            for df in all_dataframes:
                for col in all_columns:
                    if col not in df.columns:
                        df[col] = None
                # Reorder columns to match
                df = df[all_columns]
            
            # Concatenate all dataframes
            combined_df = pd.concat(all_dataframes, ignore_index=True, sort=False)
            
            # Ensure unique table name
            safe_table_name = self.ensure_unique_table_name(table_name, mode)
            
            # Validate combined data before import
            errors, warnings = self.validate_import_data(combined_df, safe_table_name, mode)
            
            # Show validation results if any
            if errors:
                error_msg = "Cannot proceed with folder import:\n\n" + "\n".join(f"• {error}" for error in errors)
                QMessageBox.critical(self, "Folder Import Validation Failed", error_msg)
                return
            
            if warnings:
                warning_msg = "Folder import warnings:\n\n" + "\n".join(f"• {warning}" for warning in warnings)
                warning_msg += "\n\nDo you want to continue with the import?"
                
                reply = QMessageBox.question(
                    self, "Folder Import Warnings", warning_msg,
                    QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
                    QMessageBox.StandardButton.Yes
                )
                
                if reply == QMessageBox.StandardButton.No:
                    return
            
            # Handle import mode
            if mode == 'replace':
                # Drop table if exists
                try:
                    if self.current_connection_info['type'].lower() == 'duckdb':
                        self.current_connection.execute(f"DROP TABLE IF EXISTS {safe_table_name}")
                    else:  # SQLite
                        self.current_connection.execute(f"DROP TABLE IF EXISTS {safe_table_name}")
                        self.current_connection.commit()
                except:
                    pass  # Table might not exist
            
            # Import combined data to database
            success = self.safe_import_to_database(combined_df, safe_table_name, mode)
            
            # Show results
            if success:
                # Create summary message
                total_rows = len(combined_df)
                total_cols = len(combined_df.columns)
                
                summary_parts = [
                    f"✅ Successfully imported {total_rows:,} rows × {total_cols} columns",
                    f"📊 Table: {safe_table_name}",
                    f"📁 From {len(successful_files)} files"
                ]
                
                if failed_files:
                    summary_parts.append(f"⚠️  {len(failed_files)} files failed")
                
                summary_msg = "\n".join(summary_parts)
                
                # Show table name change if it occurred
                if safe_table_name != table_name:
                    summary_msg += f"\n\n⚠️ Table name was changed from '{table_name}' to '{safe_table_name}' to ensure uniqueness."
                
                if failed_files:
                    failed_details = "\n".join(failed_files[:5])  # Show first 5 failures
                    if len(failed_files) > 5:
                        failed_details += f"\n... and {len(failed_files) - 5} more"
                    
                    summary_msg += f"\n\nFailed files:\n{failed_details}"
                
                QMessageBox.information(self, "Folder Import Complete", summary_msg)
                self.statusBar().showMessage(f"Folder import complete: {total_rows:,} rows imported", 5000)
                
                # Refresh schema browser to show new/updated table
                self.refresh_schema_browser()
            else:
                QMessageBox.critical(self, "Import Failed", "Failed to import the combined data to database.")
                self.statusBar().showMessage("Folder import failed", 5000)
                
        except Exception as e:
            error_msg = str(e)
            QMessageBox.critical(self, "Folder Import Error", f"Failed to import folder:\n{error_msg}")
            self.statusBar().showMessage(f"Folder import failed: {error_msg}", 5000)
    
    def get_delimiter_for_file(self, file_ext, folder_import_info):
        """Get the appropriate delimiter for the file type"""
        delimiter = folder_import_info.get('csv_delimiter', ',')
        
        # Handle special delimiter representations
        if delimiter == '\\t':
            delimiter = '\t'
        elif delimiter == '\\n':
            delimiter = '\n'
        elif delimiter == '\\r':
            delimiter = '\r'
        
        if file_ext == '.csv':
            return delimiter
        elif file_ext == '.tsv':
            return '\t'  # Always tab for TSV
        elif file_ext == '.txt':
            return delimiter
        else:
            return delimiter
    
    def load_excel_all_sheets(self, file_path):
        """Load all sheets from an Excel file and combine them"""
        try:
            excel_file = pd.ExcelFile(file_path)
            all_sheets = []
            
            for sheet_name in excel_file.sheet_names:
                try:
                    sheet_df = pd.read_excel(file_path, sheet_name=sheet_name, dtype=str)
                    if not sheet_df.empty:
                        # Add sheet name column
                        sheet_df['_sheet_name'] = sheet_name
                        all_sheets.append(sheet_df)
                except Exception as e:
                    print(f"Failed to read sheet '{sheet_name}': {e}")
                    continue
            
            if all_sheets:
                # Combine all sheets
                combined_df = pd.concat(all_sheets, ignore_index=True, sort=False)
                return combined_df
            else:
                return None
                
        except Exception as e:
            print(f"Failed to load Excel file '{file_path}': {e}")
            return None
    
    def load_excel_with_dialog(self, file_path):
        """Load Excel file with user sheet selection"""
        try:
            excel_file = pd.ExcelFile(file_path)
            sheet_names = excel_file.sheet_names
            
            if len(sheet_names) == 1:
                # Only one sheet, just load it
                return pd.read_excel(file_path, sheet_name=0, dtype=str)
            
            # Multiple sheets - ask user to select
            sheet_name, ok = QInputDialog.getItem(
                self, 
                f"Select Sheet - {os.path.basename(file_path)}", 
                "Choose which sheet to import:",
                sheet_names, 
                0, 
                False
            )
            
            if ok and sheet_name:
                return pd.read_excel(file_path, sheet_name=sheet_name, dtype=str)
            else:
                # User cancelled - skip this file
                return None
                
        except Exception as e:
            print(f"Failed to load Excel file '{file_path}': {e}")
            return None
    
    def safe_load_data(self, file_path, file_type, import_info):
        """Safely load data from any file type with robust error handling"""
        df = None
        
        try:
            if file_type == '.csv':
                # Try multiple encoding strategies for CSV
                encodings = [import_info.get('encoding', 'utf-8'), 'utf-8', 'latin-1', 'cp1252', 'iso-8859-1']
                for encoding in encodings:
                    try:
                        df = pd.read_csv(
                            file_path,
                            delimiter=import_info.get('delimiter', ','),
                            encoding=encoding,
                            header=0 if import_info.get('header', True) else None,
                            on_bad_lines='skip',  # Skip bad lines instead of failing
                            low_memory=False,     # Read entire file to avoid dtype warnings
                            dtype=str             # Read everything as string initially
                        )
                        print(f"Successfully loaded CSV with encoding: {encoding}")
                        break
                    except UnicodeDecodeError:
                        continue
                    except Exception as e:
                        if encoding == encodings[-1]:  # Last encoding attempt
                            print(f"CSV load failed with all encodings: {e}")
                            # Try to load with different parameters
                            try:
                                df = pd.read_csv(file_path, delimiter=',', encoding='utf-8', header=None, on_bad_lines='skip', dtype=str)
                            except:
                                pass
            
            elif file_type == '.tsv':
                try:
                    df = pd.read_csv(
                        file_path,
                        delimiter=import_info.get('delimiter', '\t'),
                        encoding=import_info.get('encoding', 'utf-8'),
                        header=0 if import_info.get('header', True) else None,
                        on_bad_lines='skip',
                        dtype=str
                    )
                except:
                    # Fallback
                    df = pd.read_csv(file_path, delimiter='\t', encoding='utf-8', header=None, on_bad_lines='skip', dtype=str)
            
            elif file_type == '.txt':
                try:
                    df = pd.read_csv(
                        file_path,
                        delimiter=import_info.get('delimiter', ','),
                        encoding=import_info.get('encoding', 'utf-8'),
                        header=0 if import_info.get('header', True) else None,
                        on_bad_lines='skip',
                        dtype=str
                    )
                except:
                    # Fallback
                    df = pd.read_csv(file_path, delimiter=',', encoding='utf-8', header=None, on_bad_lines='skip', dtype=str)
            
            elif file_type in ['.xlsx', '.xls']:
                try:
                    sheet_name = import_info.get('sheet_name', 0)
                    df = pd.read_excel(file_path, sheet_name=sheet_name, dtype=str)
                except Exception as e:
                    print(f"Excel load failed: {e}")
                    # Try reading first sheet as fallback
                    try:
                        df = pd.read_excel(file_path, sheet_name=0, dtype=str)
                    except:
                        pass
            
            elif file_type == '.parquet':
                try:
                    df = pd.read_parquet(file_path)
                    # Convert to string to avoid type issues
                    df = df.astype(str)
                except Exception as e:
                    print(f"Parquet load failed: {e}")
            
            elif file_type == '.json':
                try:
                    df = pd.read_json(file_path)
                    # Convert to string to avoid type issues
                    df = df.astype(str)
                except Exception as e:
                    print(f"JSON load failed: {e}")
                    # Try alternative JSON loading
                    try:
                        with open(file_path, 'r', encoding='utf-8') as f:
                            import json
                            data = json.load(f)
                            if isinstance(data, list):
                                df = pd.DataFrame(data)
                            elif isinstance(data, dict):
                                df = pd.DataFrame([data])
                            df = df.astype(str)
                    except:
                        pass
            
        except Exception as e:
            print(f"Failed to load {file_type} file: {e}")
            
        return df
    
    def sanitize_dataframe(self, df):
        """Sanitize dataframe to prevent any import errors by converting problematic data to text"""
        try:
            print(f"Sanitizing dataframe with {len(df)} rows and {len(df.columns)} columns")
            
            # Create a copy to avoid modifying the original
            df_clean = df.copy()
            
            # Handle column names - ensure they're clean
            df_clean.columns = [str(col).strip() if col is not None else f"col_{i}" for i, col in enumerate(df_clean.columns)]
            
            # Process each column
            for col in df_clean.columns:
                try:
                    # Convert the entire column to string initially
                    df_clean[col] = df_clean[col].astype(str)
                    
                    # Handle specific problematic values
                    df_clean[col] = df_clean[col].replace({
                        'nan': None,
                        'NaN': None,
                        'None': None,
                        'null': None,
                        'NULL': None,
                        '': None,
                        'inf': 'infinity',
                        '-inf': '-infinity',
                        'Infinity': 'infinity',
                        '-Infinity': '-infinity'
                    })
                    
                    # Remove any remaining problematic characters
                    df_clean[col] = df_clean[col].apply(lambda x: self.safe_string_convert(x) if x is not None else None)
                    
                except Exception as e:
                    print(f"Error processing column {col}: {e}")
                    # If anything fails, convert entire column to safe strings
                    df_clean[col] = df_clean[col].apply(lambda x: str(x) if x is not None else None)
            
            # Remove completely empty rows
            df_clean = df_clean.dropna(how='all')
            
            # Ensure no column names are duplicated
            cols = df_clean.columns.tolist()
            seen = set()
            unique_cols = []
            for col in cols:
                original_col = col
                counter = 1
                while col in seen:
                    col = f"{original_col}_{counter}"
                    counter += 1
                seen.add(col)
                unique_cols.append(col)
            df_clean.columns = unique_cols
            
            print(f"Sanitization complete: {len(df_clean)} rows × {len(df_clean.columns)} columns")
            return df_clean
            
        except Exception as e:
            print(f"Sanitization failed: {e}")
            # Last resort: convert everything to string
            try:
                df_safe = pd.DataFrame()
                for i, col in enumerate(df.columns):
                    col_name = f"column_{i}" if col is None or str(col).strip() == '' else str(col)
                    df_safe[col_name] = df.iloc[:, i].apply(lambda x: str(x) if x is not None else None)
                return df_safe
            except:
                # Ultimate fallback: return empty dataframe with at least one column
                return pd.DataFrame({'data': ['No data could be imported']})
    
    def detect_csv_delimiter(self, file_path, sample_size=1024):
        """Automatically detect the delimiter used in a CSV file"""
        try:
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as file:
                sample = file.read(sample_size)
                
            # Use csv.Sniffer to detect delimiter
            if csv:
                sniffer = csv.Sniffer()
                try:
                    dialect = sniffer.sniff(sample, delimiters=',;\t|')
                    detected_delimiter = dialect.delimiter
                    print(f"Auto-detected delimiter: '{detected_delimiter}' for file: {os.path.basename(file_path)}")
                    return detected_delimiter
                except:
                    pass
            
            # Fallback: count common delimiters and choose the most frequent
            delimiters = {',': 0, ';': 0, '\t': 0, '|': 0}
            for delimiter in delimiters:
                delimiters[delimiter] = sample.count(delimiter)
            
            # Choose the delimiter with the highest count (but at least 1)
            best_delimiter = max(delimiters, key=delimiters.get)
            if delimiters[best_delimiter] > 0:
                print(f"Fallback delimiter detection: '{best_delimiter}' for file: {os.path.basename(file_path)}")
                return best_delimiter
            
            # Default to comma if no delimiter found
            print(f"No delimiter detected, defaulting to comma for file: {os.path.basename(file_path)}")
            return ','
            
        except Exception as e:
            print(f"Error detecting delimiter for {file_path}: {e}")
            return ','
    
    def safe_string_convert(self, value):
        """Safely convert any value to a database-compatible string"""
        try:
            if value is None or pd.isna(value):
                return None
            
            # Handle different types
            if isinstance(value, (int, float)):
                if pd.isna(value) or pd.isinf(value):
                    return None
                return str(value)
            
            # Convert to string and handle encoding issues
            str_val = str(value)
            
            # Remove or replace problematic characters
            str_val = str_val.replace('\x00', '')  # Remove null bytes
            str_val = str_val.replace('\r\n', '\n')  # Normalize line endings
            str_val = str_val.replace('\r', '\n')
            
            # Limit string length to prevent database issues
            if len(str_val) > 10000:  # Reasonable limit
                str_val = str_val[:10000] + "...[truncated]"
            
            return str_val
            
        except Exception:
            return "conversion_error"
    
    def safe_import_to_database(self, df, table_name, mode):
        """Safely import dataframe to database with multiple fallback strategies"""
        try:
            db_type = self.current_connection_info['type'].lower()
            
            # Strategy 1: Try database-specific import
            try:
                if db_type == 'duckdb':
                    # Use DuckDB-specific import method to avoid pandas to_sql issues
                    return self.duckdb_safe_import(df, table_name, mode)
                else:
                    # SQLite import - use safer method without 'multi'
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
                
                print("Normal import successful")
                return True
                
            except Exception as e:
                print(f"Normal import failed: {e}")
                
                # Strategy 2: Force all columns to TEXT type
                try:
                    print("Trying with all TEXT columns...")
                    
                    # Create table manually with all TEXT columns
                    columns_sql = ", ".join([f'"{col}" TEXT' for col in df.columns])
                    
                    if mode == 'replace':
                        self.current_connection.execute(f"DROP TABLE IF EXISTS {table_name}")
                    
                    create_sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns_sql})"
                    self.current_connection.execute(create_sql)
                    
                    if db_type == 'sqlite':
                        self.current_connection.commit()
                    
                    # Insert data row by row if needed
                    df.to_sql(table_name, self.current_connection, if_exists='append', index=False)
                    
                    if db_type == 'sqlite':
                        self.current_connection.commit()
                    
                    print("TEXT columns import successful")
                    return True
                    
                except Exception as e2:
                    print(f"TEXT columns import failed: {e2}")
                    
                    # Strategy 3: Row-by-row insert with error handling
                    try:
                        print("Trying row-by-row insert...")
                        
                        # Ensure table exists
                        if mode == 'replace':
                            self.current_connection.execute(f"DROP TABLE IF EXISTS {table_name}")
                        
                        # Create table with generic structure
                        columns_sql = ", ".join([f'"{col}" TEXT' for col in df.columns])
                        create_sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns_sql})"
                        self.current_connection.execute(create_sql)
                        
                        if db_type == 'sqlite':
                            self.current_connection.commit()
                        
                        # Insert rows one by one, skipping problematic ones
                        successful_rows = 0
                        placeholders = ", ".join(["?" for _ in df.columns])
                        insert_sql = f'INSERT INTO {table_name} VALUES ({placeholders})'
                        
                        for idx, row in df.iterrows():
                            try:
                                values = [self.safe_string_convert(val) for val in row.values]
                                self.current_connection.execute(insert_sql, values)
                                successful_rows += 1
                                
                                if successful_rows % 1000 == 0:
                                    if db_type == 'sqlite':
                                        self.current_connection.commit()
                                    print(f"Inserted {successful_rows} rows...")
                                    
                            except Exception as row_error:
                                print(f"Skipped row {idx}: {row_error}")
                                continue
                        
                        if db_type == 'sqlite':
                            self.current_connection.commit()
                        
                        print(f"Row-by-row insert completed: {successful_rows} rows inserted")
                        return True
                        
                    except Exception as e3:
                        print(f"Row-by-row insert failed: {e3}")
                        return False
        
        except Exception as e:
            print(f"Safe import completely failed: {e}")
            return False
    
    def duckdb_safe_import(self, df, table_name, mode):
        """DuckDB-specific import method to handle transactions properly"""
        try:
            # For DuckDB, avoid pandas to_sql which can cause transaction issues
            # Instead, create table manually and use INSERT statements
            
            if mode == 'create':
                # Check if table exists for create mode
                try:
                    result = self.current_connection.execute(f"SELECT 1 FROM {table_name} LIMIT 1").fetchone()
                    if result is not None:
                        raise ValueError(f"Table '{table_name}' already exists. Use 'Replace' mode to overwrite or 'Append' to add data.")
                except Exception as e:
                    if "does not exist" not in str(e).lower() and "no such table" not in str(e).lower():
                        raise e  # Re-raise if it's not a "table doesn't exist" error
                    # Table doesn't exist, which is what we want for create mode
            elif mode == 'replace':
                try:
                    self.current_connection.execute(f"DROP TABLE IF EXISTS {table_name}")
                except:
                    pass
            
            # Create table with all TEXT columns for safety
            columns_sql = ", ".join([f'"{col}" TEXT' for col in df.columns])
            
            if mode in ['create', 'replace']:
                create_sql = f"CREATE TABLE {table_name} ({columns_sql})"
                self.current_connection.execute(create_sql)
            elif mode == 'append':
                # For append mode, table should already exist
                try:
                    result = self.current_connection.execute(f"SELECT 1 FROM {table_name} LIMIT 1").fetchone()
                except Exception as e:
                    if "does not exist" in str(e).lower() or "no such table" in str(e).lower():
                        raise ValueError(f"Table '{table_name}' does not exist. Use 'Create' mode to create a new table.")
                    raise e
            
            # Insert data using DuckDB's efficient INSERT
            placeholders = ", ".join(["?" for _ in df.columns])
            insert_sql = f'INSERT INTO {table_name} VALUES ({placeholders})'
            
            # Convert all data to strings to avoid type issues
            data_rows = []
            for _, row in df.iterrows():
                converted_row = [self.safe_string_convert(val) for val in row.values]
                data_rows.append(converted_row)
            
            # Batch insert for efficiency
            batch_size = 1000
            for i in range(0, len(data_rows), batch_size):
                batch = data_rows[i:i + batch_size]
                try:
                    self.current_connection.executemany(insert_sql, batch)
                except Exception as batch_error:
                    print(f"Batch insert failed, trying row by row: {batch_error}")
                    # Fall back to individual inserts for this batch
                    for row in batch:
                        try:
                            self.current_connection.execute(insert_sql, row)
                        except Exception as row_error:
                            print(f"Skipped problematic row: {row_error}")
                            continue
            
            print(f"DuckDB import successful: {len(df)} rows")
            return True
            
        except Exception as e:
            print(f"DuckDB import failed: {e}")
            return self.row_by_row_insert_fallback(df, table_name, mode)
    
    def row_by_row_insert_fallback(self, df, table_name, mode):
        """Final fallback method for problematic imports"""
        try:
            db_type = self.current_connection_info['type'].lower()
            
            # Drop and recreate table for replace mode
            if mode == 'replace':
                try:
                    self.current_connection.execute(f"DROP TABLE IF EXISTS {table_name}")
                    if db_type == 'sqlite':
                        self.current_connection.commit()
                except:
                    pass
            
            # Create table with all TEXT columns
            columns_sql = ", ".join([f'"{col}" TEXT' for col in df.columns])
            create_sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns_sql})"
            self.current_connection.execute(create_sql)
            
            if db_type == 'sqlite':
                self.current_connection.commit()
            
            # Insert rows one by one with error handling
            successful_rows = 0
            placeholders = ", ".join(["?" for _ in df.columns])
            insert_sql = f'INSERT INTO {table_name} VALUES ({placeholders})'
            
            for idx, row in df.iterrows():
                try:
                    values = [self.safe_string_convert(val) for val in row.values]
                    self.current_connection.execute(insert_sql, values)
                    successful_rows += 1
                    
                    # Commit periodically for SQLite
                    if successful_rows % 500 == 0 and db_type == 'sqlite':
                        self.current_connection.commit()
                        
                except Exception as row_error:
                    print(f"Skipped row {idx}: {row_error}")
                    continue
            
            if db_type == 'sqlite':
                self.current_connection.commit()
            
            print(f"Fallback import completed: {successful_rows}/{len(df)} rows inserted")
            return successful_rows > 0
            
        except Exception as e:
            print(f"Fallback import failed: {e}")
            return False
    
    def flexible_append_data(self, df, table_name, db_type):
        """Append data with flexible column handling - adds missing columns automatically and handles all errors"""
        try:
            # Get existing table schema with error handling
            existing_columns = set()
            table_exists = False
            
            try:
                if db_type == 'duckdb':
                    existing_columns_df = self.current_connection.execute(f"PRAGMA table_info('{table_name}')").fetchdf()
                    existing_columns = set(existing_columns_df['name'].tolist())
                    table_exists = True
                else:  # sqlite
                    cursor = self.current_connection.cursor()
                    cursor.execute(f"PRAGMA table_info({table_name})")
                    existing_columns = set([row[1] for row in cursor.fetchall()])
                    table_exists = True
            except:
                table_exists = False
            
            if not table_exists:
                # Table doesn't exist, create it with all TEXT columns for safety
                try:
                    columns_sql = ", ".join([f'"{col}" TEXT' for col in df.columns])
                    create_sql = f"CREATE TABLE {table_name} ({columns_sql})"
                    self.current_connection.execute(create_sql)
                    
                    if db_type == 'sqlite':
                        self.current_connection.commit()
                    
                    # Now insert the data
                    df.to_sql(table_name, self.current_connection, if_exists='append', index=False)
                    
                    if db_type == 'sqlite':
                        self.current_connection.commit()
                    return
                except Exception as e:
                    print(f"Failed to create new table: {e}")
                    # Use row-by-row insert as fallback
                    return self.row_by_row_insert_fallback(df, table_name, 'create')
            
            # Get new columns from the dataframe
            new_columns = set(df.columns)
            
            # Find columns that need to be added to the existing table
            missing_columns = new_columns - existing_columns
            
            # Add missing columns to the existing table (always as TEXT for safety)
            if missing_columns:
                print(f"Adding new columns to table '{table_name}': {', '.join(missing_columns)}")
                for col in missing_columns:
                    try:
                        # Always use TEXT type to avoid conflicts
                        alter_sql = f'ALTER TABLE {table_name} ADD COLUMN "{col}" TEXT'
                        self.current_connection.execute(alter_sql)
                    except Exception as alter_error:
                        print(f"Failed to add column {col}: {alter_error}")
                        # Continue with other columns
                        
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
            
            # Convert all data to strings to prevent type conflicts
            for col in df.columns:
                df[col] = df[col].apply(lambda x: self.safe_string_convert(x) if x is not None else None)
            
            # Try to append the data
            try:
                df.to_sql(table_name, self.current_connection, if_exists='append', index=False)
                
                if db_type == 'sqlite':
                    self.current_connection.commit()
                    
            except Exception as append_error:
                print(f"Bulk append failed: {append_error}")
                
                # Try row-by-row insert as fallback
                try:
                    successful_rows = 0
                    placeholders = ", ".join(["?" for _ in df.columns])
                    insert_sql = f'INSERT INTO {table_name} VALUES ({placeholders})'
                    
                    for idx, row in df.iterrows():
                        try:
                            values = [self.safe_string_convert(val) for val in row.values]
                            self.current_connection.execute(insert_sql, values)
                            successful_rows += 1
                            
                            if successful_rows % 100 == 0 and db_type == 'sqlite':
                                self.current_connection.commit()
                                
                        except Exception as row_error:
                            print(f"Skipped problematic row {idx}: {row_error}")
                            continue
                    
                    if db_type == 'sqlite':
                        self.current_connection.commit()
                        
                    print(f"Flexible append completed: {successful_rows} rows inserted")
                    
                except Exception as row_error:
                    print(f"Row-by-row insert also failed: {row_error}")
                    raise
                
        except Exception as e:
            # Final fallback to safe import
            print(f"Flexible append completely failed: {e}")
            return self.safe_import_to_database(df, table_name, 'append')
    
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
    
    def ensure_unique_table_name(self, base_name, mode='create'):
        """Ensure table name is unique and safe for database operations"""
        if not base_name:
            base_name = "imported_table"
        
        # Clean the base name
        clean_base = self.clean_table_name(base_name)
        
        # If we're creating a new table, check for conflicts
        if mode == 'create':
            return self.get_unique_table_name(clean_base)
        else:
            # For append/replace modes, just clean the name
            return clean_base
    
    def clean_table_name(self, table_name):
        """Clean table name to be database-safe"""
        if not table_name:
            return "imported_table"
        
        # Convert to string and strip whitespace
        clean_name = str(table_name).strip()
        
        # Replace problematic characters with underscores
        clean_name = re.sub(r'[^\w\s]', '_', clean_name)
        
        # Replace whitespace with underscores
        clean_name = re.sub(r'\s+', '_', clean_name)
        
        # Remove multiple consecutive underscores
        clean_name = re.sub(r'_+', '_', clean_name)
        
        # Remove leading/trailing underscores
        clean_name = clean_name.strip('_')
        
        # Ensure it doesn't start with a number
        if clean_name and clean_name[0].isdigit():
            clean_name = f"table_{clean_name}"
        
        # Ensure it's not empty and not too long
        if not clean_name:
            clean_name = "imported_table"
        elif len(clean_name) > 50:  # Reasonable limit for table names
            clean_name = clean_name[:50].rstrip('_')
        
        # Check against SQL reserved words
        sql_reserved = {
            'select', 'from', 'where', 'insert', 'update', 'delete', 'create', 'drop', 
            'alter', 'table', 'index', 'view', 'database', 'schema', 'primary', 'key',
            'foreign', 'references', 'constraint', 'unique', 'not', 'null', 'default',
            'check', 'order', 'by', 'group', 'having', 'union', 'join', 'inner', 'outer',
            'left', 'right', 'on', 'as', 'distinct', 'count', 'sum', 'avg', 'max', 'min'
        }
        
        if clean_name.lower() in sql_reserved:
            clean_name = f"tbl_{clean_name}"
        
        return clean_name.lower()
    
    def get_unique_table_name(self, base_name):
        """Get a unique table name by checking existing tables and adding suffix if needed"""
        if not self.current_connection or not self.current_connection_info:
            return base_name
        
        try:
            # Get existing table names
            existing_tables = set()
            
            if self.current_connection_info['type'].lower() == 'duckdb':
                tables_df = self.current_connection.execute("SHOW TABLES").fetchdf()
                existing_tables = set(tables_df['name'].str.lower()) if not tables_df.empty else set()
            else:  # SQLite
                cursor = self.current_connection.cursor()
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
                existing_tables = {row[0].lower() for row in cursor.fetchall()}
            
            # If base name is unique, use it
            if base_name.lower() not in existing_tables:
                return base_name
            
            # Find unique name with suffix
            counter = 1
            while True:
                candidate = f"{base_name}_{counter}"
                if candidate.lower() not in existing_tables:
                    return candidate
                counter += 1
                
                # Safety check to prevent infinite loop
                if counter > 1000:
                    import time
                    timestamp = int(time.time())
                    return f"{base_name}_{timestamp}"
                    
        except Exception as e:
            print(f"Error checking table uniqueness: {e}")
            # Fallback to timestamp-based naming
            import time
            timestamp = int(time.time())
            return f"{base_name}_{timestamp}"
    
    def validate_import_data(self, df, table_name, mode):
        """Validate data before import and provide helpful error messages"""
        errors = []
        warnings = []
        
        # Check if dataframe is empty
        if df is None or df.empty:
            errors.append("No data to import - the file appears to be empty")
            return errors, warnings
        
        # Check table name
        if not table_name or not table_name.strip():
            errors.append("Table name cannot be empty")
        elif len(table_name.strip()) > 50:
            warnings.append(f"Table name is very long ({len(table_name)} chars) - it will be truncated")
        
        # Check column names
        if df.columns.empty:
            errors.append("No columns found in the data")
        else:
            # Check for duplicate column names
            column_counts = df.columns.value_counts()
            duplicates = column_counts[column_counts > 1]
            if not duplicates.empty:
                warnings.append(f"Duplicate column names found: {list(duplicates.index)} - they will be renamed")
            
            # Check for problematic column names
            problematic_cols = []
            for col in df.columns:
                if not str(col).strip():
                    problematic_cols.append("(empty column name)")
                elif str(col).strip().isdigit():
                    problematic_cols.append(f"'{col}' (starts with number)")
            
            if problematic_cols:
                warnings.append(f"Problematic column names will be cleaned: {problematic_cols}")
        
        # Check data size
        row_count = len(df)
        col_count = len(df.columns)
        
        if row_count == 0:
            warnings.append("No data rows found (only headers)")
        elif row_count > 1000000:
            warnings.append(f"Large dataset ({row_count:,} rows) - import may take some time")
        
        if col_count > 100:
            warnings.append(f"Many columns ({col_count}) - consider if all are needed")
        
        # Check for mode-specific issues
        if mode in ['append', 'replace'] and self.current_connection:
            try:
                # Check if target table exists for append/replace
                table_exists = False
                if self.current_connection_info['type'].lower() == 'duckdb':
                    result = self.current_connection.execute(f"SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '{table_name}'").fetchone()
                    table_exists = result[0] > 0 if result else False
                else:  # SQLite
                    cursor = self.current_connection.cursor()
                    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table_name,))
                    table_exists = cursor.fetchone() is not None
                
                if not table_exists:
                    if mode == 'append':
                        errors.append(f"Cannot append to table '{table_name}' - table does not exist")
                    elif mode == 'replace':
                        warnings.append(f"Table '{table_name}' does not exist - will create new table")
                        
            except Exception as e:
                warnings.append(f"Could not verify table existence: {str(e)}")
        
        return errors, warnings
    
    def handle_duplicate_columns(self, df):
        """Handle duplicate column names by renaming them"""
        if df is None or df.empty:
            return df
        
        # Get column names and find duplicates
        columns = list(df.columns)
        seen = {}
        new_columns = []
        
        for col in columns:
            col_str = str(col).strip()
            col_lower = col_str.lower()
            
            if col_lower in seen:
                # This is a duplicate, add a suffix
                seen[col_lower] += 1
                new_col = f"{col_str}_{seen[col_lower]}"
            else:
                # First occurrence
                seen[col_lower] = 0
                new_col = col_str
            
            new_columns.append(new_col)
        
        # Update dataframe columns
        df.columns = new_columns
        return df
    
    def execute_current_query(self):
        current_tab = self.tab_widget.currentWidget()
        if current_tab:
            current_tab.execute_query()
    
    def execute_selected_query(self):
        """Execute only the selected text as a query"""
        current_tab = self.tab_widget.currentWidget()
        if current_tab:
            cursor = current_tab.editor.textCursor()
            if cursor.hasSelection():
                current_tab.execute_query()  # Will automatically detect selection
            else:
                # If no selection, show a message
                self.statusBar().showMessage("No text selected. Select SQL code to execute or use F5 to run the entire query.", 3000)
    
    def export_current_results(self):
        """Export results from the current tab"""
        current_tab = self.tab_widget.currentWidget()
        if current_tab and hasattr(current_tab, 'show_export_menu'):
            current_tab.show_export_menu()
        else:
            QMessageBox.information(self, "No Results", "No query results to export. Please run a query first.")
    
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
