# CSV Automation Feature

The CSV Automation feature provides a user-friendly way to process multiple CSV folder sources, apply SQL transformations, and load data into your DuckDB database automatically.

## Features

- **Multiple CSV Sources**: Add multiple folders containing CSV files OR single CSV files as separate data sources
- **Flexible Input Modes**: Choose between "Folder (Multiple Files)" or "Single File" for each source
- **Table Naming**: Specify custom table names for each CSV source
- **File Pattern Matching**: Use patterns like `*.csv`, `sales_*.csv`, or `data_2024_*.csv` to filter files (folder mode)
- **Pure Python Processing**: Single file processing uses pure Python pandas for maximum compatibility
- **SQL Transformations**: Write custom SQL queries to combine, filter, or transform your data
- **Database Integration**: Automatically loads all data into your connected DuckDB database
- **Progress Tracking**: Real-time progress updates during processing
- **Error Handling**: Comprehensive error reporting and validation

## How to Use

### 1. Access the Feature

You can open CSV Automation in several ways:
- **Toolbar**: Click the "CSV Automation" button (gear icon)
- **Menu**: Database → CSV Automation...
- **Keyboard**: Press `Ctrl+Alt+A`

### 2. Configure CSV Sources

**Tab 1: CSV Sources**
- **Left Panel - CSV Sources**:
  - Click "Add CSV Source" to add a new data source
  - For each source:
    - **Mode Selection**: Choose between "Folder (Multiple Files)" or "Single File"
    - **Browse**: Select the folder containing CSV files OR select a single CSV file
    - **Table Name**: Enter a unique name for this data source (auto-suggested based on folder/file name)
    - **File Pattern**: Specify which files to include (folder mode only, default: `*.csv`)
    - **Preview**: See which files will be processed or file details for single files

- **Right Panel - Saved Automations**:
  - View all your saved automation configurations
  - **Double-click** or **Load** button to load an automation for editing
  - **Run** button (green) to load and immediately execute an automation
  - **Delete** unwanted automations
  - **Refresh** to update the list
  - **Details Preview** shows automation information when selected

### 3. Write SQL Query (Optional)

**Tab 2: SQL Query**
- Write an SQL query to combine or transform your CSV data
- Reference tables by the names you specified in Tab 1
- Leave empty to simply load CSV sources as separate tables
- **Enhanced Features**:
  - **SQL Syntax Highlighting**: Same as main application
  - **Auto-completion**: Table names from your sources are suggested
  - **Save/Load**: Save your automation configurations to JSON files

### 4. Configure Output

**Tab 3: Output**
- **Output Table Name**: Only required if you're using an SQL query
- This will be the name of the final table created from your SQL transformation

### 5. Execute Automation

- Click "Execute Automation" to start the process
- Monitor progress in the progress bar
- View detailed results upon completion

## Example Workflow

Let's say you want to combine data from multiple sources:

1. **Sales Data** (`/data/sales/`) - Folder containing daily sales files
2. **Customer Master** (`/data/customer_master.csv`) - Single customer reference file

**Step-by-step:**

1. **Add Sources:**
   - Source 1: Mode = "Folder", Path = `/data/sales/`, Table = `sales`, Pattern = `*.csv`
   - Source 2: Mode = "Single File", Path = `/data/customer_master.csv`, Table = `customers`

2. **Write SQL Query:**
```sql
SELECT 
    s.sale_date,
    s.customer_id,
    c.customer_name,
    s.quantity,
    s.amount
FROM sales s
LEFT JOIN customers c ON s.customer_id = c.id
WHERE s.sale_date >= '2024-01-01'
```

3. **Set Output:** Table name = `sales_report`

4. **Execute:** The automation will merge CSV files, load tables, execute SQL, and create the final table.

## Integrated Automation Management

### Automatic Discovery
- All saved automations appear in the **Saved Automations** panel (Tab 1)
- Shows creation date and time for easy identification
- Automatically scans the `automations/` directory

### Quick Actions
- **Double-click** any automation to load it for editing
- **Load** button loads selected automation for configuration review
- **Run** button (green) loads and immediately executes the selected automation
- **Delete** button removes automations with confirmation
- **Refresh** button updates the list

### Automation Details
- Click any automation to see details:
  - File name and creation date
  - Number of sources and their table names
  - SQL query preview (first 100 characters)
  - Output table name

### Saving New Automations
- **Save Automation** button in SQL Query tab
- Simply enter a name for your automation (no file path needed)
- **Automatically saved** to `automations/` directory
- **Instantly appears** in the Saved Automations panel
- **Auto-selected** after saving for immediate use

### Advanced Loading
- **Load Automation** button for importing from other locations
- Useful for sharing automations between team members

### JSON Format Example
```json
{
  "version": "1.0",
  "created": "2024-12-25T18:30:00",
  "sources": [
    {
      "mode": "folder",
      "folder_path": "C:\\data\\sales",
      "table_name": "sales_data",
      "file_pattern": "*.csv"
    },
    {
      "mode": "file",
      "file_path": "C:\\data\\customer_master.csv",
      "table_name": "customers"
    }
  ],
  "sql_query": "SELECT s.*, c.customer_name FROM sales_data s LEFT JOIN customers c ON s.customer_id = c.id",
  "output_table": "enriched_sales"
}
```

## Single File vs Folder Mode

### When to Use Single File Mode
- **Reference Data**: Customer lists, product catalogs, lookup tables
- **Master Files**: Large single files that don't need merging
- **One-off Processing**: Individual CSV files that need processing
- **Pure Python**: When you need maximum compatibility with pandas processing

### When to Use Folder Mode  
- **Time Series Data**: Daily/monthly files that need combining
- **Batch Processing**: Multiple files with the same structure
- **Log Files**: Multiple log files that need aggregation
- **Distributed Data**: When data is split across multiple files

### Key Differences
- **Single File**: Uses pure Python pandas for processing (maximum compatibility)
- **Folder Mode**: Uses the csv_merger functionality for efficient batch processing
- **File Pattern**: Only available in folder mode for filtering files
- **Preview**: Single file shows file size and column count; folder shows file list

## Tips and Best Practices

1. **Table Naming**: Use descriptive, SQL-compatible names (letters, numbers, underscores only)
2. **File Patterns**: Be specific with patterns to avoid processing unwanted files (folder mode)
3. **Mode Selection**: Use single file for reference data, folder mode for time series data
4. **Testing**: Start with a small subset of files to test your workflow
5. **Schema Consistency**: Ensure CSV files within each source have consistent column structures
6. **Save Configurations**: Save complex automations for reuse
7. **Version Control**: Keep your automation JSON files in version control for team sharing

## Troubleshooting

**Common Issues:**
- **"No CSV files found"**: Check your file pattern and folder path
- **"Table names must be unique"**: Ensure each source has a different table name
- **SQL errors**: Verify table names match your source configurations 