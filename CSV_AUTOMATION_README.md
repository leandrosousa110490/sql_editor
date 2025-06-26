# CSV Automation Feature

The CSV Automation feature provides a user-friendly way to process multiple CSV folder sources, apply SQL transformations, and load data into your DuckDB database automatically.

## Features

- **Multiple CSV Sources**: Add multiple folders containing CSV files as separate data sources
- **Table Naming**: Specify custom table names for each CSV source
- **File Pattern Matching**: Use patterns like `*.csv`, `sales_*.csv`, or `data_2024_*.csv` to filter files
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
    - **Browse**: Select the folder containing your CSV files
    - **Table Name**: Enter a unique name for this data source (auto-suggested based on folder name)
    - **File Pattern**: Specify which files to include (default: `*.csv`)
    - **Preview**: See which files will be processed

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

Let's say you have three folders of CSV files you want to combine:

1. **Sales Data** (`/data/sales/`) - Contains daily sales files
2. **Customer Data** (`/data/customers/`) - Contains customer information

**Step-by-step:**

1. **Add Sources:**
   - Source 1: Folder = `/data/sales/`, Table = `sales`, Pattern = `*.csv`
   - Source 2: Folder = `/data/customers/`, Table = `customers`, Pattern = `*.csv`

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
      "folder_path": "C:\\data\\sales",
      "table_name": "sales_data",
      "file_pattern": "*.csv"
    }
  ],
  "sql_query": "SELECT * FROM sales_data WHERE amount > 1000",
  "output_table": "high_value_sales"
}
```

## Tips and Best Practices

1. **Table Naming**: Use descriptive, SQL-compatible names (letters, numbers, underscores only)
2. **File Patterns**: Be specific with patterns to avoid processing unwanted files
3. **Testing**: Start with a small subset of files to test your workflow
4. **Schema Consistency**: Ensure CSV files within each source have consistent column structures
5. **Save Configurations**: Save complex automations for reuse
6. **Version Control**: Keep your automation JSON files in version control for team sharing

## Troubleshooting

**Common Issues:**
- **"No CSV files found"**: Check your file pattern and folder path
- **"Table names must be unique"**: Ensure each source has a different table name
- **SQL errors**: Verify table names match your source configurations 