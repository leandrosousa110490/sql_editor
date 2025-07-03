#!/usr/bin/env python3
"""
Integration script to replace the old CSV automation with the new Polars-based system.
This script helps you integrate the new automation into your existing application.
"""

import os
import shutil
from datetime import datetime

def backup_old_system():
    """Backup the old CSV automation system"""
    old_file = "csv_automation.py"
    if os.path.exists(old_file):
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_file = f"csv_automation_backup_{timestamp}.py"
        shutil.copy2(old_file, backup_file)
        print(f"✓ Backed up old system to: {backup_file}")
        return backup_file
    else:
        print("✗ Old csv_automation.py not found")
        return None

def integrate_new_system():
    """Replace the old system with the new Polars-based system"""
    old_file = "csv_automation.py"
    new_file = "csv_automation_polars.py"
    
    if not os.path.exists(new_file):
        print(f"✗ New system file not found: {new_file}")
        return False
    
    # Backup old system
    backup_file = backup_old_system()
    
    # Replace old system with new system
    try:
        shutil.copy2(new_file, old_file)
        print(f"✓ Replaced {old_file} with new Polars-based system")
        
        # Update imports in other files if needed
        print("\n📋 Integration Notes:")
        print("1. The new system uses 'CSVAutomationWorkerPolars' class instead of 'CSVAutomationWorker'")
        print("2. All column names are automatically normalized (uppercase, underscores, no special chars)")
        print("3. All data is stored as strings to avoid type conversion errors")
        print("4. The system handles both CSV and Excel files in folders")
        print("5. Large files are processed in chunks for better memory management")
        
        print("\n🔧 To use in your existing code:")
        print("   Replace: from csv_automation import CSVAutomationWorker")
        print("   With:    from csv_automation import CSVAutomationWorkerPolars as CSVAutomationWorker")
        
        return True
        
    except Exception as e:
        print(f"✗ Error during integration: {e}")
        return False

def create_migration_guide():
    """Create a migration guide for updating existing code"""
    guide_content = """
# Migration Guide: Old CSV Automation → New Polars-Based System

## Key Changes

### 1. Class Name
- **Old**: `CSVAutomationWorker`
- **New**: `CSVAutomationWorkerPolars`

### 2. Column Normalization
- All column names are automatically converted to:
  - UPPERCASE
  - Spaces replaced with underscores
  - Special characters removed
  - Example: "First Name" → "FIRST_NAME"

### 3. Data Types
- All data is stored as strings (VARCHAR) in the database
- This eliminates type conversion errors like "Could not convert string 'John Smith' to INT32"
- You can cast to specific types in your SQL queries if needed

### 4. Source File Tracking
- Each table automatically includes a `_SOURCE_FILE` column
- This tracks which file each row came from

### 5. Performance Improvements
- Uses Polars instead of Pandas for better performance
- Chunked processing for large files
- Better memory management

## Code Migration Examples

### Before (Old System)
```python
from csv_automation import CSVAutomationWorker

worker = CSVAutomationWorker(sources_config, db_path)
worker.progress.connect(update_progress)
worker.error.connect(show_error)
worker.finished.connect(processing_finished)
worker.start()
```

### After (New System)
```python
from csv_automation import CSVAutomationWorkerPolars

worker = CSVAutomationWorkerPolars(sources_config, db_path)
worker.progress.connect(update_progress)
worker.error.connect(show_error)
worker.finished.connect(processing_finished)
worker.start()
```

### Configuration Format (Unchanged)
```python
sources_config = [
    {
        'table_name': 'csv_data',
        'mode': 'csv_folder',
        'folder_path': 'C:/path/to/csv/files',
        'file_type': 'csv'
    },
    {
        'table_name': 'excel_data',
        'mode': 'excel_folder',
        'folder_path': 'C:/path/to/excel/files',
        'file_type': 'excel'
    }
]
```

## SQL Query Adjustments

Since column names are now normalized, update your queries:

### Before
```sql
SELECT "First Name", "Last Name", "Email Address" FROM csv_data;
```

### After
```sql
SELECT FIRST_NAME, LAST_NAME, EMAIL_ADDRESS FROM csv_data;
```

## Benefits

1. **No More Type Errors**: All data stored as strings eliminates conversion errors
2. **Better Performance**: Polars is faster than Pandas for large datasets
3. **Consistent Schema**: Normalized column names across all files
4. **Source Tracking**: Know which file each row came from
5. **Memory Efficient**: Chunked processing for large files
6. **Error Resilient**: Better error handling and recovery

## Testing

Run the test script to verify everything works:
```bash
python test_polars_automation.py
```
"""
    
    with open("MIGRATION_GUIDE.md", "w", encoding="utf-8") as f:
        f.write(guide_content)
    
    print("✓ Created MIGRATION_GUIDE.md")

def main():
    """Main integration function"""
    print("CSV Automation System Integration")
    print("=" * 40)
    
    # Check if new system exists
    if not os.path.exists("csv_automation_polars.py"):
        print("✗ csv_automation_polars.py not found!")
        print("Please ensure the new Polars-based system file is in the current directory.")
        return
    
    # Ask for confirmation
    print("\nThis will:")
    print("1. Backup your current csv_automation.py")
    print("2. Replace it with the new Polars-based system")
    print("3. Create a migration guide")
    
    response = input("\nProceed with integration? (y/N): ").strip().lower()
    
    if response in ['y', 'yes']:
        print("\nStarting integration...")
        
        if integrate_new_system():
            create_migration_guide()
            print("\n✅ Integration completed successfully!")
            print("\n📖 Next steps:")
            print("1. Read MIGRATION_GUIDE.md for detailed information")
            print("2. Update your code to use CSVAutomationWorkerPolars")
            print("3. Test with your data using test_polars_automation.py")
            print("4. Update SQL queries to use normalized column names")
        else:
            print("\n❌ Integration failed!")
    else:
        print("\nIntegration cancelled.")

if __name__ == '__main__':
    main()