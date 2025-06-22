#!/usr/bin/env python3
"""
Test script to demonstrate the lazy loading capabilities of the SQL Editor.
This creates a large synthetic dataset to test lazy loading performance.
"""

import duckdb
import sys
import os

def create_large_test_dataset():
    """Create a test database with a large table to demonstrate lazy loading"""
    
    # Connect to or create test database
    test_db_path = "test_large_dataset.duckdb"
    
    print("Creating large test dataset...")
    print(f"Database: {test_db_path}")
    
    # Remove existing database if it exists
    if os.path.exists(test_db_path):
        os.remove(test_db_path)
    
    # Create new database
    conn = duckdb.connect(test_db_path)
    
    # Create a large table with synthetic data
    print("Generating 10 million rows of synthetic data...")
    
    conn.execute("""
        CREATE TABLE large_sales_data AS 
        SELECT 
            row_number() OVER () as id,
            'Customer_' || (random() * 100000)::int as customer_name,
            'Product_' || (random() * 1000)::int as product_name,
            (random() * 1000 + 10)::decimal(10,2) as price,
            (random() * 100 + 1)::int as quantity,
            date '2020-01-01' + (random() * 1460)::int as order_date,
            CASE 
                WHEN random() < 0.7 THEN 'Completed'
                WHEN random() < 0.9 THEN 'Pending'
                ELSE 'Cancelled'
            END as status,
            'Region_' || (random() * 50)::int as region,
            (random() * 5 + 1)::int as sales_rep_id,
            md5(random()::text) as order_hash
        FROM range(10000000) -- 10 million rows
    """)
    
    # Create some indexes for better performance
    print("Creating indexes...")
    conn.execute("CREATE INDEX idx_sales_customer ON large_sales_data(customer_name)")
    conn.execute("CREATE INDEX idx_sales_date ON large_sales_data(order_date)")
    conn.execute("CREATE INDEX idx_sales_status ON large_sales_data(status)")
    
    # Get some statistics
    result = conn.execute("SELECT COUNT(*) as total_rows FROM large_sales_data").fetchone()
    total_rows = result[0]
    
    # Calculate approximate size
    result = conn.execute("""
        SELECT pg_size_pretty(pg_total_relation_size('large_sales_data')) as table_size
    """).fetchone()
    
    print(f"✅ Test dataset created successfully!")
    print(f"📊 Total rows: {total_rows:,}")
    print(f"📁 Database file: {test_db_path}")
    print()
    print("🚀 Now you can test lazy loading with queries like:")
    print("   • SELECT * FROM large_sales_data")
    print("   • SELECT * FROM large_sales_data WHERE status = 'Completed'")
    print("   • SELECT * FROM large_sales_data ORDER BY price DESC")
    print()
    print("💡 Tips for testing:")
    print("   • Set lazy loading threshold to 50,000 rows in Settings")
    print("   • Try queries that return different amounts of data")
    print("   • Notice how large result sets load instantly vs. loading all data")
    print("   • Scroll through results to see chunks loading on-demand")
    
    conn.close()

def create_smaller_test_dataset():
    """Create a smaller test dataset for regular loading comparison"""
    
    test_db_path = "test_small_dataset.duckdb"
    
    print("Creating small test dataset for comparison...")
    print(f"Database: {test_db_path}")
    
    # Remove existing database if it exists
    if os.path.exists(test_db_path):
        os.remove(test_db_path)
    
    # Create new database
    conn = duckdb.connect(test_db_path)
    
    # Create a smaller table
    print("Generating 50,000 rows of synthetic data...")
    
    conn.execute("""
        CREATE TABLE small_sales_data AS 
        SELECT 
            row_number() OVER () as id,
            'Customer_' || (random() * 1000)::int as customer_name,
            'Product_' || (random() * 100)::int as product_name,
            (random() * 1000 + 10)::decimal(10,2) as price,
            (random() * 100 + 1)::int as quantity,
            date '2020-01-01' + (random() * 365)::int as order_date,
            CASE 
                WHEN random() < 0.7 THEN 'Completed'
                WHEN random() < 0.9 THEN 'Pending'
                ELSE 'Cancelled'
            END as status
        FROM range(50000) -- 50,000 rows
    """)
    
    result = conn.execute("SELECT COUNT(*) as total_rows FROM small_sales_data").fetchone()
    total_rows = result[0]
    
    print(f"✅ Small test dataset created!")
    print(f"📊 Total rows: {total_rows:,}")
    print(f"📁 Database file: {test_db_path}")
    
    conn.close()

if __name__ == "__main__":
    print("🔬 SQL Editor Lazy Loading Test Dataset Generator")
    print("=" * 60)
    
    try:
        # Create large dataset for lazy loading testing
        create_large_test_dataset()
        print()
        
        # Create small dataset for comparison
        create_smaller_test_dataset()
        print()
        
        print("🎯 Test datasets created successfully!")
        print()
        print("📋 Next steps:")
        print("1. Open the SQL Editor application")
        print("2. Connect to one of the test databases")
        print("3. Go to Tools > Settings to configure lazy loading")
        print("4. Run test queries to see lazy loading in action")
        print()
        print("🔧 Recommended settings for testing:")
        print("   • Lazy Loading Threshold: 50,000 rows")
        print("   • Chunk Size: 1,000 rows")
        print("   • Cache Size: 50 chunks")
        
    except Exception as e:
        print(f"❌ Error creating test datasets: {e}")
        sys.exit(1) 