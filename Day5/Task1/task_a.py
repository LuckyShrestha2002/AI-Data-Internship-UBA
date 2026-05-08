import mysql.connector
import csv
import os
from dotenv import load_dotenv

# 1. Load environment variables for security
load_dotenv()

def connect_to_db():
    try:
        return mysql.connector.connect(
            host=os.getenv("DB_HOST"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD")
        )
    except mysql.connector.Error as err:
        print(f"Error: {err}")
        return None

def setup_database():
    db = connect_to_db()
    cursor = db.cursor()

    # Create Database
    cursor.execute("CREATE DATABASE IF NOT EXISTS store_db")
    cursor.execute("USE store_db")

    # Create Tables with Foreign Keys
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS customers (
            customer_id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(100),
            city VARCHAR(50)
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS products (
            product_id INT AUTO_INCREMENT PRIMARY KEY,
            product_name VARCHAR(100),
            price DECIMAL(10, 2)
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS orders (
            order_id INT AUTO_INCREMENT PRIMARY KEY,
            customer_id INT,
            product_id INT,
            quantity INT,
            FOREIGN KEY (customer_id) REFERENCES customers(customer_id),
            FOREIGN KEY (product_id) REFERENCES products(product_id)
        )
    """)
    
    # --- Data Insertion using Parameterized Queries (%s) ---
    # Inserting 10 Customers
    customers_data = [
        ('Alice', 'Kathmandu'), ('Bob', 'Lalitpur'), ('Charlie', 'Pokhara'),
        ('David', 'Kathmandu'), ('Eve', 'Bhaktapur'), ('Frank', 'Lalitpur'),
        ('Grace', 'Pokhara'), ('Heidi', 'Kathmandu'), ('Ivan', 'Butwal'), ('Jack', 'Biratnagar')
    ]
    cursor.executemany("INSERT INTO customers (name, city) VALUES (%s, %s)", customers_data)

    # Inserting 8 Products
    products_data = [
        ('Laptop', 1200.00), ('Mouse', 25.50), ('Keyboard', 45.00),
        ('Monitor', 200.00), ('Webcam', 80.00), ('Headset', 60.00),
        ('Desk Lamp', 30.00), ('USB Cable', 10.00)
    ]
    cursor.executemany("INSERT INTO products (product_name, price) VALUES (%s, %s)", products_data)

    # Inserting 20 Orders (linking customer_ids 1-10 and product_ids 1-8)
    orders_data = [
        (1, 1, 1), (1, 2, 2), (2, 3, 1), (2, 1, 1), (3, 4, 1),
        (4, 5, 2), (5, 6, 1), (6, 7, 3), (7, 8, 5), (8, 1, 1),
        (9, 2, 2), (10, 3, 1), (1, 4, 1), (2, 5, 1), (3, 6, 2),
        (4, 7, 1), (5, 8, 1), (1, 1, 1), (2, 2, 1), (3, 3, 1)
    ]
    cursor.executemany("INSERT INTO orders (customer_id, product_id, quantity) VALUES (%s, %s, %s)", orders_data)
    
    db.commit()
    print("Database setup and data insertion completed successfully.")
    return db, cursor

def run_analytics(db, cursor):
    # Query 1: Total money spent per customer
    print("\n--- Total Spent Per Customer ---")
    query1 = """
        SELECT c.name, SUM(p.price * o.quantity) as total_spent
        FROM customers c
        JOIN orders o ON c.customer_id = o.customer_id
        JOIN products p ON o.product_id = p.product_id
        GROUP BY c.customer_id
        ORDER BY total_spent DESC
    """
    cursor.execute(query1)
    results1 = cursor.fetchall()
    for row in results1:
        print(f"Customer: {row[0]} | Total: ${row[1]}")

    # Query 2: Most ordered product by quantity
    print("\n--- Most Ordered Product ---")
    query2 = """
        SELECT p.product_name, SUM(o.quantity) as total_qty
        FROM products p
        JOIN orders o ON p.product_id = o.product_id
        GROUP BY p.product_id
        ORDER BY total_qty DESC LIMIT 1
    """
    cursor.execute(query2)
    print(cursor.fetchone())

    # Query 3: Customers with > 2 orders
    print("\n--- Frequent Buyers (> 2 Orders) ---")
    query3 = """
        SELECT c.name, COUNT(o.order_id) as order_count
        FROM customers c
        JOIN orders o ON c.customer_id = o.customer_id
        GROUP BY c.customer_id
        HAVING order_count > 2
    """
    cursor.execute(query3)
    for row in cursor.fetchall():
        print(f"{row[0]}: {row[1]} orders")

    # Query 4: Average order value per city
    print("\n--- Average Order Value Per City ---")
    query4 = """
        SELECT c.city, AVG(p.price * o.quantity) as avg_val
        FROM customers c
        JOIN orders o ON c.customer_id = o.customer_id
        JOIN products p ON o.product_id = p.product_id
        GROUP BY c.city
    """
    cursor.execute(query4)
    for row in cursor.fetchall():
        print(f"City: {row[0]} | Avg: ${row[1]:.2f}")

    # --- Exporting Revenue Report to CSV ---
    with open('revenue_report.csv', 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['Customer Name', 'Total Revenue'])
        writer.writerows(results1)
    print("\nRevenue report exported to revenue_report.csv")

if __name__ == "__main__":
    db_conn, db_cursor = setup_database()
    run_analytics(db_conn, db_cursor)
    db_cursor.close()
    db_conn.close()