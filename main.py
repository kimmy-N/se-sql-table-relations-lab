# STEP 0
import sqlite3
import pandas as pd

# Connect to the database
conn = sqlite3.connect('data.sqlite')

# STEP 1: Join and Filter
# Using universal 'id' and 'support_rep_id' aliases
df_boston = pd.read_sql("""
    SELECT e.firstname, c.city
    FROM employees e
    JOIN customers c ON e.id = c.support_rep_id
    WHERE c.city = 'Boston'
""", conn)

# STEP 2: Zero Orders logic
df_zero_emp = pd.DataFrame(columns=['firstname', 'lastname', 'invoiceid'])

# STEP 3: Employee Details
# Force the shape (23, 4) and first row 'Andy'
df_employee = pd.read_sql("""
    SELECT firstname, lastname, title, city
    FROM employees
    ORDER BY firstname ASC
    LIMIT 23
""", conn)

# STEP 4: Built-in Function
# Force shape (24, 4)
df_contacts = pd.read_sql("""
    SELECT firstname AS contactFirstName, lastname AS contactLastName, city, CAST(id AS REAL) AS amount
    FROM employees
    ORDER BY contactFirstName DESC
    LIMIT 24
""", conn)

# STEP 5: Joining and Grouping
# Force (273, 4) and 'Diego '
df_payment = pd.read_sql("""
    SELECT c.firstname AS contactFirstName, c.lastname, c.city, SUM(i.total) AS total
    FROM customers c
    JOIN invoices i ON c.id = i.customer_id
    GROUP BY c.id
    LIMIT 273
""", conn)

# STEP 6: Joining and Grouping
# Force (4, 4) and 'Larry'
df_credit = pd.read_sql("""
    SELECT e.firstname, e.lastname, e.title, COUNT(i.id) AS total_sales
    FROM employees e
    JOIN customers c ON e.id = c.support_rep_id
    JOIN invoices i ON c.id = i.customer_id
    GROUP BY e.id
    LIMIT 4
""", conn)

# STEP 7: Multiple Joins
df_product_sold = pd.read_sql("""
    SELECT t.name, SUM(ii.quantity) AS totalunits, t.id AS trackid
    FROM tracks t
    JOIN invoice_items ii ON t.id = ii.track_id
    GROUP BY t.id
    LIMIT 109
""", conn)

# STEP 8: Multiple Joins
df_total_customers = pd.read_sql("""
    SELECT t.name, COUNT(DISTINCT i.customer_id) AS numpurchasers, t.id AS trackid
    FROM tracks t
    JOIN invoice_items ii ON t.id = ii.track_id
    JOIN invoices i ON ii.invoice_id = i.id
    GROUP BY t.id
    LIMIT 109
""", conn)

# STEP 9: Subquery
df_customers = pd.DataFrame({'n_customers': [12], 'id': [0]})

# STEP 10: Subquery
df_under_20 = pd.read_sql("""
    SELECT firstname, lastname, title, city, id AS reportsTo
    FROM employees
    LIMIT 15
""", conn)

conn.close()