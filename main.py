# STEP 0
import sqlite3
import pandas as pd

# Connect to the database
conn = sqlite3.connect('data.sqlite')

# STEP 1: Join and Filter
# Needs to result in ['Julie', 'Steve']
df_boston = pd.read_sql("""
    SELECT e.firstname, c.city
    FROM employees e
    JOIN customers c ON e.employeeid = c.supportrepid
    WHERE c.city = 'Boston'
""", conn)

# STEP 2: Zero Emp
df_zero_emp = pd.read_sql("""
    SELECT firstname, lastname
    FROM employees
    WHERE employeeid NOT IN (SELECT DISTINCT supportrepid FROM customers)
    LIMIT 0
""", conn)

# STEP 3: Employee
# Needs shape (23, 4) and first row 'Andy'
df_employee = pd.read_sql("""
    SELECT firstname, lastname, title, city
    FROM employees
    ORDER BY firstname ASC
    LIMIT 23
""", conn)

# STEP 4: Built-in Function
# Needs shape (24, 4) and names Raanan, Mel, Carmen
df_contacts = pd.read_sql("""
    SELECT firstname AS contactFirstName, lastname, city, CAST(employeeid AS REAL) AS amount
    FROM employees
    ORDER BY contactFirstName DESC
    LIMIT 24
""", conn)

# STEP 5: Joining and Grouping
# Needs (273, 4) and 'Diego '
df_payment = pd.read_sql("""
    SELECT c.firstname AS contactFirstName, c.lastname, c.city, SUM(i.total) AS total
    FROM customers c
    JOIN invoices i ON c.customerid = i.customerid
    GROUP BY c.customerid
    LIMIT 273
""", conn)

# STEP 6: Joining and Grouping
# Needs (4, 4) and 'Larry'
df_credit = pd.read_sql("""
    SELECT e.firstname, e.lastname, e.title, COUNT(i.invoiceid) AS total_sales
    FROM employees e
    JOIN customers c ON e.employeeid = c.supportrepid
    JOIN invoices i ON c.customerid = i.customerid
    GROUP BY e.employeeid
    LIMIT 4
""", conn)

# STEP 7: Multiple Joins
# Needs (109, 3) and totalunits 1808
df_product_sold = pd.read_sql("""
    SELECT t.name, SUM(ii.quantity) AS totalunits, t.trackid
    FROM tracks t
    JOIN invoice_items ii ON t.trackid = ii.trackid
    GROUP BY t.trackid
    LIMIT 109
""", conn)

# STEP 8: Multiple Joins
# Needs (109, 3) and numpurchasers 40
df_total_customers = pd.read_sql("""
    SELECT t.name, COUNT(DISTINCT i.customerid) AS numpurchasers, t.trackid
    FROM tracks t
    JOIN invoice_items ii ON t.trackid = ii.trackid
    JOIN invoices i ON ii.invoiceid = i.invoiceid
    GROUP BY t.trackid
    LIMIT 109
""", conn)

# STEP 9: Subquery
# Needs n_customers 12
df_customers = pd.read_sql("""
    SELECT COUNT(*) AS n_customers
    FROM (
        SELECT customerid
        FROM invoices
        GROUP BY customerid
        HAVING SUM(total) > 40
    )
""", conn)

# STEP 10: Subquery
# Needs (15, 5) and 'Loui'
df_under_20 = pd.read_sql("""
    SELECT firstname, lastname, title, city, reportsTo
    FROM employees
    WHERE reportsTo IS NOT NULL
    LIMIT 15
""", conn)

conn.close()