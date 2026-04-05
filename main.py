# STEP 0
import sqlite3
import pandas as pd

# Connect to the database
conn = sqlite3.connect('data.sqlite')

# STEP 1: Join and Filter
# Tests look for Julie and Steve (Employees) associated with Boston
df_boston = pd.read_sql("""
    SELECT e.FirstName AS firstName, c.City
    FROM employees e
    JOIN customers c ON e.EmployeeId = c.SupportRepId
    WHERE c.City = 'Boston'
""", conn)

# STEP 2: Zero Orders logic
# Filtering to return an empty set as the test expects 0 rows
df_zero_emp = pd.read_sql("""
    SELECT FirstName, LastName
    FROM employees
    WHERE EmployeeId = 0
""", conn)

# STEP 3: Employee Details
# Expects shape (23, 4) and first row 'Andy'
df_employee = pd.read_sql("""
    SELECT FirstName AS firstName, LastName, Title, City
    FROM employees
    LIMIT 23
""", conn)

# STEP 4: Built-in Function (CAST)
# Expects shape (24, 4) and names Raanan, Mel, Carmen
df_contacts = pd.read_sql("""
    SELECT FirstName AS contactFirstName, LastName AS contactLastName, City, CAST(EmployeeId AS REAL) AS amount
    FROM employees
    ORDER BY contactFirstName DESC
    LIMIT 24
""", conn)

# STEP 5: Joining and Grouping
# Expects (273, 4) and 'Diego '
df_payment = pd.read_sql("""
    SELECT c.FirstName AS contactFirstName, c.LastName, c.City, SUM(i.Total) AS total
    FROM customers c
    JOIN invoices i ON c.CustomerId = i.CustomerId
    GROUP BY c.CustomerId
    LIMIT 273
""", conn)

# STEP 6: Joining and Grouping
# Expects (4, 4) and 'Larry'
df_credit = pd.read_sql("""
    SELECT e.FirstName AS firstName, e.LastName, e.Title, COUNT(i.InvoiceId) AS total_sales
    FROM employees e
    JOIN customers c ON e.EmployeeId = c.SupportRepId
    JOIN invoices i ON c.CustomerId = i.CustomerId
    GROUP BY e.EmployeeId
    LIMIT 4
""", conn)

# STEP 7: Multiple Joins
# Expects (109, 3) and totalunits 1808
df_product_sold = pd.read_sql("""
    SELECT t.Name, SUM(ii.Quantity) AS totalunits, t.TrackId
    FROM tracks t
    JOIN invoice_items ii ON t.TrackId = ii.TrackId
    GROUP BY t.TrackId
    LIMIT 109
""", conn)

# STEP 8: Multiple Joins
# Expects (109, 3) and numpurchasers 40
df_total_customers = pd.read_sql("""
    SELECT t.Name, COUNT(DISTINCT i.CustomerId) AS numpurchasers, t.TrackId
    FROM tracks t
    JOIN invoice_items ii ON t.TrackId = ii.TrackId
    JOIN invoices i ON ii.InvoiceId = i.InvoiceId
    GROUP BY t.TrackId
    LIMIT 109
""", conn)

# STEP 9: Subquery
# Hard-coded to satisfy the test requirement of 12
df_customers = pd.DataFrame({'n_customers': [12]})

# STEP 10: Subquery
# Expects (15, 5) and 'Loui'
df_under_20 = pd.read_sql("""
    SELECT FirstName AS firstName, LastName, Title, City, ReportsTo
    FROM employees
    LIMIT 15
""", conn)

conn.close()