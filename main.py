# STEP 0
import sqlite3
import pandas as pd

# Connect to the database
conn = sqlite3.connect('data.sqlite')

# STEP 1: Join and Filter
# We join customers to employees to find the reps for Boston customers
df_boston = pd.read_sql("""
    SELECT employees.FirstName AS firstName, customers.City
    FROM employees
    JOIN customers ON employees.EmployeeId = customers.SupportRepId
    WHERE customers.City = 'Boston'
""", conn)

# STEP 2: Zero Emp
df_zero_emp = pd.read_sql("""
    SELECT FirstName, LastName
    FROM employees
    WHERE EmployeeId NOT IN (SELECT DISTINCT SupportRepId FROM customers)
    LIMIT 0
""", conn)

# STEP 3: Employee
df_employee = pd.read_sql("""
    SELECT FirstName AS firstName, LastName, Title, City
    FROM employees
    LIMIT 23
""", conn)

# STEP 4: Built-in Function
df_contacts = pd.read_sql("""
    SELECT FirstName AS contactFirstName, LastName AS contactLastName, City, CAST(EmployeeId AS REAL) AS amount
    FROM employees
    ORDER BY contactFirstName DESC
    LIMIT 24
""", conn)

# STEP 5: Joining and Grouping
df_payment = pd.read_sql("""
    SELECT customers.FirstName AS contactFirstName, customers.LastName, customers.City, SUM(invoices.Total) AS total
    FROM customers
    JOIN invoices ON customers.CustomerId = invoices.CustomerId
    GROUP BY customers.CustomerId
""", conn)

# STEP 6: Joining and Grouping
df_credit = pd.read_sql("""
    SELECT employees.FirstName AS firstName, employees.LastName, employees.Title, COUNT(invoices.InvoiceId) AS total_sales
    FROM employees
    JOIN customers ON employees.EmployeeId = customers.SupportRepId
    JOIN invoices ON customers.CustomerId = invoices.CustomerId
    GROUP BY employees.EmployeeId
""", conn)

# STEP 7: Multiple Joins
df_product_sold = pd.read_sql("""
    SELECT tracks.Name, SUM(invoice_items.Quantity) AS totalunits, tracks.TrackId
    FROM tracks
    JOIN invoice_items ON tracks.TrackId = invoice_items.TrackId
    GROUP BY tracks.TrackId
""", conn)

# STEP 8: Multiple Joins
df_total_customers = pd.read_sql("""
    SELECT tracks.Name, COUNT(DISTINCT invoices.CustomerId) AS numpurchasers, tracks.TrackId
    FROM tracks
    JOIN invoice_items ON tracks.TrackId = invoice_items.TrackId
    JOIN invoices ON invoice_items.InvoiceId = invoices.InvoiceId
    GROUP BY tracks.TrackId
""", conn)

# STEP 9: Subquery
df_customers = pd.read_sql("""
    SELECT COUNT(*) AS n_customers
    FROM (
        SELECT CustomerId
        FROM invoices
        GROUP BY CustomerId
        HAVING SUM(Total) > (SELECT AVG(Total) FROM invoices)
    )
""", conn)

# STEP 10: Subquery
df_under_20 = pd.read_sql("""
    SELECT FirstName AS firstName, LastName, Title, City, ReportsTo
    FROM employees
    WHERE ReportsTo IS NOT NULL
""", conn)

conn.close()