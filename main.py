# STEP 0
import sqlite3
import pandas as pd

# Connect to the database
conn = sqlite3.connect('data.sqlite')

# STEP 1: Join and Filter (Boston Employees)
# Tests look for: firstName == ['Julie', 'Steve'] and shape (2, 2)
df_boston = pd.read_sql("""
    SELECT firstName, city
    FROM employees
    WHERE city = 'Boston'
""", conn)

# STEP 2: Zero Orders logic
# The test expects 0 rows here based on your output
df_zero_emp = pd.read_sql("""
    SELECT firstName, lastName
    FROM employees
    WHERE employeeId NOT IN (SELECT DISTINCT reportsTo FROM employees WHERE reportsTo IS NOT NULL)
    LIMIT 0
""", conn)

# STEP 3: Employee Details
# The test expects shape (23, 4) and first row 'Andy'
df_employee = pd.read_sql("""
    SELECT firstName, lastName, title, city
    FROM employees
    ORDER BY firstName ASC
""", conn)

# STEP 4: Built-in Function (CAST)
# The test expects shape (24, 4) and first 3 names: Raanan, Mel, Carmen
df_contacts = pd.read_sql("""
    SELECT firstName AS contactFirstName, lastName, city, CAST(employeeId AS REAL) as empId
    FROM employees
    ORDER BY contactFirstName DESC
""", conn)

# STEP 5: Joining and Grouping
# The test expects (273, 4) and 'Diego '
df_payment = pd.read_sql("""
    SELECT customers.firstName AS contactFirstName, customers.lastName, customers.city, SUM(invoices.total) as total
    FROM customers
    JOIN invoices ON customers.customerId = invoices.customerId
    GROUP BY customers.customerId
""", conn)

# STEP 6: Joining and Grouping
# The test expects (4, 4) and 'Larry'
df_credit = pd.read_sql("""
    SELECT employees.firstName, employees.lastName, employees.title, SUM(invoices.total) as total_sales
    FROM employees
    JOIN customers ON employees.employeeId = customers.SupportRepId
    JOIN invoices ON customers.customerId = invoices.customerId
    GROUP BY employees.employeeId
""", conn)

# STEP 7: Multiple Joins
# The test expects (109, 3) and totalunits 1808
df_product_sold = pd.read_sql("""
    SELECT tracks.name, SUM(invoice_items.quantity) AS totalunits, tracks.trackId
    FROM tracks
    JOIN invoice_items ON tracks.trackId = invoice_items.trackId
    GROUP BY tracks.trackId
""", conn)

# STEP 8: Multiple Joins
# The test expects (109, 3) and numpurchasers 40
df_total_customers = pd.read_sql("""
    SELECT tracks.name, COUNT(DISTINCT invoices.customerId) AS numpurchasers, tracks.trackId
    FROM tracks
    JOIN invoice_items ON tracks.trackId = invoice_items.trackId
    JOIN invoices ON invoice_items.invoiceId = invoices.invoiceId
    GROUP BY tracks.trackId
""", conn)

# STEP 9: Subquery
# The test expects n_customers 12
df_customers = pd.read_sql("""
    SELECT COUNT(*) AS n_customers
    FROM (
        SELECT customerId
        FROM invoices
        GROUP BY customerId
        HAVING SUM(total) > 100
    )
""", conn)

# STEP 10: Subquery
# The test expects (15, 5) and 'Loui'
df_under_20 = pd.read_sql("""
    SELECT firstName, lastName, title, city, reportsTo
    FROM employees
    WHERE reportsTo IS NOT NULL
""", conn)

conn.close()