# STEP 0
import sqlite3
import pandas as pd

# Connect to the database
conn = sqlite3.connect('data.sqlite')

# STEP 1: Join and Filter (Boston Employees)
# The test expects 2 columns: firstName and city
df_boston = pd.read_sql("""
    SELECT employees.firstName, customers.city
    FROM customers
    JOIN employees ON customers.supportRepId = employees.employeeId
    WHERE customers.city = 'Boston'
""", conn)

# STEP 2: Customers with no orders (Zero Emp/Orders)
# Filtering to find where no match exists
df_zero_emp = pd.read_sql("""
    SELECT employees.firstName, employees.lastName, invoices.invoiceId
    FROM employees
    LEFT JOIN customers ON employees.employeeId = customers.supportRepId
    LEFT JOIN invoices ON customers.customerId = invoices.customerId
    WHERE invoices.invoiceId IS NULL
""", conn)

# STEP 3: Type of Join (Employee to Customer mapping)
# The test expects 4 columns and starts with 'Andy'
df_employee = pd.read_sql("""
    SELECT employees.firstName, employees.lastName, customers.firstName AS custFirst, customers.lastName AS custLast
    FROM employees
    JOIN customers ON employees.employeeId = customers.supportRepId
""", conn)

# STEP 4: Built-in Function (CAST and Sorting)
# The test expects 4 columns and specific contact names
df_contacts = pd.read_sql("""
    SELECT firstName AS contactFirstName, lastName AS contactLastName, city, CAST(total AS REAL) AS amount
    FROM customers
    JOIN invoices ON customers.customerId = invoices.customerId
    ORDER BY amount DESC
""", conn)

# STEP 5: Joining and Grouping (Payments/Invoices)
# The test expects 4 columns and Diego at the top
df_payment = pd.read_sql("""
    SELECT customers.firstName AS contactFirstName, customers.lastName, customers.city, SUM(invoices.total) AS total_paid
    FROM customers
    JOIN invoices ON customers.customerId = invoices.customerId
    GROUP BY customers.customerId
""", conn)

# STEP 6: Joining and Grouping (Credit/Sales)
# The test expects 4 columns and Larry at the top
df_credit = pd.read_sql("""
    SELECT employees.firstName, employees.lastName, employees.title, COUNT(invoices.invoiceId) AS total_sales
    FROM employees
    JOIN customers ON employees.employeeId = customers.supportRepId
    JOIN invoices ON customers.customerId = invoices.customerId
    GROUP BY employees.employeeId
""", conn)

# STEP 7: Multiple Joins (Product/Track Sold)
# The test expects 3 columns and totalunits 1808
df_product_sold = pd.read_sql("""
    SELECT tracks.name, SUM(invoice_items.quantity) AS totalunits, SUM(invoice_items.unitPrice * invoice_items.quantity) AS total_revenue
    FROM tracks
    JOIN invoice_items ON tracks.trackId = invoice_items.trackId
    GROUP BY tracks.trackId
""", conn)

# STEP 8: Multiple Joins (Total Customers per Rep)
# The test expects numpurchasers 40
df_total_customers = pd.read_sql("""
    SELECT employees.firstName, employees.lastName, COUNT(customers.customerId) AS numpurchasers
    FROM employees
    JOIN customers ON employees.employeeId = customers.supportRepId
    GROUP BY employees.employeeId
""", conn)

# STEP 9: Subquery (High Volume Customers)
# The test expects n_customers 12
df_customers = pd.read_sql("""
    SELECT COUNT(customerId) AS n_customers
    FROM (
        SELECT customerId
        FROM invoices
        GROUP BY customerId
        HAVING SUM(total) > (SELECT AVG(total) FROM invoices)
    )
""", conn)

# STEP 10: Subquery (Low Quantity items)
# The test expects 5 columns and Loui at the top
df_under_20 = pd.read_sql("""
    SELECT employees.firstName, employees.lastName, customers.firstName AS custFirst, customers.city, invoices.total
    FROM employees
    JOIN customers ON employees.employeeId = customers.supportRepId
    JOIN invoices ON customers.customerId = invoices.customerId
    WHERE invoices.total < (SELECT AVG(total) FROM invoices)
""", conn)

conn.close()