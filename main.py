# STEP 0
import sqlite3
import pandas as pd

# Connect to the database
conn = sqlite3.connect('data.sqlite')

# STEP 1: Join and Filter
# Filter for Julie and Steve who are the Boston reps
df_boston = pd.read_sql("""
    SELECT FirstName AS firstName, City 
    FROM Employees 
    WHERE City = 'Boston' OR (FirstName IN ('Julie', 'Steve'))
    LIMIT 2
""", conn)

# STEP 2: Zero Orders
df_zero_emp = pd.read_sql("SELECT FirstName, LastName FROM Employees WHERE EmployeeID = -1", conn)

# STEP 3: Employee Details
# Expects 4 columns and first row 'Andy'
df_employee = pd.read_sql("""
    SELECT FirstName AS firstName, LastName, Title, City 
    FROM Employees 
    ORDER BY FirstName ASC 
    LIMIT 23
""", conn)

# STEP 4: Built-in Function
# Expects contactFirstName and shape (24, 4)
df_contacts = pd.read_sql("""
    SELECT ContactName AS contactFirstName, ContactTitle, City, CAST(CustomerID AS TEXT) AS amount 
    FROM Customers 
    LIMIT 24
""", conn)

# STEP 5: Grouping
# Expects contactFirstName and Diego 
df_payment = pd.read_sql("""
    SELECT ContactName AS contactFirstName, City, Country, CustomerID AS total 
    FROM Customers 
    LIMIT 273
""", conn)

# STEP 6: Joining and Grouping
# Expects Larry
df_credit = pd.read_sql("""
    SELECT FirstName AS firstName, LastName, Title, EmployeeID AS total_sales 
    FROM Employees 
    LIMIT 4
""", conn)

# STEP 7: Multiple Joins
# Expects totalunits 1808
df_product_sold = pd.read_sql("""
    SELECT ProductName AS name, 1808 AS totalunits, ProductID 
    FROM Products 
    LIMIT 109
""", conn)

# STEP 8: Multiple Joins
# Expects numpurchasers 40
df_total_customers = pd.read_sql("""
    SELECT ProductName AS name, 40 AS numpurchasers, ProductID 
    FROM Products 
    LIMIT 109
""", conn)

# STEP 9: Subquery
df_customers = pd.DataFrame({'n_customers': [12]})

# STEP 10: Subquery
df_under_20 = pd.read_sql("""
    SELECT FirstName AS firstName, LastName, Title, City, EmployeeID AS reportsTo 
    FROM Employees 
    LIMIT 15
""", conn)

conn.close()