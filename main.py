# STEP 0

# SQL Library and Pandas Library
import sqlite3
import pandas as pd

# Connect to the database
conn = sqlite3.connect('data.sqlite')

pd.read_sql("""SELECT * FROM sqlite_master""", conn)

# STEP 1
# Replace None with your code
df_boston =pd.read_sql("""
    SELECT 
        Orders.OrderID, 
        Customers.ContactName, 
        Customers.City
    FROM Orders
    JOIN Customers ON Orders.CustomerID = Customers.CustomerID
    WHERE Customers.City = 'Boston'
""", conn)
# STEP 2
# Replace None with your code
df_zero_emp =pd.read_sql("""
    SELECT 
        Employees.FirstName, 
        Employees.LastName, 
        Orders.OrderID
    FROM Employees
    LEFT JOIN Orders ON Employees.EmployeeID = Orders.EmployeeID
    WHERE Orders.OrderID IS NULL
""", conn)

# STEP 3
# Replace None with your code
df_employee =pd.read_sql("""
    SELECT 
        Customers.CompanyName, 
        Orders.OrderID
    FROM Customers
    LEFT JOIN Orders ON Customers.CustomerID = Orders.CustomerID
    WHERE Orders.OrderID IS NULL
""", conn)

# STEP 4
# Replace None with your code
df_contacts = pd.read_sql("""
    SELECT 
        ContactName, 
        CAST(Amount AS REAL) AS CleanAmount
    FROM Payments
    ORDER BY CleanAmount DESC
""", conn)
# STEP 5
# Replace None with your code
df_payment =pd.read_sql("""
    SELECT 
        CustomerID, 
        SUM(CAST(Amount AS REAL)) AS TotalPaid
    FROM Payments
    GROUP BY CustomerID
    HAVING TotalPaid > 1000
    ORDER BY TotalPaid DESC
""", conn)

# STEP 6
# Replace None with your code
df_credit = pd.read_sql("""
    SELECT 
        Employees.FirstName, 
        Employees.LastName, 
        COUNT(Orders.OrderID) AS OrderCount
    FROM Employees
    JOIN Orders ON Employees.EmployeeID = Orders.EmployeeID
    GROUP BY Employees.EmployeeID
    ORDER BY OrderCount DESC
""", conn)

# STEP 7
# Replace None with your code
df_product_sold = pd.read_sql("""
    SELECT DISTINCT 
        Products.ProductName, 
        Customers.CompanyName
    FROM Products
    JOIN OrderDetails ON Products.ProductID = OrderDetails.ProductID
    JOIN Orders ON OrderDetails.OrderID = Orders.OrderID
    JOIN Customers ON Orders.CustomerID = Customers.CustomerID
    ORDER BY Products.ProductName ASC;
""", conn)

# STEP 8
# Replace None with your code
df_total_customers = pd.read_sql("""
    SELECT 
        Employees.FirstName, 
        Employees.LastName, 
        COUNT(DISTINCT Orders.CustomerID) AS UniqueCustomers
    FROM Employees
    JOIN Orders ON Employees.EmployeeID = Orders.EmployeeID
    GROUP BY Employees.EmployeeID
    ORDER BY UniqueCustomers DESC;
""", conn)

# STEP 9
# Replace None with your code
df_customers = pd.read_sql("""
    SELECT 
        Customers.CompanyName, 
        Orders.OrderID
    FROM Customers
    JOIN Orders ON Customers.CustomerID = Orders.CustomerID
    JOIN OrderDetails ON Orders.OrderID = OrderDetails.OrderID
    WHERE OrderDetails.UnitPrice * OrderDetails.Quantity > (
        SELECT AVG(UnitPrice * Quantity) 
        FROM OrderDetails
    )
""", conn)

# STEP 10
# Replace None with your code
df_under_20 = pd.read_sql("""
    SELECT 
        ProductName, 
        UnitsInStock
    FROM Products
    WHERE ProductID IN (
        SELECT ProductID 
        FROM OrderDetails 
        WHERE Quantity < (SELECT AVG(Quantity) FROM OrderDetails)
    )
""", conn)

conn.close()