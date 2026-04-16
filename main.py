# STEP 0

# SQL Library and Pandas Library
import sqlite3
import pandas as pd

# Connect to the database
conn = sqlite3.connect('data.sqlite')

pd.read_sql("""SELECT * FROM sqlite_master""", conn)

# STEP 1
# Replace None with your code
df_boston = pd.read_sql("""SELECT firstName, lastName 
                         FROM employees 
                         JOIN offices ON employees.officeCode = offices.officeCode 
                         WHERE offices.city = 'Boston'""", conn)

# STEP 2
# Replace None with your code
df_zero_emp = pd.read_sql("""SELECT offices.officeCode, offices.city 
                           FROM offices 
                           LEFT JOIN employees ON offices.officeCode = employees.officeCode 
                           WHERE employees.employeeNumber IS NULL""", conn)

# STEP 3
# Replace None with your code
df_employee = pd.read_sql("""SELECT firstName, lastName, offices.city, offices.state 
                         FROM employees 
                         LEFT JOIN offices ON employees.officeCode = offices.officeCode 
                         ORDER BY firstName, lastName""", conn)

# STEP 4
# Replace None with your code
df_contacts = pd.read_sql("""SELECT contactFirstName, contactLastName, phone, salesRepEmployeeNumber 
                         FROM customers 
                         LEFT JOIN orders ON customers.customerNumber = orders.customerNumber 
                         WHERE orders.orderNumber IS NULL 
                         ORDER BY contactLastName""", conn)

# STEP 5
# Replace None with your code
df_payment = pd.read_sql("""SELECT contactFirstName, contactLastName, amount, paymentDate 
                         FROM customers 
                         JOIN payments ON customers.customerNumber = payments.customerNumber 
                         ORDER BY CAST(amount AS REAL) DESC""", conn)

# STEP 6
# Replace None with your code
df_credit = pd.read_sql("""SELECT employees.employeeNumber, firstName, lastName, COUNT(customerNumber) AS num_customers 
                        FROM employees 
                        JOIN customers ON employees.employeeNumber = customers.salesRepEmployeeNumber 
                        GROUP BY employees.employeeNumber 
                        HAVING AVG(creditLimit) > 90000 
                        ORDER BY num_customers DESC""", conn)

# STEP 7
# Replace None with your code
df_product_sold = pd.read_sql("""SELECT productName, COUNT(orderNumber) AS numorders, SUM(quantityOrdered) AS totalunits 
                             FROM products 
                             JOIN orderdetails ON products.productCode = orderdetails.productCode 
                             GROUP BY products.productCode 
                             ORDER BY totalunits DESC""", conn)

# STEP 8
# Replace None with your code
df_total_customers = pd.read_sql("""SELECT productName, products.productCode, COUNT(DISTINCT customerNumber) AS numpurchasers 
                                FROM products 
                                JOIN orderdetails ON products.productCode = orderdetails.productCode 
                                JOIN orders ON orderdetails.orderNumber = orders.orderNumber 
                                GROUP BY products.productCode 
                                ORDER BY numpurchasers DESC""", conn)

# STEP 9
# Replace None with your code
df_customers = pd.read_sql("""SELECT COUNT(customerNumber) AS n_customers, offices.officeCode, offices.city 
                           FROM offices 
                           JOIN employees ON offices.officeCode = employees.officeCode 
                           JOIN customers ON employees.employeeNumber = customers.salesRepEmployeeNumber 
                           GROUP BY offices.officeCode""", conn)

# STEP 10
# Replace None with your code
df_under_20 = pd.read_sql("""SELECT DISTINCT employees.employeeNumber, firstName, lastName, offices.city, offices.officeCode 
                           FROM employees 
                           JOIN offices ON employees.officeCode = offices.officeCode 
                           JOIN customers ON employees.employeeNumber = customers.salesRepEmployeeNumber 
                           JOIN orders ON customers.customerNumber = orders.customerNumber 
                           JOIN orderdetails ON orders.orderNumber = orderdetails.orderNumber 
                           WHERE productCode IN (
                               SELECT productCode 
                               FROM orderdetails 
                               JOIN orders ON orderdetails.orderNumber = orders.orderNumber 
                               GROUP BY productCode 
                               HAVING COUNT(DISTINCT customerNumber) < 20
                           )
                           ORDER BY lastName, firstName DESC""", conn)

conn.close()