USE classicmodel;

SHOW TABLE STATUS;

-- show the tables in the database
SHOW TABLES;

-- 1. Retrieve all the records from the "customers" table.
SELECT * FROM customers;

-- 2. Select the order number, order date, and status of all orders with the status “In Process”.
SELECT orderNumber, orderDate, status FROM orders WHERE status = 'In Process';

-- 3. Count the number of products in the “products” table.
SELECT COUNT(*) FROM products;

-- 4. Select the maximum, minimum, and average credit limit of all customers.
SELECT MAX(creditLimit) AS MaxCreditLimit, MIN(creditLimit) AS MinCreditLimit, AVG(creditLimit) AS AvgCreditLimit FROM customers;

-- 5. Select the customer name, order number, and total cost of the order for all orders.
SELECT customers.customerName, orderdetails.orderNumber, SUM(orderdetails.priceEach * orderdetails.quantityOrdered) as TotalCost 
FROM customers 
JOIN orders ON customers.customerNumber = orders.customerNumber 
JOIN orderdetails ON orders.orderNumber = orderdetails.orderNumber 
GROUP BY orderdetails.orderNumber;


-- 6. Retrieve the name of customers who made more than 5 orders.
SELECT customerName FROM customers 
WHERE customerNumber IN 
(SELECT customerNumber FROM orders GROUP BY customerNumber HAVING COUNT(*) > 5);

-- 7. Retrieve the orders of customers with more than 200,000 in credit limit (customer.creditLimit)
SELECT * FROM orders 
WHERE customerNumber 
IN (SELECT customerNumber FROM customers WHERE creditLimit > 200000);


-- 8. Return the order numbers of products with at least 8,000 items in stock (products.quantityInStock)
SELECT DISTINCT od.orderNumber
FROM orderdetails od
JOIN products p ON od.productCode = p.productCode
WHERE p.quantityInStock > 8000;


-- 4. Query Analysis: Explain Analyze query (8) 
EXPLAIN ANALYZE SELECT DISTINCT od.orderNumber
FROM orderdetails od
JOIN products p ON od.productCode = p.productCode
WHERE p.quantityInStock > 8000;

