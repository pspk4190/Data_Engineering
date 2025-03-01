CREATE TABLE customers (
    cust_id VARCHAR(10) ,
    cust_name VARCHAR(10),
);

INSERT INTO customers (cust_id, cust_name) VALUES
('C1', 'Alice'),
('C2', 'Bob'),
('C3', 'Charlie'),
('C4', 'David'),
('C5', 'Eve'),
('C6', 'Pavan'),
('C7', 'Kiran');

CREATE TABLE transactions (
    cust_id VARCHAR(10),
    trans_amt INT,
    trans_date DATE,
);

INSERT INTO transactions (cust_id, trans_amt, trans_date) VALUES
('C1', 1000, '2024-01-15'),
('C2', 2000, '2024-01-17'),
('C3', 4000, '2024-02-10'),
('C4', 3000, '2024-02-05'),
('C5', 5000, '2024-03-10'),
('C3', 2000, '2024-04-01'),
('C1', 1500, '2024-04-15'),
('C2', 2000, '2024-05-01'),
('C4', 1200, '2024-05-05'),
('C5',1000, '2024-05-10'),
('C3', 900, '2024-05-15'),
('C4', 1000,'2024-06-01'),
('C8', 900, '2024-05-15'),
('C9', 1000,'2024-06-01');


SELECT
C.*,T.*
FROM CUSTOMERS C
INNER JOIN TRANSACTIONS T
ON C.CUST_ID=T.CUST_ID;


SELECT
C.*,T.*
FROM CUSTOMERS C
LEFT JOIN TRANSACTIONS T
ON C.CUST_ID=T.CUST_ID;

SELECT
C.CUST_ID,C.CUST_NAME
FROM CUSTOMERS C
LEFT JOIN TRANSACTIONS T
ON C.CUST_ID=T.CUST_ID
WHERE T.TRANS_AMT IS NULL;

SELECT
C.*,T.*
FROM CUSTOMERS C
RIGHT JOIN TRANSACTIONS T
ON C.CUST_ID=T.CUST_ID;

SELECT
C.CUST_ID,
C.CUST_NAME,
T.CUST_ID,
T.TRANS_AMT
FROM CUSTOMERS C
RIGHT JOIN TRANSACTIONS T
ON C.CUST_ID=T.CUST_ID
WHERE C.CUST_ID IS NULL;

SELECT
C.*,T.*
FROM CUSTOMERS C
FULL OUTER JOIN TRANSACTIONS T
ON C.CUST_ID=T.CUST_ID;

SELECT
C.*,T.*
FROM CUSTOMERS C
CROSS JOIN TRANSACTIONS T