# activity 2.1

*create table statement:*
CREATE TABLE orders (
  customer_name TEXT,
  product_category TEXT,
  quantity INTEGER,
  price_per_unit NUMERIC(10, 2),
  order_date DATE,
  country TEXT
);

*a. highest price:*
SELECT * FROM orders ORDER BY price_per_unit DESC LIMIT 1;

*result:* Emma Brown | Automotive | 3 | 2000.0 | 2024-10-11 | Italy 

*b. top 3 categories by quantity:*
SELECT product_category, SUM(quantity) AS total_quantity
FROM orders
GROUP BY product_category
ORDER BY total_quantity DESC
LIMIT 3;

*result:* Health & Beauty (300842), Electronics (300804), Toys (300598)

**c. total revenue per category:**
SELECT product_category, SUM(quantity * price_per_unit) AS total_revenue
FROM orders
GROUP BY product_category
ORDER BY total_revenue DESC;

*result:* Automotive (306589798.86), Electronics (241525009.45), Home & Garden (78023780.09), Sports (61848990.83), 
Health & Beauty (46599817.89), Office Supplies (38276061.64), Fashion (31566368.22), Toys (23271039.02), Grocery (15268355.66), Books (12731976.04)

**d. top 5 customers:**
SELECT customer_name, SUM(quantity * price_per_unit) AS total_spending
FROM orders
GROUP BY customer_name
ORDER BY total_spending DESC
LIMIT 5;

*result:* Carol Taylor (991179.18), Nina Lopez (975444.95), Daniel Jackson (959344.48), Carol Lewis (947708.57), Daniel Young (946030.14)

**e. spending totals observation:**
SELECT customer_name, COUNT(*), SUM(quantity * price_per_unit) AS total_spending
FROM orders
WHERE customer_name IN (
  SELECT customer_name FROM orders GROUP BY customer_name ORDER BY SUM(quantity * price_per_unit) DESC LIMIT 5
)
GROUP BY customer_name;


*explanation:* The query shows that each of these top 5 customers placed approximately 1,000 orders 
each (e.g., Daniel Jackson with 1033 orders). dataset_generator.py intentionally hardcodes a few users to act 
as massive statistical outliers and bulk buyers, concentrating extreme buying power into a handful of generated names.

# activity 2.2

**step 1 - growth table:**
- 50k rows: 4.23 seconds, 27,501,822 pairs
- 100k rows: 15.12 seconds, 109,946,508 pairs
- 200k rows: 81.76 seconds, 439,395,606 pairs

*prediction for 1m rows:* The time roughly quadruples when data doubles (O(n^2) complexity). 
Since 1M rows requires multiple doublings from 200k, it would take well over an hour to compute.

**step 2 - index performance:**
Adding an index sped up the initial table read, but did not solve the core issue. 
The execution plan showed the database still had to generate ~110 million rows in memory to compute the Cartesian product.

**step 3 - rewrite query:**
SELECT SUM(cnt * cnt) FROM (
  SELECT country, COUNT(*) AS cnt
  FROM people_big
  GROUP BY country
) sub;

*result:* This ran in 0.801 ms on the 1M row table.

**step 4 - discussion:**
Algorithmic optimization (rewriting the query) is vastly superior to adding hardware or indexes for inefficient self-joins. 
If the business actually needed the literal pair rows rather than just the count, 
a single-node OLTP database like PostgreSQL would bottleneck on disk I/O and network transfer. 
An OLAP database or a distributed cluster would be required for that scale.




