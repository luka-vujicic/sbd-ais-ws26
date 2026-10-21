CREATE TABLE orders (
  customer_name TEXT,
  product_category TEXT,
  quantity INTEGER,
  price_per_unit NUMERIC(10, 2),
  order_date DATE,
  country TEXT
);

SELECT * FROM orders ORDER BY price_per_unit DESC LIMIT 1;

SELECT product_category, SUM(quantity) AS total_quantity
FROM orders
GROUP BY product_category
ORDER BY total_quantity DESC
LIMIT 3;

SELECT product_category, SUM(quantity * price_per_unit) AS total_revenue
FROM orders
GROUP BY product_category
ORDER BY total_revenue DESC;

SELECT customer_name, SUM(quantity * price_per_unit) AS total_spending
FROM orders
GROUP BY customer_name
ORDER BY total_spending DESC
LIMIT 5;

SELECT customer_name, COUNT(*), SUM(quantity * price_per_unit) AS total_spending
FROM orders
WHERE customer_name IN (
  SELECT customer_name FROM orders GROUP BY customer_name ORDER BY SUM(quantity * price_per_unit) DESC LIMIT 5
)
GROUP BY customer_name;

SELECT SUM(cnt * cnt) FROM (
  SELECT country, COUNT(*) AS cnt
  FROM people_big
  GROUP BY country
) sub;