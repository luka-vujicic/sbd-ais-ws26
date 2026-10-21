# Activity 2.1: PostgreSQL Analytical Queries (E-commerce)

**A. Highest Price Per Unit**
* **Result:** Emma Brown | Automotive | 3 | 2000.0 | 2024-10-11 | Italy 

**B. Top 3 Categories by Total Quantity**
* **Result:** Health & Beauty (300,842), Electronics (300,804), Toys (300,598)

**C. Total Revenue Per Category**
* **Result:** Automotive (306,589,798.86), Electronics (241,525,009.45), Home & Garden (78,023,780.09), Sports (61,848,990.83), Health & Beauty (46,599,817.89), Office Supplies (38,276,061.64), Fashion (31,566,368.22), Toys (23,271,039.02), Grocery (15,268,355.66), Books (12,731,976.04)

**D. Top 5 Customers by Total Spending**
* **Result:** Carol Taylor (991,179.18), Nina Lopez (975,444.95), Daniel Jackson (959,344.48), Carol Lewis (947,708.57), Daniel Young (946,030.14)

**E. Spending Totals Observation**
* **Explanation:** Executing a cross-check of order frequency for the top 5 customers reveals that each placed approximately 1,000 orders (e.g., Daniel Jackson with 1,033 orders). The `dataset_generator.py` script intentionally hardcodes a few users to act as massive statistical outliers and bulk buyers, artificially concentrating extreme buying power into a handful of generated customer names.

---

# Activity 2.2: Why Is This Self-Join So Slow?

**Step 1 - Growth Table**
* **50k rows:** 4.23 seconds (27,501,822 pairs)
* **100k rows:** 15.12 seconds (109,946,508 pairs)
* **200k rows:** 81.76 seconds (439,395,606 pairs)
* **Prediction for 1M rows:** The execution time roughly quadruples every time the data volume doubles, indicating a quadratic $O(n^2)$ time complexity. Scaling from 200,000 to 1,000,000 rows requires multiple doublings, meaning the unoptimized query would take well over an hour to compute on the full dataset.

**Step 2 - Index Performance**
Adding an index sped up the initial table read, but it failed to solve the core performance bottleneck. The `EXPLAIN ANALYZE` execution plan demonstrated that the database still had to generate approximately 110 million rows in memory just to compute the Cartesian product for the 100k table.

**Step 3 - Rewrite Query**
* **Result:** The rewritten query leveraging mathematical aggregation ran in **0.801 ms** on the full 1M row table.

**Step 4 - Discussion**
Algorithmic optimization (rewriting the query to use grouped mathematical aggregation) is vastly superior to adding hardware or indexes for inefficient relational self-joins. If the business actually required the literal matched pair rows rather than just the aggregate count, a single-node OLTP database like PostgreSQL would bottleneck entirely on disk I/O and network transfer attempting to process billions of generated rows. An OLAP database or a distributed computing cluster would be explicitly required to handle an operation of that scale.