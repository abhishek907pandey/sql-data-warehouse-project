/*
===============================================================================
Quality Checks
===============================================================================
Script Purpose:
    This script performs quality checks to validate the integrity, consistency, 
    and accuracy of the Gold Layer. These checks ensure:
    - Uniqueness of surrogate keys in dimension tables.
    - Referential integrity between fact and dimension tables.
    - Validation of relationships in the data model for analytical purposes.

Usage Notes:
    - Investigate and resolve any discrepancies found during the checks.
===============================================================================
*/

-- ====================================================================
-- Checking 'gold.dim_customer'
-- ====================================================================
-- Check for Uniqueness of customer_pk in gold.dim_customer
-- Expectation: No results 
SELECT 
     customer_pk, 
    COUNT(*) AS duplicate_count
FROM gold.dim_customer
GROUP BY customer_pk
HAVING COUNT(*) > 1;

-- ====================================================================
-- Checking 'gold.product_key'
-- ====================================================================
-- Check for Uniqueness of product_pk in gold.dim_product
-- Expectation: No results 
SELECT 
    product_pk,
    COUNT(*) AS duplicate_count
FROM gold.dim_product
GROUP BY product_pk
HAVING COUNT(*) > 1;

-- ====================================================================
-- Checking 'gold.fact_sale'
-- ====================================================================
-- Check the data model connectivity between fact and dimensions
-- expectation no records
SELECT * 
FROM gold.fact_sales f
LEFT JOIN gold.dim_customer c
ON c.customer_pk = f.customer_pk
LEFT JOIN gold.dim_product p
ON p.product_pk = f.product_pk
WHERE p.product_pk IS NULL OR c.customer_pk IS NULL  
