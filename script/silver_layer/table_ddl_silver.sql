create or alter proc silver.Usp_DefineTables AS
BEGIN
--=========>> Table >> silver.crm_cust_info

if OBJECT_ID('silver.crm_cust_info') is not null 
drop table silver.crm_cust_info;
		

create table silver.crm_cust_info
(
	customer_pk int identity(1,1) primary key,
	cst_id int unique not null,
	cst_key nvarchar(50) unique,
	cst_firstname nvarchar(50),
	cst_lastname nvarchar(50),
	cst_marital_status nvarchar(50),
	cst_gndr nvarchar(50),
	cst_create_date date,
	dwh_loadDateTime datetime default GETDATE()
);

--=========>> Table >>  silver.crm_prd_info

if OBJECT_ID('silver.crm_prd_info') is not null 
drop table silver.crm_prd_info;


create table silver.crm_prd_info
(   
	product_pk int identity(1,1) primary key,
	prd_id int unique,	
	prd_key nvarchar(50),
	prd_nm nvarchar(50),
	prd_cost int not null,
	prd_line nvarchar(50),
	prd_start_dt date,
	prd_end_dt date,

	prd_px_Cat_key nvarchar(50) ,
	prd_sls_prod_key nvarchar(50) UNIQUE ,
	dwh_loadDateTime datetime default GETDATE()
);

--=========>> Table >>  silver.crm_sales_details

if OBJECT_ID('silver.crm_sales_details') is not null 
drop table silver.crm_sales_details;


create table silver.crm_sales_details
(
	sls_ord_num nvarchar(50),
	product_pk int,  -- ref of product_key in silver.crm_prd_info
	sls_cust_id int,
	sls_order_dt date,
	sls_ship_dt date,
	sls_due_dt date,
	sls_sales int,
	sls_quantity int,
	sls_price int,

	dwh_loadDateTime datetime default GETDATE()
);

--=========>> Table >> silver.erp_cust_az12
if OBJECT_ID('silver.erp_cust_az12','U') is not null 
	drop table silver.erp_cust_az12

CREATE TABLE silver.erp_cust_az12 (
	cid    NVARCHAR(50) PRIMARY KEY,
	bdate  DATE,
	gen    NVARCHAR(50),

	dwh_loadDateTime datetime default GETDATE()
);

--=========>> Table >> silver.erp_LOC_A101
if OBJECT_ID('silver.erp_LOC_A101','U') IS NOT NULL
DROP TABLE silver.erp_loc_a101;

CREATE TABLE silver.erp_LOC_A101 (
	cid    NVARCHAR(50) PRIMARY KEY,
	cntry  NVARCHAR(50),

	dwh_loadDateTime datetime default GETDATE()
)	;

--=========>> Table >> silver.erp_PX_CAT_G1V2
IF OBJECT_ID('silver.erp_px_cat_g1v2', 'U') IS NOT NULL
	DROP TABLE silver.erp_px_cat_g1v2;

CREATE TABLE silver.erp_px_cat_g1v2 (
	id           NVARCHAR(50) primary key,
	cat          NVARCHAR(50),
	subcat       NVARCHAR(50),
	maintenance  NVARCHAR(50),

	dwh_loadDateTime datetime default GETDATE()
);

END

