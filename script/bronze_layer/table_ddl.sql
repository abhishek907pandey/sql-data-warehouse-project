/*
creates table from CRM and ERP sources
*/


create table bronze.crm_cust_info
(
	cst_id int not null,
	cst_key nvarchar(50),
	cst_firstname nvarchar(50),
	cst_lastname nvarchar(50),
	cst_marital_status nvarchar(50),
	cst_gndr nvarchar(50),
	cst_create_date date
);

create table bronze.crm_prd_info
(
	prd_id int not null,
	prd_key nvarchar(50),
	prd_nm nvarchar(50),
	prd_cost int,
	prd_line nvarchar(50),
	prd_start_dt date,
	prd_end_dt date
);

create table bronze.crm_sales_details
(
	sls_ord_num nvarchar(50),
	sls_prd_key nvarchar(50),
	sls_cust_id int,
	sls_order_dt int,
	sls_ship_dt int,
	sls_due_dt int,
	sls_sales int,
	sls_quantity int,
	sls_price int
);

create table bronze.erp_CUST_AZ12
(
	CID nvarchar(50),
	BDATE date,
	GEN nvarchar(50)
);

create table bronze.erp_LOC_A101
(
	CID nvarchar(50),
	CNTRY nvarchar(50)
);


create table bronze.erp_PX_CAT_G1V2
(
	ID nvarchar(50),
	CAT nvarchar(50),
	SUBCAT nvarchar(50),
	MAINTENANCE nvarchar(50)
);

