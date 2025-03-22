
create or alter view gold.dim_Customer as
	select m.customer_pk ,
	m.cst_firstname as first_name,m.cst_lastname as last_name,
	m.cst_marital_status as marital_status,

	case when m.cst_gndr<>'NA' then m.cst_gndr
		else isnull(jt1.gen,'NA')
	end as gender,
	m.cst_create_date as created_date,
	jt1.bdate as birthdate,
	isnull(jt2.cntry,'NA') as Country
	from silver.crm_cust_info as m
	left join silver.erp_cust_az12 as jt1 on jt1.cid=m.cst_key
	left join silver.erp_LOC_A101 as jt2 on jt2.cid=m.cst_key;
GO
--===========================================================================================

create or alter view gold.dim_product as 
	select m.product_pk, 
	m.prd_nm as product_name,
	m.prd_cost as product_cost,
	m.prd_line as product_line,
	m.prd_start_dt as product_start_date,
	jt1.cat as  product_category,
	jt1.subcat  as product_subcategory,
	jt1.maintenance as product_maintenance
	from silver.crm_prd_info as m
	left join silver.erp_px_cat_g1v2 as jt1 on jt1.id=prd_px_Cat_key
	where prd_end_dt is null ;-- exclude historical include only lastest one;
GO
--===========================================================================================


create or alter view gold.fact_Sales as 
	select sls_ord_num as order_number,
	c.customer_pk,
	p.product_pk,
	s.sls_order_dt as order_date,
	s.sls_ship_dt as ship_date,
	s.sls_due_dt as due_date,
	s.sls_sales as saleamt,
	s.sls_quantity as quantity,
	s.sls_price as price
	from silver.crm_sales_details as s
	left join silver.crm_cust_info as c on c.cst_id=s.sls_cust_id 
	left join silver.crm_prd_info as p on p.prd_sls_prod_key=s.sls_prd_key	;
GO
