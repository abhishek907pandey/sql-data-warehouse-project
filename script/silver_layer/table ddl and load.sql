create or alter proc silver.Usp_DefineTables_Bulkload_files
as
BEGIN
	BEGIN TRY
	declare @strtTime datetime;
	set @strtTime=GETDATE();

		-->> Table >> silver.crm_cust_info
		--DDL

		if OBJECT_ID('silver.crm_cust_info') is not null 
		drop table silver.crm_cust_info;
		

		create table silver.crm_cust_info
		(
			cst_id int primary key ,
			cst_key nvarchar(50) unique,
			cst_firstname nvarchar(50),
			cst_lastname nvarchar(50),
			cst_marital_status nvarchar(50),
			cst_gndr nvarchar(50),
			cst_create_date date,
			dwh_loadDateTime datetime default GETDATE()
		);

		--===============================================================
		-->> Table >> silver.crm_cust_info
		--insert

		with set1 as
		(
		select *,
		ROW_NUMBER() over(partition by cst_id order by cst_create_date desc) as Sr
		from bronze.crm_cust_info
		)

		insert into silver.crm_cust_info
		(cst_id,cst_key,cst_firstname,cst_lastname,cst_marital_status,cst_gndr,cst_create_date)
		select  
		cst_id,
		cst_key,
		UPPER(TRIM(cst_firstname)) AS cst_firstname,
		UPPER(TRIM(cst_lastname)) AS cst_lastname,
		case UPPER(TRIM(cst_marital_status))
			when 'S' then 'Single'
			when 'M' then 'Married'
			else 'NA'
		end as cst_marital_status,
		case UPPER(TRIM(cst_gndr))
			when 'M' then 'Male'
			when 'F' then 'Female'
			when 'T' then 'Trans'
			else 'NA'
		end as cst_gndr,
		cst_create_date

		from set1
		where sr=1 and cst_id is not null --considering only latest date record in case of duplication


		--==============================================================================================================================
		--==============================================================================================================================
		--==============================================================================================================================

		-->> Table >> silver.crm_prd_info
		--DDL

		if OBJECT_ID('silver.crm_prd_info') is not null 
		drop table silver.crm_prd_info;
	

		create table silver.crm_prd_info
		(
			prd_id int primary key,	prd_key nvarchar(50),
			prd_nm nvarchar(50),	prd_cost int not null,
			prd_line nvarchar(50),	prd_start_dt date,
			prd_end_dt date,

			prd_px_Cat_key nvarchar(50),
			prd_sls_prod_key nvarchar(50),
			dwh_loadDateTime datetime default GETDATE()
		)
		;

		--===============================================================
		-->> Table >> silver.crm_cust_info
		--insert

		insert into silver.crm_prd_info(prd_id,prd_key,prd_nm,prd_cost,prd_line,prd_start_dt,prd_end_dt,prd_px_Cat_key,prd_sls_prod_key)
		select
		prd_id,
		UPPER(TRIM(prd_key)) AS prd_key,
		UPPER(TRIM(prd_nm)) AS prd_nm,
		ISNULL(prd_cost,0) AS prd_cost,
		CASE UPPER(TRIM(prd_line))
			when 'M' then 'Mountain'
			when 'R' then 'Road'
			when 'T' then 'Touring'
			when 'S' then 'Other Sales'
		END AS prd_line,
		cast(prd_start_dt as date) as prd_start_dt,
		dateadd(day,-1,cast(LEAD(prd_start_dt) over(partition by prd_key order by prd_start_dt)
		 as date))  as prd_end_dt,
		Replace(left(UPPER(TRIM(prd_key)),5),'-','_') as prd_px_Cat_key,
		SUBSTRING(UPPER(TRIM(prd_key)),7,LEN(UPPER(TRIM(prd_key)))) as prd_sls_prod_key
		from bronze.crm_prd_info

		--==============================================================================================================================
		--==============================================================================================================================
		--==============================================================================================================================

		-->> Table >> silver.crm_sales_details
		--DDL

		if OBJECT_ID('silver.crm_sales_details') is not null 
		drop table silver.crm_sales_details;

		create table silver.crm_sales_details
		(
			sls_ord_num nvarchar(50),
			sls_prd_key nvarchar(50),
			sls_cust_id int,
			sls_order_dt date,
			sls_ship_dt date,
			sls_due_dt date,
			sls_sales int,
			sls_quantity int,
			sls_price int,

			dwh_loadDateTime datetime default GETDATE()
		);

		--===============================================================
		-->> Table >> silver.crm_sales_details
		--insert


		insert into silver.crm_sales_details (sls_ord_num,sls_prd_key,sls_cust_id,sls_order_dt,sls_ship_dt,sls_due_dt,sls_sales,sls_quantity,sls_price
		)
		Select 
		sls_ord_num,
		UPPER(TRIM(sls_prd_key)) as sls_prd_key,
		sls_cust_id,
		CASE 
			WHEN (LEN(sls_order_dt)=8 AND ISNUMERIC(sls_order_dt)=1) THEN CAST(CAST(sls_order_dt AS NVARCHAR(50)) AS DATE)
			ELSE NULL 
		END AS sls_order_dt,
		CASE 
			WHEN (LEN(sls_ship_dt)=8 AND ISNUMERIC(sls_ship_dt)=1) THEN CAST(CAST(sls_ship_dt AS NVARCHAR(50)) AS DATE)
			ELSE NULL 
		END AS sls_ship_dt,
		CASE 
			WHEN (LEN(sls_due_dt)=8 AND ISNUMERIC(sls_due_dt)=1) THEN CAST(CAST(sls_due_dt AS NVARCHAR(50)) AS DATE)
			ELSE NULL 
		END AS sls_due_dt,
		case 
			when sls_sales is null or sls_sales<0 or sls_sales<> abs(sls_price*sls_quantity) then abs(sls_price*isnull(sls_quantity,0))
			else sls_sales
		end as sls_sales,
		isnull(sls_quantity,0) as sls_quantity,
		case 
			when sls_price is null or sls_price<0 or sls_price<> abs(sls_sales/sls_quantity) then abs(sls_sales/sls_quantity) 
			else sls_price 
		end as sls_price
		from bronze.crm_sales_details


		--==============================================================================================================================
		--==============================================================================================================================
		--==============================================================================================================================

		-->> Table >> silver.erp_cust_az12
		--DDL 

		if OBJECT_ID('silver.erp_cust_az12','U') is not null 
			drop table silver.erp_cust_az12

		CREATE TABLE silver.erp_cust_az12 (
			cid    NVARCHAR(50) PRIMARY KEY,
			bdate  DATE,
			gen    NVARCHAR(50),

			dwh_loadDateTime datetime default GETDATE()
		);

		--===============================================================
		-->> Table >> silver.erp_cust_az12
		--insert

		INSERT INTO silver.erp_cust_az12 (cid,bdate,gen)
		SELECT 
		case 
			when left(cid,3)='NAS' then UPPER(RIGHT(cid,len(cid)-3))
			else UPPER(cid)
		end as cid	,
		bdate,
		isnull(gen,'NA') AS gen
		FROM bronze.erp_cust_az12


		--==============================================================================================================================
		--==============================================================================================================================
		--==============================================================================================================================

		-->> Table >> silver.erp_LOC_A101
		--DDL 
		if OBJECT_ID('silver.erp_LOC_A101','U') IS NOT NULL
		DROP TABLE silver.erp_loc_a101;

		CREATE TABLE silver.erp_LOC_A101 (
			cid    NVARCHAR(50) PRIMARY KEY,
			cntry  NVARCHAR(50),

			dwh_loadDateTime datetime default GETDATE()
		)

		;

		--===============================================================
		-->> Table >> silver.erp_loc_a101
		--insert
		INSERT INTO silver.erp_loc_a101(CID,CNTRY)
		SELECT
			REPLACE(CID,'-','') AS CID,
			CNTRY
		FROM bronze.erp_LOC_A101


		--==============================================================================================================================
		--==============================================================================================================================
		--==============================================================================================================================

		-->> Table >> silver.erp_PX_CAT_G1V2
		--DDL 
		IF OBJECT_ID('silver.erp_px_cat_g1v2', 'U') IS NOT NULL
			DROP TABLE silver.erp_px_cat_g1v2;

		CREATE TABLE silver.erp_px_cat_g1v2 (
			id           NVARCHAR(50) primary key,
			cat          NVARCHAR(50),
			subcat       NVARCHAR(50),
			maintenance  NVARCHAR(50),

			dwh_loadDateTime datetime default GETDATE()
		);

		-->> Table >> silver.erp_PX_CAT_G1V2
		--insert
		INSERT INTO silver.erp_PX_CAT_G1V2 (id,cat,subcat,maintenance)
		select
		ID,
		UPPER(trim(CAT)) as CAT,
		UPPER(TRIM(SUBCAT)) AS SUBCAT,
		UPPER(TRIM(MAINTENANCE)) AS MAINTENANCE

		from bronze.erp_PX_CAT_G1V2

	 declare @endTime datetime;
	 set @endTime=GETDATE();

	 print concat('==compleated in ===',datediff(second,@strtTime,@endTime), ' seconds')

	END TRY
	BEGIN CATCH
	    PRINT '!!!Error while loading in table > ' ; 
		PRINT CONCAT('ERROR_MESSAGE > ',ERROR_MESSAGE());
		PRINT CONCAT('ERROR_NUMBER > ',ERROR_NUMBER());
		PRINT CONCAT('ERROR_LINE > ', ERROR_LINE());
		PRINT CONCAT('ERROR_PROCEDURE > ',ERROR_STATE());
		PRINT CONCAT('ERROR_PROCEDURE > ',ERROR_PROCEDURE());
	END CATCH
END
