/*
Full loading of source files data into respective tables using bronze.Usp_BulkLoad_abstract 
*/

create or alter proc bronze.Usp_Bulkload_files
as
begin
	EXEC bronze.Usp_BulkLoad_abstract 'bronze.crm_cust_info','D:\sql-data-warehouse-project\datasets\source_crm\cust_info.csv';
	EXEC bronze.Usp_BulkLoad_abstract 'bronze.crm_prd_info','D:\sql-data-warehouse-project\datasets\source_crm\prd_info.csv';
	EXEC bronze.Usp_BulkLoad_abstract 'bronze.crm_sales_details','D:\sql-data-warehouse-project\datasets\source_crm\sales_details.csv';

	EXEC bronze.Usp_BulkLoad_abstract 'bronze.erp_CUST_AZ12','D:\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv';
	EXEC bronze.Usp_BulkLoad_abstract 'bronze.erp_LOC_A101','D:\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv';
	EXEC bronze.Usp_BulkLoad_abstract 'bronze.erp_PX_CAT_G1V2','D:\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv';
end
GO
;

--=====================================
exec bronze.Usp_Bulkload_files;
