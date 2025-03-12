/*
Creating user defined store procedure for bulk insert
this is customised abstraction for bulk insert 
This will support DRY principle and provides uniformity
*/

CREATE OR ALTER PROC bronze.Usp_BulkLoad_abstract
@tbl_fullSchemaNm nvarchar(200),
@fullFilePathString nvarchar(1000) --path with file name
AS
BEGIN
	BEGIN TRY
		Declare @sqlStr nvarchar(max),@strtTm datetime,@endTm datetime;

		set @strtTm =GETDATE();
		PRINT 'Truncating and Loading in >> ' + @tbl_fullSchemaNm + ' table'

		set @sqlStr =N'TRUNCATE TABLE ' + @tbl_fullSchemaNm+ '; '+
		' BULK INSERT '+ @tbl_fullSchemaNm + ' FROM '+ ''''+ @fullFilePathString + '''' +
		' WITH(FIRSTROW =2,FIELDTERMINATOR='','',TABLOCK);'
	
		--print @sqlStr
	
		EXEC sp_executesql @sqlStr

		SET @endTm=GETDATE();

		PRINT 'Load completion time ' + CAST(DATEDIFF(second,@strtTm,@endTm) AS NVARCHAR(30)) + ' seconds'
	END TRY
	BEGIN CATCH
	    PRINT '!!!Error while loading in table > ' + @tbl_fullSchemaNm; 
		PRINT CONCAT('ERROR_MESSAGE > ',ERROR_MESSAGE());
		PRINT CONCAT('ERROR_NUMBER > ',ERROR_NUMBER());
		PRINT CONCAT('ERROR_LINE > ', ERROR_LINE());
		PRINT CONCAT('ERROR_PROCEDURE > ',ERROR_STATE());
		PRINT CONCAT('ERROR_PROCEDURE > ',ERROR_PROCEDURE());
	END CATCH
PRINT CONCAT(CHAR(10), '==============')
END
GO
;
