CREATE OR ALTER PROCEDURE Bronze.Load_bronze AS
BEGIN
DECLARE @start_bronze DATETIME, @end_bronze DATETIME;
DECLARE @start_time DATETIME, @end_time DATETIME;
SET @start_bronze = GETDATE();
	BEGIN TRY
		-- source = crm
		PRINT '===========================';
		PRINT 'LOADING BRONZE LAYER';
		PRINT '===========================';

		PRINT '---------------------------';
		PRINT 'LOADING crm TABLES';
		PRINT '---------------------------';

		SET @start_time = GETDATE();
		PRINT '>>TRUNCATING TABLE: Bronze.crm_cust_info'
		TRUNCATE TABLE Bronze.crm_cust_info
		PRINT '>>INSERTING DATA INTO:  Bronze.crm_cust_info'
		BULK INSERT Bronze.crm_cust_info
		FROM 'C:\Users\Kirubel\Downloads\sql-data-warehouse-project-main\sql-data-warehouse-project-main\datasets\source_crm/cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT ' LOADING DURATION:' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + 'seconds';
		PRINT '------------------------------------------------'

		SET @start_time = GETDATE()
		PRINT '>>TRUNCATING TABLE: Bronze.crm_prd_info';
		TRUNCATE TABLE Bronze.crm_prd_info
		PRINT '>>INSERTING DATA INTO:  Bronze.crm_prd_info';
		BULK INSERT Bronze.crm_prd_info
		FROM 'C:\Users\Kirubel\Downloads\sql-data-warehouse-project-main\sql-data-warehouse-project-main\datasets\source_crm/prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT ' LOADING DURATION:' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + 'seconds';
		PRINT '------------------------------------------------'

		SET @start_time = GETDATE()
		PRINT '>>TRUNCATING TABLE: Bronze.crm_sales_details';
		TRUNCATE TABLE Bronze.crm_sales_details
		PRINT '>>INSERTING DATA INTO: Bronze.crm_sales_details';
		BULK INSERT Bronze.crm_sales_details
		FROM 'C:\Users\Kirubel\Downloads\sql-data-warehouse-project-main\sql-data-warehouse-project-main\datasets\source_crm/sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT ' LOADING DURATION:' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + 'seconds';
		PRINT '------------------------------------------------'

		-- source = erp
		PRINT '---------------------------';
		PRINT 'LOADING erp TABLES';
		PRINT '---------------------------';
		SET @start_time = GETDATE()
		PRINT '>>TRUNCATING TABLE:Bronze.erp_cust_az12';
		TRUNCATE TABLE Bronze.erp_cust_az12
		PRINT '>>INSERTING DATA INTO: Bronze.erp_cust_az12';
		BULK INSERT Bronze.erp_cust_az12
		FROM 'C:\Users\Kirubel\Downloads\sql-data-warehouse-project-main\sql-data-warehouse-project-main\datasets\source_erp\CUST_AZ12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT ' LOADING DURATION:' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + 'seconds';
		PRINT '------------------------------------------------'

		SET @start_time = GETDATE()
		PRINT '>>TRUNCATING TABLE:Bronze.erp_loc_a101';
		TRUNCATE TABLE Bronze.erp_loc_a101
		PRINT '>>INSERTING DATA INTO: Bronze.erp_loc_a101';
		BULK INSERT Bronze.erp_loc_a101
		FROM 'C:\Users\Kirubel\Downloads\sql-data-warehouse-project-main\sql-data-warehouse-project-main\datasets\source_erp\LOC_A101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT ' LOADING DURATION:' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + 'seconds';
		PRINT '------------------------------------------------'

		SET @start_time = GETDATE()
		PRINT '>>TRUNCATING TABLE:Bronze.erp_px_cat_g1v2';
		TRUNCATE TABLE Bronze.erp_px_cat_g1v2
		PRINT '>>INSERTING DATA INTO: Bronze.erp_px_cat_g1v2';
		BULK INSERT Bronze.erp_px_cat_g1v2
		FROM 'C:\Users\Kirubel\Downloads\sql-data-warehouse-project-main\sql-data-warehouse-project-main\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT ' LOADING DURATION:' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + 'seconds';
		PRINT '------------------------------------------------'
	END TRY 
	BEGIN CATCH 
		PRINT '===========================';
		PRINT 'Error occured during loading into bronze layer';
		PRINT 'Error Message:' + ERROR_MESSAGE();
		PRINT 'Error line Number:' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error message:' + CAST(ERROR_STATE() AS NVARCHAR);
		PRINT '===========================';
	END CATCH
SET @end_bronze = GETDATE();
PRINT 'Duration to load into bronze layer: ' + CAST(DATEDIFF(SECOND,@start_bronze,@end_bronze) AS NVARCHAR) + 'seconds'
END
