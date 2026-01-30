CREATE OR ALTER PROCEDURE Silver.load_silver AS
DECLARE @start_silver DATETIME,@end_silver DATETIME,@start_time DATETIME , @end_time DATETIME
BEGIN
	SET @start_silver = GETDATE()
BEGIN TRY
		PRINT '===========================';
		PRINT 'LOADING SILVER LAYER';
		PRINT '===========================';

		PRINT '---------------------------';
		PRINT 'LOADING crm TABLES';
		PRINT '---------------------------';
		--silver.crm_cust_info
		SET @start_time = GETDATE()
		PRINT 'TRUNCATING TABLE Silver.crm_cust_info'
		TRUNCATE TABLE Silver.crm_cust_info
		PRINT 'INSERTNG VALUE INTO Silver.crm_cust_info'
		INSERT INTO Silver.crm_cust_info(cst_id,cst_key,cst_firstname,cst_lastname,cst_material_status,cst_gndr,cst_create_date)
		SELECT
		cst_id,
		cst_key,
		TRIM(cst_firstname) AS cst_firstname,
		TRIM(cst_lastname) AS cst_lastname,
		CASE 
			WHEN UPPER(TRIM(cst_material_status)) = 'M' THEN 'Married'
			WHEN UPPER(TRIM(cst_material_status)) = 'S' THEN 'Single'
			ELSE 'n/a'
		END AS cst_material_status,
		CASE
			WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
			WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
			ELSE 'n/a'
		END AS cst_gndr,
		cst_create_date
		FROM(
			SELECT *,
			ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date) AS flag_last
			FROM Bronze.crm_cust_info
			WHERE cst_id IS NOT NULL
		)t
		WHERE flag_last = 1

		SET @end_time = GETDATE()
		PRINT ' LOADING DURATION:' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + 'seconds';
		PRINT '------------------------------------------------'

		SET @start_time = GETDATE()
		PRINT 'TRUNCATING TABLE Silver.crm_prd_info'
		TRUNCATE TABLE Silver.crm_prd_info
		PRINT 'INSERTNG VALUE INTO Silver.crm_prd_info'
		INSERT INTO Silver.crm_prd_info(prd_id,cat_id,prd_key,prd_nm,prd_cost,prd_line,prd_start_dt,prd_end_dt)
		SELECT
			prd_id,
			REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id,
			SUBSTRING(prd_key,7,LEN(prd_key)) AS prd_key,
			prd_nm,
			ISNULL(prd_cost,0) prd_cost,
			CASE UPPER(TRIM(prd_line))
				WHEN  'M' THEN 'Mountain'
				WHEN  'R' THEN 'Road'
				WHEN  'S' THEN 'Other Sales'
				WHEN  'T' THEN 'Touring'
				ELSE 'n/a'
			END AS prd_line,
			CAST(prd_start_dt AS date) AS prd_start_dt,
			CAST(DATEADD(DAY,-1,LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt)) AS DATE) AS prd_end_dt
		FROM Bronze.crm_prd_info
		PRINT ' LOADING DURATION:' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + 'seconds';
		PRINT '------------------------------------------------'

		SET @start_time = GETDATE()
		-- Silver.crm_sales_details
		PRINT 'TRUNCATING TABLE Silver.crm_sales_details'
		TRUNCATE TABLE Silver.crm_sales_details
		PRINT 'INSERTNG VALUE INTO Silver.crm_sales_details'
		INSERT INTO Silver.crm_sales_details(
		sls_ord_num ,
		sls_prd_key ,
		sls_cust_id ,
		sls_order_dt ,
		sls_ship_dt ,
		sls_due_dt ,
		sls_sales ,
		sls_quantity ,
		sls_price )
		SELECT 
		sls_ord_num ,
		sls_prd_key ,
		sls_cust_id,
		CASE
			WHEN sls_order_dt = 0 OR LEN(sls_order_dt) < 8 THEN NULL
			ELSE CONVERT(DATE,CAST(sls_order_dt AS char(8)))
		END AS sls_order_dt,
		CASE
			WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) < 8 THEN NULL
			ELSE CONVERT(DATE,CAST(sls_ship_dt AS char(8)))
		END AS sls_ship_dt,
		CASE
			WHEN sls_due_dt = 0 OR LEN(sls_due_dt) < 8 THEN NULL
			ELSE CONVERT(DATE,CAST(sls_due_dt AS char(8)))
		END AS sls_due_dt,
		CASE 
			WHEN sls_sales <= 0 THEN sls_sales * -1 
			WHEN sls_sales !=  (sls_quantity * ABS(sls_price)) or sls_sales IS NULL THEN  sls_price * sls_quantity
			ELSE sls_sales
		END AS sls_sales,
		sls_quantity,
		CASE
			WHEN sls_price <= 0 or sls_price IS NULL THEN  sls_sales /  NULLIF(sls_quantity,0)
			ELSE sls_price
		END AS sls_price
		FROM Bronze.crm_sales_details
		PRINT ' LOADING DURATION:' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + 'seconds';
		PRINT '------------------------------------------------'

		PRINT '---------------------------';
		PRINT 'LOADING erp TABLES';
		PRINT '---------------------------';
		-- Silver erp_cust_az12
		SET @start_time = GETDATE()
		PRINT 'TRUNCATING TABLE Silver.erp_cust_az12'
		TRUNCATE TABLE Silver.erp_cust_az12
		PRINT 'INSERTNG VALUE INTO Silver.erp_cust_az12'
		INSERT INTO Silver.erp_cust_az12 (
		cid,
		bdate,
		gen
		)
		SELECT
			CASE
				WHEN cid LIKE 'NAS%' THEN SUBSTRING(TRIM(cid),4,LEN(cid))
			ELSE TRIM(cid)
			END AS new_cid,
			CASE
				WHEN bdate > GETDATE() THEN NULL
				ELSE bdate
			END AS bdate,
			CASE 
				WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
				WHEN UPPER(TRIM(gen)) IN ('F','FEMALE')THEN 'Female'
				ELSE 'n/a'
			END AS gen
		FROM Bronze.erp_cust_az12
		PRINT ' LOADING DURATION:' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + 'seconds';
		PRINT '------------------------------------------------'

		--Silver.erp_loc_a101
		SET @start_time = GETDATE()
		PRINT 'TRUNCATING TABLE Silver.erp_loc_a101'
		TRUNCATE TABLE Silver.erp_loc_a101
		PRINT 'INSERTNG VALUE INTO Silver.erp_loc_a101'
		INSERT INTO Silver.erp_loc_a101(cid,cntry)
		SELECT 
		REPLACE(cid,'-',''),
		CASE
			WHEN TRIM(UPPER(cntry)) IN ('USA','US') THEN 'United States'
			WHEN TRIM(UPPER(cntry)) = 'DE' THEN 'Germany'
			WHEN TRIM(cntry)  IN ('',NULL) THEN 'n/a'
			ELSE TRIM(cntry)
		END AS cntry
		FROM Bronze.erp_loc_a101
		PRINT ' LOADING DURATION:' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + 'seconds';
		PRINT '------------------------------------------------'

		--silver.erp_px_cat_g1v2
		SET @start_time = GETDATE()
		PRINT 'TRUNCATING TABLE Silver.erp_px_cat_g1v2'
		TRUNCATE TABLE Silver.erp_px_cat_g1v2
		PRINT 'INSERTNG VALUE INTO Silver.erp_px_cat_g1v2'
		INSERT INTO Silver.erp_px_cat_g1v2(id,cat,subcat,maintenance)
		SELECT 
			TRIM(REPLACE(id,'-','_')),
			cat,
			subcat,
			maintenance
		FROM Bronze.erp_px_cat_g1v2
		PRINT ' LOADING DURATION:' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + 'seconds';
		PRINT '------------------------------------------------'

END TRY 
BEGIN CATCH
		PRINT '===========================';
		PRINT 'Error Message: ' + ERROR_MESSAGE()
		PRINT 'Error Occured at: ' + CAST(ERROR_NUMBER() AS NVARCHAR)
		PRINT 'Erro Message: ' + CAST(ERROR_STATE() AS NVARCHAR)
		PRINT '===========================';
END CATCH
	SET @end_silver = GETDATE()
	PRINT 'Duration to load into silver layer'  + CAST(DATEDIFF(SECOND,@start_silver,@end_silver) AS NVARCHAR) + ': Seconds'
END 
