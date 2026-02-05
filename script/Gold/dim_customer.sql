-- finaly let us create the final layer of the DataWareHouse which is the Gold layer
CREATE VIEW gold.dim_customer AS
SELECT 
	ROW_NUMBER() OVER(ORDER BY ci.cst_id ) AS customer_key,--surrogate key
	ci.cst_id AS customer_id,
	ci.cst_key AS customer_number,
	ci.cst_firstname AS first_name,
	ci.cst_lastname AS last_name,
	cl.cntry AS country,
	cd.bdate AS birth_date,
	ci.cst_material_status AS marital_status,
	CASE 
	WHEN ci.cst_gndr !=  'n/a' THEN ci.cst_gndr
		ELSE COALESCE(cd.gen,'n/a')
	END AS gender,
	ci.cst_create_date AS create_date
FROM Silver.crm_cust_info AS ci
LEFT JOIN Silver.erp_cust_az12 AS cd
ON ci.cst_key = cd.cid
LEFT JOIN Silver.erp_loc_a101 AS cl
ON ci.cst_key = cl.cid
