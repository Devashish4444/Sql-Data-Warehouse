CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME, @start_time_bronze DATETIME, @end_time_bronze DATETIME;
    BEGIN TRY 
        PRINT 'Loading Bronze Layer'
        PRINT '============================'

        PRINT '----------------------------'
        PRINT 'Loading crm tables'
        PRINT '----------------------------'

    SET @start_time_bronze = GETDATE();

        SET @start_time = GETDATE();
        PRINT'TABLE : bronze.crm_cust_info'
        TRUNCATE TABLE bronze.crm_cust_info;
        BULK INSERT bronze.crm_cust_info 
        from '/mnt/data/cust_info.csv'
        WITH (
            FIRSTROW = 2 , 
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: '+ CAST(DATEDIFF(second, @start_time, @end_time) as NVARCHAR) + 'seconds'

        PRINT'----------------------------'
        SET @start_time = GETDATE();
        PRINT'TABLE : bronze.crm_prd_info'
        TRUNCATE TABLE bronze.crm_prd_info;
        BULK INSERT bronze.crm_prd_info
        from '/mnt/data/sql-ultimate-course/source_crm/prd_info.csv'
        WITH (
            FIRSTROW = 2 , 
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: '+ CAST(DATEDIFF(second, @start_time, @end_time) as NVARCHAR) + 'seconds'        
        PRINT'----------------------------'
        SET @start_time = GETDATE();
        PRINT'TABLE : bronze.crm_sales_details'
        TRUNCATE TABLE bronze.crm_sales_details;
        BULK INSERT bronze.crm_sales_details
        from '/mnt/data/sql-ultimate-course/source_crm/sales_details.csv'
        WITH (
            FIRSTROW = 2 , 
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: '+ CAST(DATEDIFF(second, @start_time, @end_time) as NVARCHAR) + 'seconds'
        PRINT'----------------------------'
        PRINT '----------------------------'
        PRINT 'Loading erp tables'
        PRINT '----------------------------'
        PRINT'----------------------------'
        SET @start_time = GETDATE();
        PRINT'TABLE : bronze.erp_cust_az12'
        TRUNCATE TABLE bronze.erp_cust_az12;
        BULK INSERT bronze.erp_cust_az12
        from '/mnt/data/sql-ultimate-course/source_erp/CUST_AZ12.csv'
        WITH (
            FIRSTROW = 2 , 
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: '+ CAST(DATEDIFF(second, @start_time, @end_time) as NVARCHAR) + 'seconds'
        PRINT'----------------------------'
        SET @start_time = GETDATE();
        PRINT'TABLE : bronze.erp_loc_a101'
        TRUNCATE TABLE bronze.erp_loc_a101;
        BULK INSERT bronze.erp_loc_a101
        from '/mnt/data/sql-ultimate-course/source_erp/LOC_A101.csv'
        WITH (
            FIRSTROW = 2 , 
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: '+ CAST(DATEDIFF(second, @start_time, @end_time) as NVARCHAR) + 'seconds'
        PRINT'----------------------------'
        SET @start_time = GETDATE();
        PRINT'TABLE : bronze.erp_px_cat_g1v2'
        TRUNCATE TABLE bronze.erp_px_cat_g1v2;
        BULK INSERT bronze.erp_px_cat_g1v2
        from '/mnt/data/sql-ultimate-course/source_erp/PX_CAT_G1V2.csv'
        WITH (
            FIRSTROW = 2 , 
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: '+ CAST(DATEDIFF(second, @start_time, @end_time) as NVARCHAR) + 'seconds'

    SET @end_time_bronze = GETDATE();
    PRINT '>> Load Duration of bronze layer: '+ CAST(DATEDIFF(second, @start_time_bronze, @end_time_bronze) as NVARCHAR) + 'seconds'
    
    END TRY
    BEGIN CATCH 
        PRINT'-------------------------------------------'
        PRINT 'ERROR OCCURRED DURING LOADING BRONZE LAYER'
        PRINT 'ERROR MESSAGE' + ERROR_MESSAGE();
        PRINT 'ERROR NUMBER' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'ERROR STATE' + CAST(ERROR_STATE() AS NVARCHAR);
        PRINT'-------------------------------------------'
    END CATCH
END

--EXEC bronze.load_bronze
