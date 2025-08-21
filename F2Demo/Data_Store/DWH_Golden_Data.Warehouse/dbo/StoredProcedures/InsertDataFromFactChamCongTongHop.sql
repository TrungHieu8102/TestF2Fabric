-- Step 1: Create a stored procedure named InsertDataFromFactChamCongTongHop
-- Step 2: Define the input parameter for the procedure to accept MA_DU_AN
-- Step 3: Use an INSERT INTO statement to insert data into the Test table
-- Step 4: Select all columns from Fact_ChamCongTongHop where MA_DU_AN matches the input parameter
-- Step 5: Ensure that the Test table exists and has the same structure as the selected columns from Fact_ChamCongTongHop

CREATE PROCEDURE InsertDataFromFactChamCongTongHop
    @MA_DU_AN VARCHAR(50) -- Assuming MA_DU_AN is of type VARCHAR(50), adjust as necessary
AS
BEGIN
    -- Step 6: Insert data into the Test table from Fact_ChamCongTongHop based on the provided MA_DU_AN
    INSERT INTO [DWH_Golden_Data].[dbo].[Test] (
        [MA_DU_AN],
        [TEN_DU_AN],
        [NGAY],
        [TUAN],
        [NAM],
        [MA_NHANVIEN],
        [TEN_NHANVIEN],
        [MA_PM],
        [TEN_PM],
        [SO_GIO_CONG_ONSITE],
        [SO_GIO_CONG_KHONG_ONSITE],
        [SO_GIO_CONG_OT],
        [SO_GIO_CONG_KHONG_OT],
        [TIEN_AN],
        [TIEN_DI_LAI]
    )
    SELECT 
        [MA_DU_AN],
        [TEN_DU_AN],
        [NGAY],
        [TUAN],
        [NAM],
        [MA_NHANVIEN],
        [TEN_NHANVIEN],
        [MA_PM],
        [TEN_PM],
        [SO_GIO_CONG_ONSITE],
        [SO_GIO_CONG_KHONG_ONSITE],
        [SO_GIO_CONG_OT],
        [SO_GIO_CONG_KHONG_OT],
        [TIEN_AN],
        [TIEN_DI_LAI]
    FROM 
        [DWH_Golden_Data].[dbo].[Fact_ChamCongTongHop] AS F1
    WHERE 
        [F1].[MA_DU_AN] = @MA_DU_AN;
END;