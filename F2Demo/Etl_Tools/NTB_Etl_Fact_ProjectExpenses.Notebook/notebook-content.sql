-- Fabric notebook source

-- METADATA ********************

-- META {
-- META   "kernel_info": {
-- META     "name": "sqldatawarehouse"
-- META   },
-- META   "dependencies": {
-- META     "lakehouse": {
-- META       "default_lakehouse": "41c8c31a-0f05-4bea-aad4-9b18da24a1b3",
-- META       "default_lakehouse_name": "LKH_Silver_Data",
-- META       "default_lakehouse_workspace_id": "8c76cb1c-cf62-4ebe-b9df-299509803689",
-- META       "known_lakehouses": [
-- META         {
-- META           "id": "41c8c31a-0f05-4bea-aad4-9b18da24a1b3"
-- META         }
-- META       ]
-- META     },
-- META     "warehouse": {
-- META       "default_warehouse": "3d1e11c8-06fa-445a-aef5-96f491adb3c5",
-- META       "known_warehouses": [
-- META         {
-- META           "id": "5978352d-624d-aca2-499f-71cb193d07cb",
-- META           "type": "Datawarehouse"
-- META         },
-- META         {
-- META           "id": "3d1e11c8-06fa-445a-aef5-96f491adb3c5",
-- META           "type": "Lakewarehouse"
-- META         }
-- META       ]
-- META     }
-- META   }
-- META }

-- CELL ********************

SELECT distinct cc.EmployeeName
FROM [dbo].[Ierp_ChamCong] cc
JOIN [dbo].[Ierp_DanhSachDuAn] da ON cc.DAVVId = da.Id
WHERE da.TênDAVVRútGọn = 'Presales cho ThànhNV' and cc.Year = 2024



-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }

-- CELL ********************

DELETE FROM DWH_Golden_Data.dbo.Fact_ProjectExpenses;
INSERT INTO DWH_Golden_Data.dbo.Fact_ProjectExpenses
(
    DAVVId,
    DAVVName,
    TotalFoodExpenses,
    TotalTravelExpenses,
    Year,
    Month
)
SELECT 
    DAVVId,
    DAVVName,
    SUM(CASE 
            WHEN MealAllowance <> 'None' THEN CAST(MealAllowance AS FLOAT) 
            ELSE 0 
        END) AS TotalFoodExpenses,
    SUM(CASE 
            WHEN TransportationAllowance <> 'None' THEN CAST(TransportationAllowance AS FLOAT) 
            ELSE 0 
        END) AS TotalTravelExpenses,
    CONVERT(VARCHAR,Year(Modified)) AS Year,
    CONVERT(VARCHAR,Month(Modified)) AS Month
FROM LKH_Silver_Data.dbo.Ierp_ChamCong
GROUP BY DAVVId, DAVVName,Year(Modified),Month(Modified)

-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }
