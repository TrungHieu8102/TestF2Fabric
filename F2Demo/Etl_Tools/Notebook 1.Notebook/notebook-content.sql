-- Fabric notebook source

-- METADATA ********************

-- META {
-- META   "kernel_info": {
-- META     "name": "sqldatawarehouse"
-- META   },
-- META   "dependencies": {
-- META     "warehouse": {
-- META       "default_warehouse": "3d1e11c8-06fa-445a-aef5-96f491adb3c5",
-- META       "known_warehouses": [
-- META         {
-- META           "id": "3d1e11c8-06fa-445a-aef5-96f491adb3c5",
-- META           "type": "Lakewarehouse"
-- META         },
-- META         {
-- META           "id": "5978352d-624d-aca2-499f-71cb193d07cb",
-- META           "type": "Datawarehouse"
-- META         }
-- META       ]
-- META     }
-- META   }
-- META }

-- CELL ********************

SELECT *
FROM DWH_Golden_Data.dbo.Fact_ChamCongTongHop

-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }

-- CELL ********************

SELECT * 
FROM LKH_Silver_Data.dbo.ierp_chamcongchitiet

-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }

-- CELL ********************

SELECT * 
FROM LKH_Silver_Data.dbo.Ierp_ChamCong

-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }

-- CELL ********************

SELECT * 
FROM Ierp_DanhSachDuAn


-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }

-- CELL ********************

SELECT TOP (100) [Id],
			[ContentTypeID],
			[ContentType],
			[TimekeepingID],
			[Modified],
			[Created],
			[CreatedById],
			[ModifiedById],
			[Owshiddenversion],
			[Version],
			[Path],
			[ComplianceAssetId],
			[StatusValue],
			[ManagerNote],
			[Note],
			[IsOvertime],
			[Week],
			[Year],
			[EmployeeId],
			[DAVVId],
			[DAVVName],
			[ManagerEmail],
			[EmployeeName],
			[ApproverEmail],
			[IsOnsite],
			[MealAllowance],
			[TransportationAllowance],
			[ApproverNameId],
			[LTDLAllowance],
			[ColorTag]
FROM [LKH_Silver_Data].[dbo].[Ierp_ChamCong]
WHERE [IsOnsite] = 'True'

-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }

-- CELL ********************

DELETE FROM [DWH_Golden_Data].[dbo].[Fact_ChamCongTongHop];
INSERT INTO [DWH_Golden_Data].[dbo].[Fact_ChamCongTongHop] (
    MA_DU_AN, TEN_DU_AN, NGAY, TUAN, NAM, MA_NHANVIEN, TEN_NHANVIEN, MA_PM, TEN_PM,
    SO_GIO_CONG_ONSITE, SO_GIO_CONG_KHONG_ONSITE, SO_GIO_CONG_OT, SO_GIO_CONG_KHONG_OT,
    TIEN_AN, TIEN_DI_LAI
)
SELECT  DA.[MãDAVV] MA_DU_AN,
		DA.[TênDAVVRútGọn] TEN_DU_AN,
		CONVERT(DATE,CT.[Date]) NGAY,
		CONVERT(INT,CC.[Week]) TUAN,
		CONVERT(INT,CC.[Year]) NAM,
		CONVERT(INT,CC.[EmployeeId]) MA_NHANVIEN,
		CC.[EmployeeName] TEN_NHANVIEN,
		CONVERT(INT,CC.[ApproverNameId]) MA_PM,
		CC.[ApproverEmail] TEN_PM,
		CASE
			WHEN CC.[IsOnsite] = 'True' THEN SUM(CONVERT(DECIMAL(18,2),CT.[AmountOfHour]))
			ELSE 0
		END SO_GIO_CONG_ONSITE,
		CASE
			WHEN CC.[IsOnsite] = 'False' THEN SUM(CONVERT(DECIMAL(18,2),CT.[AmountOfHour]))
			ELSE 0
		END SO_GIO_CONG_KHONG_ONSITE,
		CASE
			WHEN CC.[IsOvertime] = '1' THEN SUM(CONVERT(DECIMAL(18,2),CT.[AmountOfHour]))
			ELSE 0
		END SO_GIO_CONG_OT,
		CASE
			WHEN CC.[IsOvertime] = '0' THEN SUM(CONVERT(DECIMAL(18,2),CT.[AmountOfHour]))
			ELSE 0
		END SO_GIO_CONG_KHONG_OT,
		SUM(ISNULL(CONVERT(DECIMAL(18,2),CT.[FoodExpenses]),0)) AS TIEN_AN,
		SUM(ISNULL(CONVERT(DECIMAL(18,2),CT.[TravelExpenses]),0)) AS TIEN_DI_LAI
FROM [LKH_Silver_Data].[dbo].[ierp_chamcongchitiet] CT
LEFT JOIN [LKH_Silver_Data].[dbo].[Ierp_ChamCong] CC ON CC.Id = CT.TimekeepingID
LEFT JOIN [LKH_Silver_Data].[dbo].[Ierp_DanhSachDuAn] DA ON CC.[DAVVId] = DA.Id
GROUP BY DA.[MãDAVV],
		DA.[TênDAVVRútGọn],
		CT.[Date],
		CC.[Week],
		CC.[Year],
		CC.[EmployeeId],
		CC.[EmployeeName],
		CC.[ApproverNameId],
		CC.[ApproverEmail],
		CC.[IsOvertime],
		CC.[IsOnsite]
		
		

-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }

-- CELL ********************

SELECT TOP (100) [MãDAVV],
			[TênDAVVRútGọn],
			[PhânLoạiValue],
			[CấpĐộDựÁnValue],
			[DoanhThuGhiNhậnChoValue],
			[MôTảDAVV],
			[PMId],
			[SalesId],
			[GĐDAId],
			[KhoảngCáchĐịaLýKm],
			[SốHợpĐồng],
			[TênHợpĐồng],
			[NgàyKýHĐ],
			[NgàyKýHĐThựcTế],
			[NgàyBắtĐầuTK],
			[LoạiHĐValue],
			[LoạiSalesValue],
			[LĩnhVựcValue],
			[ĐốiTácThầuPhụ],
			[TênKHChủĐTId],
			[SPDVValue],
			[TrạngTháiValue],
			[LinkTớiThưMụcDựÁn],
			[PC_LTDL],
			[NgàyHếtHiệuLựcTheoHĐ],
			[NgàyHếtHiệuLựcHĐ_CậpNhật],
			[Complete],
			[HiệuLựcChấmCông],
			[Id],
			[ContentTypeID],
			[ContentType],
			[Modified],
			[Created],
			[CreatedById],
			[ModifiedById],
			[Owshiddenversion],
			[Version],
			[Path],
			[ComplianceAssetId],
			[ColorTag]
FROM [LKH_Silver_Data].[dbo].[Ierp_DanhSachDuAn]

-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }
