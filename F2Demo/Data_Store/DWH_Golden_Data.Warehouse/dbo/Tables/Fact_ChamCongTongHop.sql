CREATE TABLE [dbo].[Fact_ChamCongTongHop] (

	[MA_DU_AN] varchar(max) NULL, 
	[TEN_DU_AN] varchar(max) NULL, 
	[NGAY] date NULL, 
	[TUAN] int NULL, 
	[NAM] int NULL, 
	[MA_NHANVIEN] int NULL, 
	[TEN_NHANVIEN] varchar(max) NULL, 
	[MA_PM] int NULL, 
	[TEN_PM] varchar(max) NULL, 
	[SO_GIO_CONG_ONSITE] decimal(10,2) NULL, 
	[SO_GIO_CONG_KHONG_ONSITE] decimal(10,2) NULL, 
	[SO_GIO_CONG_OT] decimal(10,2) NULL, 
	[SO_GIO_CONG_KHONG_OT] decimal(10,2) NULL, 
	[TIEN_AN] decimal(18,2) NULL, 
	[TIEN_DI_LAI] decimal(18,2) NULL
);