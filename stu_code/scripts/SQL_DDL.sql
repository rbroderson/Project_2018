USE [6250]
GO

/****** Object:  Table [dbo].[ADMISSIONS]    Script Date: 4/23/2018 7:18:13 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[ADMISSIONS](
	[ROW_ID] [int] NOT NULL,
	[SUBJECT_ID] [int] NULL,
	[HADM_ID] [int] NULL,
	[ADMITTIME] [datetime2](7) NULL,
	[DISCHTIME] [datetime2](7) NULL,
	[DEATHTIME] [nvarchar](max) NULL,
	[ADMISSION_TYPE] [nvarchar](max) NULL,
	[ADMISSION_LOCATION] [nvarchar](max) NULL,
	[DISCHARGE_LOCATION] [nvarchar](max) NULL,
	[INSURANCE] [nvarchar](max) NULL,
	[LANGUAGE] [nvarchar](max) NULL,
	[RELIGION] [nvarchar](max) NULL,
	[MARITAL_STATUS] [nvarchar](max) NULL,
	[ETHNICITY] [nvarchar](max) NULL,
	[EDREGTIME] [datetime2](7) NULL,
	[EDOUTTIME] [datetime2](7) NULL,
	[DIAGNOSIS] [nvarchar](max) NULL,
	[HOSPITAL_EXPIRE_FLAG] [int] NULL,
	[HAS_CHARTEVENTS_DATA] [int] NULL,
 CONSTRAINT [PK_ADMISSIONS] PRIMARY KEY CLUSTERED 
(
	[ROW_ID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO

USE [6250]
GO

/****** Object:  Table [dbo].[ADMISSIONS2]    Script Date: 4/23/2018 7:18:29 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[ADMISSIONS2](
	[ROW_ID] [int] NOT NULL,
	[SUBJECT_ID] [int] NULL,
	[HADM_ID] [int] NULL,
	[ADMITTIME] [datetime2](7) NULL,
	[DISCHTIME] [datetime2](7) NULL,
	[DEATHTIME] [nvarchar](max) NULL,
	[ADMISSION_TYPE] [nvarchar](max) NULL,
	[ADMISSION_LOCATION] [nvarchar](max) NULL,
	[DISCHARGE_LOCATION] [nvarchar](max) NULL,
	[INSURANCE] [nvarchar](max) NULL,
	[LANGUAGE] [nvarchar](max) NULL,
	[RELIGION] [nvarchar](max) NULL,
	[MARITAL_STATUS] [nvarchar](max) NULL,
	[ETHNICITY] [nvarchar](max) NULL,
	[EDREGTIME] [datetime2](7) NULL,
	[EDOUTTIME] [datetime2](7) NULL,
	[DIAGNOSIS] [nvarchar](max) NULL,
	[HOSPITAL_EXPIRE_FLAG] [int] NULL,
	[HAS_CHARTEVENTS_DATA] [int] NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO

USE [6250]
GO

/****** Object:  Table [dbo].[CHARTEVENTS]    Script Date: 4/23/2018 7:18:46 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[CHARTEVENTS](
	[ROW_ID] [int] NOT NULL,
	[SUBJECT_ID] [int] NULL,
	[HADM_ID] [int] NULL,
	[ICUSTAY_ID] [int] NULL,
	[ITEMID] [int] NULL,
	[CHARTTIME] [datetime2](7) NULL,
	[STORETIME] [datetime2](7) NULL,
	[CGID] [int] NULL,
	[VALUE] [nvarchar](max) NULL,
	[VALUENUM] [float] NULL,
	[VALUEUOM] [nvarchar](max) NULL,
	[WARNING] [int] NULL,
	[ERROR] [int] NULL,
	[RESULTSTATUS] [nvarchar](max) NULL,
	[STOPPED] [nvarchar](max) NULL,
 CONSTRAINT [PK_CHARTEVENTS] PRIMARY KEY CLUSTERED 
(
	[ROW_ID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO

USE [6250]
GO

/****** Object:  Table [dbo].[DIAGNOSES_ICD]    Script Date: 4/23/2018 7:18:54 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[DIAGNOSES_ICD](
	[ROW_ID] [int] NOT NULL,
	[SUBJECT_ID] [int] NULL,
	[HADM_ID] [int] NULL,
	[SEQ_NUM] [int] NULL,
	[ICD9_CODE] [nvarchar](50) NULL,
 CONSTRAINT [PK_DIAGNOSES_ICD] PRIMARY KEY CLUSTERED 
(
	[ROW_ID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
GO

USE [6250]
GO

/****** Object:  Table [dbo].[LABEVENTS]    Script Date: 4/23/2018 7:19:09 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[LABEVENTS](
	[ROW_ID] [int] NOT NULL,
	[SUBJECT_ID] [int] NULL,
	[HADM_ID] [nvarchar](max) NULL,
	[ITEMID] [int] NULL,
	[CHARTTIME] [datetime2](7) NULL,
	[VALUE] [nvarchar](max) NULL,
	[VALUENUM] [float] NULL,
	[VALUEUOM] [nvarchar](max) NULL,
	[FLAG] [nvarchar](max) NULL,
	[FEATUREVALUE] [float] NULL,
 CONSTRAINT [PK_LABEVENTS] PRIMARY KEY CLUSTERED 
(
	[ROW_ID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO


USE [6250]
GO

/****** Object:  Table [dbo].[LABEVENTS2]    Script Date: 4/23/2018 7:19:18 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[LABEVENTS2](
	[SUBJECT_ID] [nvarchar](10) NULL,
	[ITEMID] [nvarchar](10) NULL,
	[FEATUREVALUE] [nvarchar](30) NULL
) ON [PRIMARY]
GO

USE [6250]
GO

/****** Object:  Table [dbo].[PATIENTS]    Script Date: 4/23/2018 7:19:30 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[PATIENTS](
	[ROW_ID] [int] NOT NULL,
	[SUBJECT_ID] [int] NOT NULL,
	[GENDER] [nvarchar](50) NULL,
	[DOB] [datetime2](7) NULL,
	[DOD] [nvarchar](max) NULL,
	[DOD_HOSP] [nvarchar](max) NULL,
	[DOD_SSN] [nvarchar](max) NULL,
	[EXPIRE_FLAG] [int] NULL,
	[FIRST_ADMISSIONTIME] [datetime2](7) NULL,
	[AGE_GROUP] [nvarchar](50) NULL,
	[INDEX_DATE] [datetime2](7) NULL,
	[GENDER_INDICATOR] [int] NULL,
 CONSTRAINT [PK_PATIENTS] PRIMARY KEY CLUSTERED 
(
	[ROW_ID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO


USE [6250]
GO

/****** Object:  Table [dbo].[PRESCRIPTIONS]    Script Date: 4/23/2018 7:19:49 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[PRESCRIPTIONS](
	[ROW_ID] [int] NOT NULL,
	[SUBJECT_ID] [int] NOT NULL,
	[HADM_ID] [int] NULL,
	[ICUSTAY_ID] [nvarchar](max) NULL,
	[STARTDATE] [datetime2](7) NULL,
	[ENDDATE] [datetime2](7) NULL,
	[DRUG_TYPE] [nvarchar](max) NULL,
	[DRUG] [nvarchar](max) NULL,
	[DRUG_NAME_POE] [nvarchar](max) NULL,
	[DRUG_NAME_GENERIC] [nvarchar](max) NULL,
	[FORMULARY_DRUG_CD] [nvarchar](max) NULL,
	[GSN] [nvarchar](max) NULL,
	[NDC] [nvarchar](max) NULL,
	[PROD_STRENGTH] [nvarchar](max) NULL,
	[DOSE_VAL_RX] [nvarchar](max) NULL,
	[DOSE_UNIT_RX] [nvarchar](max) NULL,
	[FORM_VAL_DISP] [nvarchar](max) NULL,
	[FORM_UNIT_DISP] [nvarchar](max) NULL,
	[ROUTE] [nvarchar](max) NULL,
	[FEATUREVALUE] [nvarchar](max) NULL,
 CONSTRAINT [PK_PRESCRIPTIONS] PRIMARY KEY CLUSTERED 
(
	[ROW_ID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO


USE [6250]
GO

/****** Object:  Table [dbo].[PROCEDURES_ICD]    Script Date: 4/23/2018 7:20:03 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[PROCEDURES_ICD](
	[ROW_ID] [int] NOT NULL,
	[SUBJECT_ID] [int] NULL,
	[HADM_ID] [int] NULL,
	[SEQ_NUM] [int] NULL,
	[ICD9_CODE] [nvarchar](max) NULL,
 CONSTRAINT [PK_PROCEDURES_ICD] PRIMARY KEY CLUSTERED 
(
	[ROW_ID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
