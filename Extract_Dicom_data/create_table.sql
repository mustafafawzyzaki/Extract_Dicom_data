-- ============================================================
-- Table: DicomData
-- Description: Stores DICOM file metadata extracted from 
--              medical imaging files.
-- Run this script BEFORE running insert_dicom_data.sql
-- ============================================================

IF NOT EXISTS (
    SELECT * FROM sys.objects 
    WHERE object_id = OBJECT_ID(N'[dbo].[DicomData]') 
    AND type = N'U'
)
BEGIN
    CREATE TABLE [dbo].[DicomData] (
        [id]                    INT             IDENTITY(1,1)   NOT NULL,
        [patient_id]            NVARCHAR(64)                    NULL,
        [patient_name]          NVARCHAR(256)                   NULL,
        [accession_number]      NVARCHAR(64)                    NULL,
        [study_uid]             NVARCHAR(256)                   NULL,
        [sopinstanceuid]        NVARCHAR(256)                   NULL,
        [study_id]              NVARCHAR(64)                    NULL,
        [study_date]            datetime                        NULL,
        [study_time]            NVARCHAR(16)                    NULL,
        [modality]              NVARCHAR(32)                    NULL,
        [acquisition_date]      datetime                        NULL,
        [acquisition_time]      NVARCHAR(16)                    NULL,
        [series_date]           datetime                        NULL,
        [series_time]           NVARCHAR(16)                    NULL,
        [file_path]             NVARCHAR(1024)                  NULL,
        [study_directory_path]  NVARCHAR(1024)                  NULL,
        [file_size]             BIGINT                          NULL,
        [created_at]            DATETIME        DEFAULT GETDATE() NOT NULL

        CONSTRAINT [PK_DicomData] PRIMARY KEY CLUSTERED ([id] ASC)
    );  

    PRINT 'Table DicomData created successfully.';
END
ELSE
BEGIN
    PRINT 'Table DicomData already exists. Skipping creation.';
END
