-- ============================================================
-- Stored Procedure: insert_dicom_data
-- Description: Inserts DICOM file metadata into the database.
-- ============================================================

CREATE OR ALTER PROCEDURE insert_dicom_data
    @patient_id           NVARCHAR(64),
    @patient_name         NVARCHAR(256),
    @accession_number     NVARCHAR(64),
    @study_uid            NVARCHAR(256),
    @sopinstanceuid       NVARCHAR(256),
    @study_id             NVARCHAR(64),
    @study_date           datetime,
    @study_time           NVARCHAR(16),
    @modality             NVARCHAR(32),
    @acquisition_date     datetime,
    @acquisition_time     NVARCHAR(16),
    @series_date          datetime,
    @series_time          NVARCHAR(16),
    @file_path            NVARCHAR(1024),
    @study_directory_path NVARCHAR(1024),
    @file_size            BIGINT
AS
BEGIN
    SET NOCOUNT ON;
if not exists (select * from DicomData where sopinstanceuid = @sopinstanceuid)
BEGIN
    INSERT INTO DicomData (
        patient_id,
        patient_name,
        accession_number,
        study_uid,
        sopinstanceuid,
        study_id,
        study_date,
        study_time,
        modality,
        acquisition_date,
        acquisition_time,
        series_date,
        series_time,
        file_path,
        study_directory_path,
        file_size      
    )
    VALUES (
        @patient_id,
        @patient_name,
        @accession_number,
        @study_uid,
        @sopinstanceuid,
        @study_id,
        @study_date,
        @study_time,
        @modality,
        @acquisition_date,
        @acquisition_time,
        @series_date,
        @series_time,
        @file_path,
        @study_directory_path,
        @file_size    
    );
END
END
