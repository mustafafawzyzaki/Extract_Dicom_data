import os
import pydicom
import pyodbc 
from dotenv import load_dotenv
from tqdm import tqdm

load_dotenv()

def get_db_connection(SQL_SERVER_IP, SQL_SERVER_PORT, DB_NAME, DB_USERNAME, DB_PASSWORD):
    try: 
        conn = pyodbc.connect(
            f"Driver={{SQL Server}};"
            f"Server={SQL_SERVER_IP},{SQL_SERVER_PORT};"
            f"Database={DB_NAME};"
            f"UID={DB_USERNAME};"
            f"PWD={DB_PASSWORD};"
        )
        return conn
    except Exception as e:
        print(f"Failed to connect to database: {e}")
        return None

def insert_dicom_data(con, data): 
    try: 
        cursor = con.cursor()
        query = """Execute insert_dicom_data @patient_id=?, @patient_name=?, @accession_number=?, @study_uid=?, @sopinstanceuid=?, @study_id=?, @study_date=?, @study_time=?, @modality=?, @acquisition_date=?, @acquisition_time=?, @series_date=?, @series_time=?, @file_path=?, @study_directory_path=?,@file_size=?"""
        params = (
            data['patient_id'],
            data['patient_name'],
            data['accession_number'],
            data['study_uid'],
            data['sopinstanceuid'],
            data['study_id'],
            data['study_date'],
            data['study_time'],
            data['modality'],
            data['acquisition_date'],
            data['acquisition_time'],
            data['series_date'],
            data['series_time'],
            data['file_path'],
            data['study_directory_path'],
            data['file_size'],  
        )
        cursor.execute(query, params)
        con.commit()
        return True
    except Exception as e:
        print(f"Failed to insert {data['file_path']}: {e}")
        return False
    
def read_dicom_data(file_path):
    try: 
        if file_path.lower().endswith(('.dcm', '.dcn', '.kon', '.pr', '.sr')):
            try:
                dataset = pydicom.dcmread(file_path, force=True, stop_before_pixels=True)
            except Exception as e:
                print(f"Failed to read {file_path}: {e}")
                return None

            data = {
                'patient_id':           str(getattr(dataset, 'PatientID', '') or ''),
                'patient_name':         str(getattr(dataset, 'PatientName', '') or ''),
                'accession_number':     str(getattr(dataset, 'AccessionNumber', '') or ''),
                'study_uid':            str(getattr(dataset, 'StudyInstanceUID', '') or ''),
                'sopinstanceuid':       str(getattr(dataset, 'SOPInstanceUID', '') or ''),
                'study_id':             str(getattr(dataset, 'StudyID', '') or ''),
                'study_date':           str(getattr(dataset, 'StudyDate', '') or ''),
                'study_time':           str(getattr(dataset, 'StudyTime', '') or ''),
                'modality':             str(getattr(dataset, 'Modality', '') or ''),
                'acquisition_date':     str(getattr(dataset, 'AcquisitionDate', '') or ''),
                'acquisition_time':     str(getattr(dataset, 'AcquisitionTime', '') or ''),
                'series_date':          str(getattr(dataset, 'SeriesDate', '') or ''),
                'series_time':          str(getattr(dataset, 'SeriesTime', '') or ''),
                'file_path':            file_path,
                'study_directory_path': "\\".join(os.path.dirname(file_path).split("\\")[:-1]),
                'file_size':            os.path.getsize(file_path),
            }
            return data
        else:
            return None
    except Exception as e:
        print(f"Failed to read {file_path}: {e}")
        return None

def deploy_sql_scripts(conn):
    try: 
        cursor = conn.cursor()
        with open('create_table.sql', 'r') as f:
            cursor.execute(f.read())
        conn.commit()
        with open('insert_dicom_data.sql', 'r') as f:
            cursor.execute(f.read())
        conn.commit()
        return True    
    except Exception as e:
        print(f"Failed to deploy SQL scripts: {e}")
        return False

def __main__():
    try: 
        SQL_SERVER_IP = os.getenv("SQL_SERVER_IP")
        SQL_SERVER_PORT = os.getenv("SQL_SERVER_PORT")
        DB_NAME = os.getenv("DB_NAME")
        DB_USERNAME = os.getenv("DB_USERNAME")
        DB_PASSWORD = os.getenv("DB_PASSWORD")
        ROOT_DIR = os.getenv("ROOT_DIR")

        deploy_sql_scripts(get_db_connection(SQL_SERVER_IP, SQL_SERVER_PORT, DB_NAME, DB_USERNAME, DB_PASSWORD))

        for root, dirs, files in tqdm(os.walk(ROOT_DIR),desc="Scanning directories", unit="dir", ):
            for file in files:
                if file.lower().endswith(('.dcm', '.dcn', '.kon','.pr', '.sr')): 
                    file_path = os.path.join(root, file)
                    data = read_dicom_data(file_path)
                    if data:
                        insert_dicom_data(get_db_connection(SQL_SERVER_IP, SQL_SERVER_PORT, DB_NAME, DB_USERNAME, DB_PASSWORD), data)
    except Exception as e:
        print(f"Failed to run main: {e}")

if __name__ == "__main__":
    __main__()