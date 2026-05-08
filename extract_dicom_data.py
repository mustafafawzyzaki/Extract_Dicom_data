import logging
import logging
import os
import pydicom
import pyodbc 
import sys
from dotenv import load_dotenv
from tqdm import tqdm
import datetime 

os.makedirs('logs', exist_ok=True)

logging.basicConfig(
    filename='logs/extract_dicom_data_' + datetime.datetime.now().strftime('%Y%m%d') + '.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s', 
    encoding='utf-8',
)

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
        logging.info(f"Successfully connected to database")
        return conn
    except Exception as e:
        print(f"Failed to connect to database: {e}")
        logging.error(f"Failed to connect to database: {e}")
        return None

def insert_dicom_data(cursor, data): 
    try: 
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
        cursor.connection.commit()
        # Using debug instead of info to prevent massive log bloat
        logging.info(f"Successfully inserted patient_id: {data['patient_id']}, file_path: {data['file_path']}")
        return True
    except Exception as e:
        print(f"Failed to insert {data['file_path']}: {e}")
        logging.error(f"Failed to insert {data['file_path']}: {e}")
        return False
    
def get_dicom_val(dataset, tag, is_date=False):
    val = getattr(dataset, tag, None)
    if val is None or val == '':
        return None if is_date else ''
    return str(val)

def read_dicom_data(file_path):
    try: 
        try:
            dataset = pydicom.dcmread(file_path, force=True, stop_before_pixels=True)
            logging.info(f"Successfully read {file_path}")
        except Exception as e:
            print(f"Failed to read {file_path}: {str(e)}")
            logging.error(f"Failed to read {file_path}: {str(e)}")
            return None

        data = {
            'patient_id':           get_dicom_val(dataset, 'PatientID'),
            'patient_name':         get_dicom_val(dataset, 'PatientName'),
            'accession_number':     get_dicom_val(dataset, 'AccessionNumber'),
            'study_uid':            get_dicom_val(dataset, 'StudyInstanceUID'),
            'sopinstanceuid':       get_dicom_val(dataset, 'SOPInstanceUID'),
            'study_id':             get_dicom_val(dataset, 'StudyID'),
            'study_date':           get_dicom_val(dataset, 'StudyDate', is_date=True),
            'study_time':           get_dicom_val(dataset, 'StudyTime'),
            'modality':             get_dicom_val(dataset, 'Modality'),
            'acquisition_date':     get_dicom_val(dataset, 'AcquisitionDate', is_date=True),
            'acquisition_time':     get_dicom_val(dataset, 'AcquisitionTime'),
            'series_date':          get_dicom_val(dataset, 'SeriesDate', is_date=True),
            'series_time':          get_dicom_val(dataset, 'SeriesTime'),
            'file_path':            file_path,
            'study_directory_path': os.path.dirname(os.path.dirname(file_path)),
            'file_size':            os.path.getsize(file_path),
        }
        logging.info(f"Successfully read accession_number: {data['accession_number']}, study_uid: {data['study_uid']}, sopinstanceuid: {data['sopinstanceuid']}, file_path: {data['file_path']}")
        return data
    except Exception as e:
        print(f"Failed to read {file_path}: {e}")
        logging.error(f"Failed to read {file_path}: {e}")
        return None

def get_resource_path(relative_path):
    """Get absolute path to resource, works for dev and for PyInstaller."""
    try:
        # PyInstaller creates a temp folder and stores path in _MEIPASS
        base_path = sys._MEIPASS
    except Exception:
        base_path = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(base_path, relative_path)

def deploy_sql_scripts(conn):
    try: 
        TABLE_SQL_PATH = os.getenv("TABLE_SQL_PATH")
        SP_SQL_PATH = os.getenv("SP_SQL_PATH")
        with open(TABLE_SQL_PATH,'r',encoding='utf-8') as f_table:
            script_table= f_table.read()
        
        with open(SP_SQL_PATH,'r',encoding='utf-8') as f_sp:
            script_sp = f_sp.read()

        cursor = conn.cursor()
        cursor.execute(script_table)
        conn.commit()
        cursor.execute(script_sp)
        conn.commit()
        logging.info(f"Successfully deployed SQL scripts")
    except Exception as e:
        logging.error(f"Failed to deploy SQL scripts: {e}")
        return False
    return True
def main():
    try: 
        starttime = datetime.datetime.now()
        logging.info(f"Starting to Extract Dicom Data Script")
        SQL_SERVER_IP = os.getenv("SQL_SERVER_IP")
        SQL_SERVER_PORT = os.getenv("SQL_SERVER_PORT")
        DB_NAME = os.getenv("DB_NAME")
        DB_USERNAME = os.getenv("DB_USERNAME")
        DB_PASSWORD = os.getenv("DB_PASSWORD")
        ROOT_DIR = os.getenv("ROOT_DIR")

        required_vars = {
            "SQL_SERVER_IP": SQL_SERVER_IP,
            "SQL_SERVER_PORT": SQL_SERVER_PORT,
            "DB_NAME": DB_NAME,
            "DB_USERNAME": DB_USERNAME,
            "DB_PASSWORD": DB_PASSWORD,
            "ROOT_DIR": ROOT_DIR,
        }
        missing = [k for k, v in required_vars.items() if not v]
        if missing:
            msg = f"Missing required environment variables: {', '.join(missing)}"
            print(msg)
            logging.error(msg)
            return False

        conn = get_db_connection(SQL_SERVER_IP, SQL_SERVER_PORT, DB_NAME, DB_USERNAME, DB_PASSWORD)
        if conn is None:
            msg = "Failed to establish database connection. Exiting."
            print(msg)
            logging.error(msg)
            return False

        try:
            deploy_sql_scripts(conn)

            cursor = conn.cursor()

            for root, dirs, files in tqdm(os.walk(ROOT_DIR), desc="Scanning directories", unit="dir"):
                try: 
                    for file in files:
                        if file.lower().endswith(('.dcm', '.dcn', '.kon','.pr', '.sr')): 
                            logging.info(f"Processing file: {file}")
                            file_path = os.path.join(root, file)
                            data = read_dicom_data(file_path)
                            if data:
                                if insert_dicom_data(cursor, data):
                                    logging.info(f"Successfully inserted patient_id: {data['patient_id']}, file_path: {data['file_path']}")
                except Exception as e:
                    print(f"Failed to process directory {root}: {str(e)}")
                    logging.error(f"Failed to process directory {root}: {str(e)}")
                    continue
            endtime = datetime.datetime.now()
            logging.info(f"Successfully processed all files in {ROOT_DIR} - Total time taken: {endtime - starttime}")
            return True
        finally:
            conn.close()
            logging.info("Database connection closed.")
    except Exception as e:
        print(f"Failed to run main: {str(e)}")
        logging.error(f"Failed to run main: {str(e)}")
        return False

if __name__ == "__main__":
    main()