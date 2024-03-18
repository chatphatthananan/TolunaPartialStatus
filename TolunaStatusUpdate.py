import ftplib
import pysftp
import re
import os
import pandas as pd
import pyodbc
from datetime import datetime
import numpy as np
import shutil
import logging
from SGTAMProdTask import SGTAMProd
from DAM_Invited_ID import DAM_invited_id

# Global variables to store dataframes created in create_dataframes() function
toluna_combined_dataset = pd.DataFrame()

# Set up logging
log_filename = f"D:/05. Data Production/SPH/IncentiveEmailAutomation/TolunaPartialStatus/log/StatusUpdate_logfile_{datetime.now().strftime('%Y-%m-%d %H-%M-%S')}.txt"
logging.basicConfig(filename=log_filename, level=logging.INFO)

s = SGTAMProd()

# To get toluna dataset from gfk SFTP 
def get_toluna_dataset():

    logging.info("Start get_toluna_dataset()")

    # SFTP settings
    sftp_hostname = 'xxx'
    sftp_username = 'xxx'
    sftp_password = 'xxx'
    remote_directory = '/TAM_OGS/Toluna'
    cnopts = pysftp.CnOpts()
    cnopts.hostkeys = None

    dataFilesLocation = 'D:/05. Data Production/SPH/IncentiveEmailAutomation/TolunaPartialStatus/data/'

    # Specify fileName patterns and corresponding names to save
    filePatternsNames = {
        r'^Decipher_Online_Data': 'Decipher_Online_Status.xlsx',
        r'^Decipher_Offline_Data': 'Decipher_Offline_Status.xlsx'
    }

    # establish the SFTP connection
    with pysftp.Connection(sftp_hostname, username=sftp_username, password=sftp_password, cnopts=cnopts) as sftp:
        
        logging.info("Connected.")

        # change to the remote directory
        sftp.cwd(remote_directory)

        # get a list of files in the remote directory
        remote_files = sftp.listdir()

        for filePattern, fileName in filePatternsNames.items():

            filename_pattern = re.compile(filePattern)

            # filter the list of files to those matching the filename pattern
            matched_files = filter(lambda f: re.match(
                filename_pattern, f), remote_files)

            # get the latest matching file
            latest_file = max(
                matched_files, key=lambda f: sftp.stat(f).st_mtime)

            # download the latest file to the local directory
            sftp.get(latest_file, os.path.join(dataFilesLocation, fileName))
            logging.info(f'[DOWNLOADED]: {dataFilesLocation+fileName}')
    logging.info("Completed get_toluna_dataset().")

# To create dataframe from dataset files
def create_dataframes():
    logging.info("Start create_dataframe().")
    # declare global infront so that this function can assign values to the local variable created at the beginning of the script
    global toluna_combined_dataset
    
    dataFilesLocation = 'D:\\05. Data Production\\SPH\\IncentiveEmailAutomation\\TolunaPartialStatus\\data\\'
    

    # create a combined dataframe for toluna offline/online datasets
    toluna_offline_dataset = pd.read_excel(dataFilesLocation+'Decipher_Offline_Status.xlsx', usecols=[
                                      'id_temp', 'PPRespID', 'SampleSource', 'IsTest', 'status', 'EndPartCode','date'])
    toluna_online_dataset = pd.read_excel(dataFilesLocation+'Decipher_Online_Status.xlsx', usecols=[
                                     'id_temp', 'PPRespID', 'SampleSource', 'IsTest', 'status', 'EndPartCode', 'date'])
    
    logging.info("Will exclude autorecover records that are not in the invited IDs for this wave")
    
    # only want rows with invited IDs for online dataframe
    ID_to_ingest = DAM_invited_id
    toluna_online_dataset = toluna_online_dataset[toluna_online_dataset['id_temp'].isin(ID_to_ingest)]
    logging.info("Excluded")

    # to exclude row with empty temp_id for offline dataset
    toluna_offline_dataset = toluna_offline_dataset.dropna(subset=['id_temp'])

    # to combine offline and online dataframes
    toluna_combined_dataset = pd.concat([toluna_offline_dataset, toluna_online_dataset],ignore_index=True)
    logging.info("Combined toluna dataframe created.")


    toluna_combined_dataset = toluna_combined_dataset[['id_temp','PPRespID','SampleSource','status','EndPartCode','date']]
    toluna_combined_dataset = toluna_combined_dataset.rename(columns={      
                                                                                  'id_temp':'id_toluna'
                                                                                  ,'PPRespID':'PPRespID_toluna'
                                                                                  ,'SampleSource':'SampleSource_toluna'
                                                                                  ,'status':'status_toluna'
                                                                                  ,'EndPartCode':'EndPartCode_toluna'
                                                                                  ,'date':'completion_date_toluna'})
    
    if toluna_combined_dataset['completion_date_toluna'].isnull().any():
        toluna_combined_dataset['completion_date_toluna'] = toluna_combined_dataset['completion_date_toluna'].replace('', np.nan)
    # convert 'completion_date_toluna' from string to datetime object
    toluna_combined_dataset['completion_date_toluna'] = pd.to_datetime(toluna_combined_dataset['completion_date_toluna'], format='%m/%d/%Y %H:%M', errors='coerce')
    

    # if column 'PPRespID_atlas' contains NaN or NULL, we cant convert to int, so has to fill those rows with 0 first. If no NaN/NULL just convert to int
    if toluna_combined_dataset['PPRespID_toluna'].isnull().any():
        toluna_combined_dataset['PPRespID_toluna'] = toluna_combined_dataset['PPRespID_toluna'].fillna(0).astype(int)        
    else:
        toluna_combined_dataset['PPRespID_toluna'] = toluna_combined_dataset['PPRespID_toluna'].astype(int)  

    logging.info("Final dataframe created successfully.")
    # just to print out dataframe for checking purposes
    print(toluna_combined_dataset.to_string())
    logging.info("Completed create_dataframes().")
       
# To insert update status in database
def insert_update_status():
    logging.info("Start insert_update_status().")
    
    server = 'xxx'
    database = 'xxx'
    username = 'xxx'
    password = 'xxx'

    conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
    logging.info("Connected to SGTAMProd.")
    cursor = conn.cursor()

    # Loop through the rows of the dataframe
    for index, row in toluna_combined_dataset.iterrows():
        # Check if the ID already exists in the table
        cursor.execute("SELECT COUNT(*) FROM tSPHSurveyInformationToluna WHERE wave = 'Wave 8' AND id_toluna=?", row['id_toluna'])
        count = cursor.fetchone()[0]
        if count == 0:
            
            # PPRespID will be insert as NULL if value = 0, completion_date_atlas will be insert as NULL if date = NaT in the pandas dataframe
            # insert_values is a 'tuple', meaning an ordered collections of elements that cannot be modified after it is created
            insert_values = (   row['id_toluna'],  
                                row['PPRespID_toluna'] if row['PPRespID_toluna'] != 0 else None, 
                                row['SampleSource_toluna'],
                                row['status_toluna'],
                                row['EndPartCode_toluna'] if row['EndPartCode_toluna'] == 1 else None,  
                                row['completion_date_toluna'].to_pydatetime() if not pd.isna(row['completion_date_toluna']) else None,
                                row['completion_date_toluna'].to_pydatetime().date() if not pd.isna(row['completion_date_toluna']) else None,                      
                                datetime.now(),
                                'Wave 8'
                            )
            # Insert a new record with today's date for the 'updateDate' column
            cursor.execute("INSERT INTO tSPHSurveyInformationToluna VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", insert_values)  
        else:
            # Check if the status is different
            cursor.execute("SELECT status_toluna, end_part_code_toluna FROM tSPHSurveyInformationToluna WHERE wave = 'Wave 8' AND id_toluna=?", row['id_toluna'])
            cursor_result = cursor.fetchone()
            current_status_toluna = cursor_result[0]
            current_EndPartCode_toluna = cursor_result[1]

            if current_status_toluna != row['status_toluna'] or current_EndPartCode_toluna != row['EndPartCode_toluna']:
                
                update_values = ( 
                                    row['status_toluna'],
                                    row['EndPartCode_toluna'] if row['EndPartCode_toluna'] == 1 else None, 
                                    row['completion_date_toluna'].to_pydatetime() if not pd.isna(row['completion_date_toluna']) else None,
                                    row['completion_date_toluna'].to_pydatetime().date() if not pd.isna(row['completion_date_toluna']) else None, 
                                    datetime.today(), 
                                    row['id_toluna'] 
                                )
                # Update the status and updateDate columns
                cursor.execute("UPDATE tSPHSurveyInformationToluna SET status_toluna=?, end_part_code_toluna=?, completion_date_toluna=?, completion_date_only_toluna=?, update_date=? WHERE id_toluna=? AND wave = 'Wave 8'",
                               update_values)
                
    conn.commit()
    conn.close()
    logging.info("Completed insert_update_status().")

def archive_data_files():
    logging.info("Start archive_data_files().")
    # source and destination directories
    dataFilesLocation = 'D:/05. Data Production/SPH/IncentiveEmailAutomation/TolunaPartialStatus/data/'
    dataFilesLocationHistory = 'D:/05. Data Production/SPH/IncentiveEmailAutomation/TolunaPartialStatus/data/history/'

    # get the current datetime string
    now = datetime.now().strftime('%Y-%m-%d %H-%M-%S')

    files_to_be_moved = [
                        'Decipher_Offline_Status.xlsx',
                        'Decipher_Online_Status.xlsx'
                        ]

    # iterate over all files in the source directory
    for filename in os.listdir(dataFilesLocation):
        # check if the file is a text file
        if filename in files_to_be_moved:
            # construct the new filename with datetime string
            new_filename = f'{filename.split(".")[0]}_{now}.{filename.split(".")[1]}'
            # move the file to the destination directory with new filename
            shutil.move(os.path.join(dataFilesLocation, filename), os.path.join(dataFilesLocationHistory, new_filename))
    
    logging.info("Completed archive_data_files().")



# Main place where codes will execute
if __name__ == '__main__':
    try:
        get_toluna_dataset()
        create_dataframes()
        insert_update_status()
        archive_data_files()

    # Error, send error email    
    except Exception as e:
        print("[ERROR] There is an exception.")
        print(e)
        logging.info("[ERROR] There is an exception.")
        logging.info(e)
        print("Sending error email.")
        logging.info("Sending error email.")
        email_body = f"<p>There is an error or exception, please check.</p><p>{e}</p>"
        email_kwargs = {
            'sender':'xxx',
            'to':'xxx',
            'subject':'[ERROR] SMAS 2024 Wave 1 Toluna Status Update Insert',
            'body':email_body,
            'is_html':True
        }
        s.send_email(**email_kwargs)
        logging.info("Email sent.")
        
    # No error
    else:
        logging.info("Task Completed!")
        print("Task Completed!")
        logging.info("Sending email.")
        print("Sending email.")
        email_body = f"<p>SMAS 2024 Wave 1 Toluna status have been updated and inserted into tSPHSurveyInformationToluna successfully.</p>"
        email_kwargs = {
            'sender':'xxx',
            'to':'xxx',
            'subject':'[OK] SMAS 2024 Wave 1 Toluna Status Update Insert',
            'body':email_body,
            'is_html':True
        }
        s.send_email(**email_kwargs)
        logging.info("Email sent.")
        print("Email sent.")
    finally:
        logging.info("Finally clause completed.")
        print("Finally clause completed.")

    