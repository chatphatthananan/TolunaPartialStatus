import pandas as pd
from datetime import date, datetime
import pysftp
import re
import os
import logging
from SGTAMProdTask import SGTAMProd

# Set up logging
log_filename = f"D:/05. Data Production/SPH/IncentiveEmailAutomation/TolunaPartialStatus/log/FixTolunaDatasetsMissingIDs_logfile_{datetime.now().strftime('%Y-%m-%d %H-%M-%S')}.txt"
logging.basicConfig(filename=log_filename, level=logging.INFO)

s = SGTAMProd()

try:    

    # Generate today's date in the desired format
    today = date.today().strftime("%Y%m%d")

    # ------------------------------------------------------------------------------------------------------------------------------------------------------------
    # Get Toluna dataset files that were downloaded from Toluna website and uploaded to SFTP
    # ------------------------------------------------------------------------------------------------------------------------------------------------------------

    logging.info('Start of download data files from SFTP code block.')
    print('Start of download data files from SFTP code block.')

    sftp_hostname = 'xxx'
    sftp_username = 'xxx'
    sftp_password = 'xxx'
    remote_directory = '/TAM_OGS/Toluna/'

    try:
        local_directory = 'D:\\05. Data Production\\SPH\\IncentiveEmailAutomation\\TolunaPartialStatus\\data\\placeholder_downloaded_datafiles_for_fix_missing_values\\'
        prepop_data_directory = 'D:\\05. Data Production\\SPH\\IncentiveEmailAutomation\\TolunaPartialStatus\\data\\prepop_data_for_fix_missing_values\\'

        cnopts = pysftp.CnOpts()
        cnopts.hostkeys = None
        # Specify fileName patterns and corresponding names to save
        filePatternsNames = {
            r'^Decipher_Online_Data': 'Decipher_Online_Data.xlsx',
            r'^Decipher_Offline_Data': 'Decipher_Offline_Data.xlsx'
        }

        logging.info('Establishing connection to SFTP')
        print('Establishing connection to SFTP')
        # establish the SFTP connection
        with pysftp.Connection(sftp_hostname, username=sftp_username, password=sftp_password, cnopts=cnopts) as sftp:
            logging.info('Connected to SFTP.')
            print('Connected to SFTP.')
            
            # change to the remote directory
            sftp.cwd(remote_directory)
            logging.info(f'Go to {remote_directory} in SFTP.')
            print(f'Go to {remote_directory} in SFTP.')
            
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
                sftp.get(latest_file, os.path.join(local_directory, fileName))
                logging.info(f'{local_directory+fileName} is downloaded.')
                print(f'{local_directory+fileName} is downloaded.')
    except Exception as e:
        raise(e)
    finally:
        logging.info('End of Download data files from SFTP code block.')
        print('End of Download data files from SFTP code block.')
    # ------------------------------------------------------------------------------------------------------------------------------------------------------------



    # ------------------------------------------------------------------------------------------------------------------------------------------------------------
    # Fix the Toluna data and export out to excel files again
    # ------------------------------------------------------------------------------------------------------------------------------------------------------------
    try:    

        logging.info('Start of create dataframes to fix the data code block.')
        print('Start of create dataframes to fix the data code block.')

        # Specify columns for dataframes
        columns_to_read_For_OnlineOfflineDataset = ['record','uuid','date','id_temp','ID','status','EndPartCode','markers']
        columns_to_read_For_PrePopulatedDataset = ['ID','SampleSource','PPRespID','IsTest']
        columns_arrangement_for_fixed_datasets = ['record','uuid','date','id_temp','ID','status','EndPartCode','SampleSource','PPRespID','IsTest','markers']

        # load your Excel file
        logging.info('Creating required dataframes.')
        print('Creating required dataframes.')
        df_online = pd.read_excel(f'{local_directory}Decipher_Online_Data.xlsx', usecols=columns_to_read_For_OnlineOfflineDataset)
        df_offline = pd.read_excel(f'{local_directory}Decipher_Offline_Data.xlsx', usecols=columns_to_read_For_OnlineOfflineDataset)
        df_online_prepop = pd.read_excel(f'{prepop_data_directory}SampleDatasetForPrePopulation_SMAS2024_W1.xlsx', usecols=columns_to_read_For_PrePopulatedDataset, sheet_name='Online')
        df_offline_prepop = pd.read_excel(f'{prepop_data_directory}SampleDatasetForPrePopulation_SMAS2024_W1.xlsx', usecols=columns_to_read_For_PrePopulatedDataset, sheet_name='Offline')


        # fill missing values in 'id_temp' column with corresponding values from 'ID'
        logging.info('Copy values from ID column to id_temp column if there is missing value.')
        print('Copy values from ID column to id_temp column if there is missing value.')
        df_online['id_temp'].fillna(df_online['ID'], inplace=True)
        df_offline['id_temp'].fillna(df_offline['ID'], inplace=True)

        # Inner join datasets dfs with prepopulated data dfs to get the required columns
        logging.info('Inner join dataset df with prepopulated df to get required columns.')
        print('Inner join dataset df with prepopulated df to get required columns.')
        df_online_fixed = df_online.merge(df_online_prepop[['ID','SampleSource','PPRespID','IsTest']], on='ID', how='inner')
        df_offline_fixed = df_offline.merge(df_offline_prepop[['ID','SampleSource','PPRespID','IsTest']], on='ID', how='inner')


        # Reindex the dataframe with the desired column order
        logging.info('Rearrange the columns of the final dfs.')
        print('Rearrange the columns of the final dfs.')
        df_online_fixed = df_online_fixed.reindex(columns=columns_arrangement_for_fixed_datasets)
        df_offline_fixed = df_offline_fixed.reindex(columns=columns_arrangement_for_fixed_datasets)


        # Save your DataFrame back to Excel
        logging.info('Export final results dfs as excel file to be uploaded to SFTP.')
        print('Export final results dfs as excel file to be uploaded to SFTP')
        df_online_fixed.to_excel(f"{local_directory}Decipher_Online_Data.xlsx", index=False)
        df_offline_fixed.to_excel(f"{local_directory}Decipher_Offline_Data.xlsx", index=False)

    except Exception as e:
        raise(e)
    finally:
        logging.info('End of create dataframes to fix the data code block.')
        print('End of create dataframes to fix the data code block.')
    # ------------------------------------------------------------------------------------------------------------------------------------------------------------


    # ------------------------------------------------------------------------------------------------------------------------------------------------------------
    # Upload the fixed data files back to SFTP again for the status update/insert process 
    # ------------------------------------------------------------------------------------------------------------------------------------------------------------
    try:
        logging.info('Start of upload data files back to SFTP.')
        print('Start of upload data files back to SFTP.')
        # Your list of files to upload and their new names. Replace with your actual list.
        files_to_upload = [(f'{local_directory}Decipher_Online_Data.xlsx', f'{local_directory}Decipher_Online_Data{today}.xlsx'), (f'{local_directory}Decipher_Offline_Data.xlsx', f'{local_directory}Decipher_Offline_Data{today}.xlsx')]

        cnopts = pysftp.CnOpts()
        cnopts.hostkeys = None  # disable host key checking.

        with pysftp.Connection(host=sftp_hostname, username=sftp_username, password=sftp_password, cnopts=cnopts) as sftp:
            logging.info('Connected to SFTP.')
            print('Connected to SFTP.')
            sftp.chdir(remote_directory)  # change directory to the remote directory
            logging.info(f'Go to {remote_directory}.')
            print(f'Go to {remote_directory}.')
            for file, new_name in files_to_upload:
                os.rename(file, new_name)  # rename file
                logging.info(f'Renamed {file} to {new_name}')
                print(f'Renamed {file} to {new_name}')
                
                sftp.put(new_name)  # upload file
                logging.info(f'Uploaded {new_name} to SFTP.')
                print(f'Uploaded {new_name} to SFTP.')
                
                os.remove(new_name)
                logging.info(f'Removed {new_name} from placeholder folder.')
                print(f'Removed {new_name} from placeholder folder.')

    except Exception as e:
        raise(e)
    finally:
        logging.info('End of upload data files back to SFTP.')
        print('End of upload data files back to SFTP.')

# ------------------------------------------------------------------------------------------------------------------------------------------------------------

# Error, sending error email
except Exception as e:
    print("[ERROR] There is an exception.")
    logging.info("[ERROR] There is an exception.")
    print(e)
    logging.info(e)
    print("Sending error email.")
    logging.info("Sending error email.")
    email_body = f"<p>There is an error or exception, please check the attached logfile.</p><p>{e}</p>"
    email_kwargs = {
        'sender':'xxx',
        'to':'xxx',
        'subject':'[ERROR] FixTolunaDatasetsMissingIDs',
        'body':email_body,
        'is_html':True,
        'filename': log_filename
    }
    s.send_email(**email_kwargs)
    logging.info("Error email sent.")
    print("Error email sent.")

# No error, send email  
else:
    print("Process completed.")
    logging.info("Process completed.")
    print("Sending email.")
    logging.info("Sending email.")
    email_body = f"<p>Toluna datasets have been fixed and reuploaded back to the SFTP.</p>"
    email_kwargs = {
        'sender':'xxx',
        'to':'xxx',
        'subject':'[OK] Fix Toluna Datasets Missing IDs',
        'body':email_body,
        'is_html':True,
        'filename': log_filename
    }
    s.send_email(**email_kwargs)
    logging.info("Email sent.")
    print("Email sent.")

finally:
    logging.info('This is finally clause, end of process.')
    print('This is finally clause, end of process.')