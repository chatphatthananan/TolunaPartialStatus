from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options
import os, shutil, zipfile, time, paramiko
from SGTAMProdTask import SGTAMProd
from datetime import datetime, date
import logging

# Set up logging
log_filename = f"D:/05. Data Production/SPH/IncentiveEmailAutomation/TolunaPartialStatus/log/DownloadTolunaData_logfile_{datetime.now().strftime('%Y-%m-%d %H-%M-%S')}.txt"
logging.basicConfig(filename=log_filename, level=logging.INFO)

s = SGTAMProd()

# Set Chrome download settings, to prevent it open Windows download dialog pop up
chrome_options = Options()
chrome_options.add_experimental_option("prefs", {
    "download.prompt_for_download": False,
    "download.directory_upgrade": True,
    "safebrowsing.enabled": True
})
#driver = webdriver.Chrome(chrome_options=chrome_options, ChromeDriverManager().install())
#driver = webdriver.Chrome(ChromeDriverManager().install(), options=chrome_options)
driver = webdriver.Chrome("D:/05. Data Production/SPH/IncentiveEmailAutomation/TolunaPartialStatus/chromeDrivers/chromedriver.exe", options=chrome_options)

# urls 
login_url = "https://tolunaapac.decipherinc.com/login"
offline_data_download_page = "xxx" #need to adjust every wave
online_data_download_page = "xxx" #need to adjust every wave

try:
    # For automating Chrome to download both the online and offline dataset from toluna
    try:
             # Navigate to the webpage
        print("Navigate to Toluna website.")
        driver.get(login_url)

        # Find the accept cookie button and click it
        accept_cookie_button = WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "button.css-rpjpg8.e1hzgnqp3"))
        )
        accept_cookie_button.click()
        print("Accept cookie button clicked.")

        # Find the username and password fields and fill them in
        username_field = WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.NAME, "username"))
        )
        username_field.send_keys("xxx")
        print("Username entered.")

        password_field = WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.NAME, "password"))
        )
        password_field.send_keys("xxx")
        print("Password entered.")

        # Find the "remember computer" and "agree to policy" checkboxes and tick them, else cannot login
        checkboxes = WebDriverWait(driver, 10).until(
            EC.presence_of_all_elements_located((By.CLASS_NAME, "custom-control-label"))
        )
        for checkbox in checkboxes:
            if not checkbox.is_selected():
                checkbox.click()
        print("Checkboxes ticked.")

        # Find the submit button and click it
        submit_button = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.NAME, "submit"))
        )
        submit_button.click()
        print("Submit button clicked.")

        # navigate to offline data and download it
        driver.get(offline_data_download_page)
        excel_offline = driver.find_element(By.XPATH,'//*[@id="dataDownloads"]/div/div[3]/ul[1]/li[1]/a') # this is the "Excel" that we click to download, check again if this XPATH changes. So far no changes for W1 and W2
        excel_offline.click()
        logging.info("Clicked 'Excel' option to download the offline data file.")
        print("Clicked 'Excel' option to download the offline data file.")
        time.sleep(15)

        # navigate to online data and download it
        driver.get(online_data_download_page)
        excel_online = driver.find_element(By.XPATH,'//*[@id="dataDownloads"]/div/div[3]/ul[1]/li[1]/a') # this is the "Excel" that we click to download, check again if this XPATH changes. So far no changes for W1 and W2
        excel_online.click()
        logging.info("Clicked 'Excel' option to download the online data file.")
        print("Clicked 'Excel' option to download the online data file.")
        time.sleep(15)

    except TimeoutException as te:
        raise(te)
    except NoSuchElementException as ne:
        raise(ne)
    finally:
        # Close the browser
        driver.quit()
        logging.info("End of chrome automation code block, Chrome driver closed.")
        print("End of chrome automation code block, Chrome driver closed.")
    
    
    # For unzip and rename the files for upload in the next block
    try:
        # Go to download folder
        os.chdir('C:\\Users\\sgtamdp\\Downloads')
        logging.info("Go to download folder in KC1.")
        print("Go to download folder in KC1.")
        
        # zip files to work on
        zipfiles = ['2625812w12024offline.zip','2625812w12024online.zip'] #need to adjust every wave
        
        # move the downloaded zip files to other folder
        for file in zipfiles:
            shutil.move(f'C:\\Users\\sgtamdp\\Downloads\\{file}', 'D:\\05. Data Production\\SPH\\IncentiveEmailAutomation\\TolunaPartialStatus\\data\\placeholder_downloaded_toluna_dataset_files')
            logging.info(f"{file} moved to placeholder_downloaded_toluna_dataset_files.")
            print(f"{file} moved to placeholder_downloaded_toluna_dataset_files.")
            
        
        # change directory to the folder containing zip files
        os.chdir('D:\\05. Data Production\\SPH\\IncentiveEmailAutomation\\TolunaPartialStatus\\data\\placeholder_downloaded_toluna_dataset_files')
        logging.info("Go to place holder folder which contains the downloaded dataset files.")
        print("Go to place holder folder which contains the downloaded dataset files.")
        
        # Define the paths to the two zip files
        zip_file_offline = "D:\\05. Data Production\\SPH\\IncentiveEmailAutomation\\TolunaPartialStatus\\data\\placeholder_downloaded_toluna_dataset_files\\2625812w12024offline.zip" #need to adjust every wave
        zip_file_online = "D:\\05. Data Production\\SPH\\IncentiveEmailAutomation\\TolunaPartialStatus\\data\\placeholder_downloaded_toluna_dataset_files\\2625812w12024online.zip" #need to adjust every wave
    
        # Define the new file names for renaming later
        new_file_name_offline = f'Decipher_Offline_Data{date.today().strftime("%Y%m%d")}.xlsx'
        new_file_name_online = f'Decipher_Online_Data{date.today().strftime("%Y%m%d")}.xlsx'
    
        # Unzip the first file and rename the file inside it
        with zipfile.ZipFile(zip_file_offline, 'r') as zip_ref:
            zip_ref.extractall()
            os.rename(zip_ref.namelist()[0], new_file_name_offline)
            logging.info("Unzipped offline dataset zip file and renamed it.")
            print("Unzipped offline dataset zip file and renamed it.")
            
    
        # Unzip the second file and rename the file inside it
        with zipfile.ZipFile(zip_file_online, 'r') as zip_ref:
            zip_ref.extractall()
            os.rename(zip_ref.namelist()[0], new_file_name_online)
            logging.info("Unzipped online dataset zip file and renamed it.")
            print("Unzipped online dataset zip file and renamed it.")
        
    except Exception as e:
        #raise(f'Error in unzip code block: {e}')
        raise(e)
    finally:
        print('End of unzip code block.')
        logging.info('End of unzip code block.')
        
        
    # For upload toluna datasets to sftp and remove the downloaded files
    try:
        # SFTP settings
        sftp_hostname = 'xxx'
        sftp_username = 'xxx'
        sftp_password = 'xxx'
        sftp_port = 22
    
        # Connect to the SFTP server
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(sftp_hostname, port=sftp_port, username=sftp_username, password=sftp_password)
        print('Connected to SFTP.')
        logging.info('Connected to SFTP.')
    
        # Open an SFTP session
        sftp = ssh.open_sftp()
    
        # Upload the offline dataset
        sftp.put(new_file_name_offline, '/TAM_OGS/Toluna/' + new_file_name_offline)
        print('Uploaded offline dataset file to SFTP.')
        logging.info('Uploaded offline dataset file to SFTP.')
        
        # Upload the online dataset
        sftp.put(new_file_name_online, '/TAM_OGS/Toluna/' + new_file_name_online)
        print('Uploaded online dataset file to SFTP.')
        logging.info('Uploaded online dataset file to SFTP.')
    
        # Close the SFTP session and the SSH connection
        sftp.close()
        ssh.close()    
        print('SFTP session and connection closed.')
        logging.info('SFTP session and connection closed.')
        
    except Exception as e:
        raise(e)
    finally:
        print("End of sftp code block")
        logging.info("End of sftp code block")
    
    
    # For removing the downloaded zip files and dataset files, this is just for clean up purpose
    try:
        files_to_be_removed = ["D:\\05. Data Production\\SPH\\IncentiveEmailAutomation\\TolunaPartialStatus\\data\\placeholder_downloaded_toluna_dataset_files\\2625812w12024offline.zip"
                            ,"D:\\05. Data Production\\SPH\\IncentiveEmailAutomation\\TolunaPartialStatus\\data\\placeholder_downloaded_toluna_dataset_files\\2625812w12024online.zip"
                            ,f"D:\\05. Data Production\\SPH\\IncentiveEmailAutomation\\TolunaPartialStatus\\data\\placeholder_downloaded_toluna_dataset_files\\Decipher_Offline_Data{date.today().strftime('%Y%m%d')}.xlsx"
                            ,f"D:\\05. Data Production\\SPH\\IncentiveEmailAutomation\\TolunaPartialStatus\\data\\placeholder_downloaded_toluna_dataset_files\\Decipher_Online_Data{date.today().strftime('%Y%m%d')}.xlsx"
                            ] #need to adjust every wave
    
        for file in files_to_be_removed:
            if os.path.exists(file):
                # Delete file if exist
                os.remove(file)
                logging.info(f"{file} has been deleted.")
                print(f"{file} has been deleted.")
            else:
                logging.info(f"{file} does not exist.")
                print(f"{file} does not exist.")
    except Exception as e:
        raise(e)
    finally:
        print("End of File Removing Block.")  
        logging.info("End of File Removing Block.")        

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
        'subject':'[ERROR] SMAS 2024 Wave 1 Download Toluna Data',
        'body':email_body,
        'is_html':True,
        'filename': log_filename
    }
    s.send_email(**email_kwargs)
    logging.info("Error email sent.")
    print("Error email sent.")

# No error, send email  
#'to':'sirikorn.chatphatthananan@gfk.com,wenxuan.goh@gfk.com,dennis.khor@gfk.com,alexz.ooi@gfk.com,SinHui.Lee@gfk.com'      
else:
    print("Process completed.")
    logging.info("Process completed.")
    print("Sending email.")
    logging.info("Sending email.")
    email_body = f"<p>The data download and upload to SFTP completed successfully.</p>"
    email_kwargs = {
        'sender':'xxx',
        'to':'xxx', 
        'subject':'[OK] SMAS 2024 Wave 1 Download Toluna Data',
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