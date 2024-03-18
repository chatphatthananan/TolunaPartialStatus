import pyodbc
import pandas as pd
from datetime import datetime
import logging
from SGTAMProdTask import SGTAMProd
from prettytable import PrettyTable
from tabulate import tabulate

# Set up logging
log_filename = f"D:/05. Data Production/SPH/IncentiveEmailAutomation/TolunaPartialStatus/log/SummaryEmail_logfile_{datetime.now().strftime('%Y-%m-%d %H-%M-%S')}.txt"
logging.basicConfig(filename=log_filename, level=logging.INFO)

s = SGTAMProd()

def send_survey_status_email():
    try:   
        server = 'xxx'
        database = 'xxx'
        username = 'xxx'
        password = 'xxx'

        query_atlas_temp_table = """
                                    SELECT a.ID,a.SurveyType, a.SurveyStatus, b.SegmentName,
                                    case when b.SegmentName = 'DAM' THEN 'Online DAM'
	    	                            when b.SegmentName = 'ES' and a.SurveyType = 'Online' then 'online - inhouse'
	    	                            when b.SegmentName = 'External' then 'Offline - street intercept'
	    	                            when b.SegmentName = 'ES' and a.SurveyType = 'Offline' then 'Offline - F2F'
	    	                            END as theStatus
                                    , a.CompletionDate, a.LastUpdateDate
                                    INTO #tt
                                    FROM tSPHSurveyInformation  a
                                    INNER JOIN tSPHAtlasExport b On a.ID = b.ID
                                    WHERE a.LastUpdateDate > '2023-12-12'

                                """

        query_atlas_breakdown = """
                                SELECT theStatus , CompletionDate ,
                                    COUNT(CASE WHEN SurveyStatus = 'COMPLETED' THEN 1 END) AS Completed,
                                    COUNT(CASE WHEN SurveyStatus = 'DISQUALIFIED' THEN 1 END) AS Disqualified,
                                    COUNT(CASE WHEN SurveyStatus = 'OVERQUOTA' THEN 1 END) AS OverQuota,
	                                COUNT(CASE WHEN SurveyStatus = 'ACTIVE' THEN 1 END) AS [In Progress]
                                FROM #tt  
                                GROUP BY theStatus, CompletionDate
                                ORDER BY theStatus, CompletionDate DESC

                                """

        query_atlas_total = """
                                SELECT theStatus , 
                                    COUNT(CASE WHEN SurveyStatus = 'COMPLETED' THEN 1 END) AS Completed,
                                    COUNT(CASE WHEN SurveyStatus = 'DISQUALIFIED' THEN 1 END) AS Disqualified,
                                    COUNT(CASE WHEN SurveyStatus = 'OVERQUOTA' THEN 1 END) AS OverQuota,
	                                COUNT(CASE WHEN SurveyStatus = 'ACTIVE' THEN 1 END) AS [In Progress]
                                FROM #tt  
                                GROUP BY theStatus
                                UNION ALL
                                SELECT 'Total',
                                    COUNT(CASE WHEN SurveyStatus = 'COMPLETED' THEN 1 END) AS Completed,
                                    COUNT(CASE WHEN SurveyStatus = 'DISQUALIFIED' THEN 1 END) AS Disqualified,
                                    COUNT(CASE WHEN SurveyStatus = 'OVERQUOTA' THEN 1 END) AS OverQuota,
	                                COUNT(CASE WHEN SurveyStatus = 'ACTIVE' THEN 1 END) AS [In Progress]	
                                FROM #tt

                            """

        query_toluna_breakdown = """
                                    SELECT  samplesource_toluna,
	                                        completion_date_only_toluna,
                                            COUNT(CASE WHEN [status_toluna] = 3 THEN 1 END) AS Completed,
                                            COUNT(CASE WHEN [status_toluna] = 1 THEN 1 END) AS Disqualified,
                                            COUNT(CASE WHEN [status_toluna] = 2 THEN 1 END) AS OverQouta,
	                                        COUNT(CASE WHEN [status_toluna] = 4 AND end_part_code_toluna IS NULL THEN 1 END) AS [In Progress],
	                                        COUNT(CASE WHEN [status_toluna] = 4 AND end_part_code_toluna = 1 THEN 1 END) AS [Completed Part One Only]
                                    FROM tSPHSurveyInformationToluna
                                    WHERE wave = 'Wave 8'
                                    GROUP BY samplesource_toluna, completion_date_only_toluna
                                    ORDER BY samplesource_toluna, completion_date_only_toluna DESC

                                """

        query_toluna_total =    """
                                    SELECT  samplesource_toluna,
                                            COUNT(CASE WHEN [status_toluna] = 3 THEN 1 END) AS Completed,
                                            COUNT(CASE WHEN [status_toluna] = 1 THEN 1 END) AS Disqualified,
                                            COUNT(CASE WHEN [status_toluna] = 2 THEN 1 END) AS OverQouta,
	                                        COUNT(CASE WHEN [status_toluna] = 4 AND end_part_code_toluna IS NULL THEN 1 END) AS [In Progress],
	                                        COUNT(CASE WHEN [status_toluna] = 4 AND end_part_code_toluna = 1 THEN 1 END) AS [Completed Part One Only]
                                    FROM tSPHSurveyInformationToluna
                                    WHERE wave = 'Wave 8'
                                    GROUP BY samplesource_toluna
                                    UNION all
                                    SELECT 'Total',
                                            COUNT(CASE WHEN [status_toluna] = 3 THEN 1 END) AS Completed,
                                            COUNT(CASE WHEN [status_toluna] = 1 THEN 1 END) AS Disqualified,
                                            COUNT(CASE WHEN [status_toluna] = 2 THEN 1 END) AS OverQouta,
	                                        COUNT(CASE WHEN [status_toluna] = 4 AND end_part_code_toluna IS NULL THEN 1 END) AS [In Progress],
	                                        COUNT(CASE WHEN [status_toluna] = 4 AND end_part_code_toluna = 1 THEN 1 END) AS [Completed Part One Only] 
                                    FROM tSPHSurveyInformationToluna
                                    WHERE wave = 'Wave 8'
                                """

        conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
        logging.info("Connected to SGTAMProd.")
        cursor = conn.cursor()

        # Create temp table for atlas 
        cursor.execute(query_atlas_temp_table)

        # Create table for Atlas breakdown
        cursor.execute(query_atlas_breakdown)
        result_atlas_breakdown = cursor.fetchall()
        table1_html = tabulate(result_atlas_breakdown, headers=[column[0] for column in cursor.description], tablefmt='html',numalign='left')
        table1_html = table1_html.replace('<table>', '<table style="border: solid; border-collapse: collapse; width: 50%; text-align: center;">')
        table1_html = table1_html.replace('<th>', '<th style="border:solid;">')
        table1_html = table1_html.replace('<td>', '<td style="border:solid;">')



        # Create table for Atlas total
        cursor.execute(query_atlas_total)
        result_atlas_total = cursor.fetchall()
        table2_html = tabulate(result_atlas_total, headers=[column[0] for column in cursor.description], tablefmt='html', numalign='left')
        table2_html = table2_html.replace('<table>', '<table style="border:solid; border-collapse: collapse; width: 50%; text-align: center;">')
        table2_html = table2_html.replace('<th>', '<th style="border:solid;">')
        table2_html = table2_html.replace('<td>', '<td style="border:solid;">')


        # Create table for Toluna breakdown
        cursor.execute(query_toluna_breakdown)
        result_toluna_breakdown = cursor.fetchall()
        table3_html = tabulate(result_toluna_breakdown, headers=[column[0] for column in cursor.description], tablefmt='html', numalign='left')
        table3_html = table3_html.replace('<table>', '<table style="border:solid; border-collapse: collapse; width: 65%; text-align: center;">')
        table3_html = table3_html.replace('<th>', '<th style="border:solid;">')
        table3_html = table3_html.replace('<td>', '<td style="border:solid;">')


        # Create table for Toluna total
        cursor.execute(query_toluna_total)
        result_toluna_total = cursor.fetchall()
        table4_html = tabulate(result_toluna_total, headers=[column[0] for column in cursor.description], tablefmt='html', numalign='left')
        table4_html = table4_html.replace('<table>', '<table style="border:solid; border-collapse: collapse; width: 50%; text-align: center;">')
        table4_html = table4_html.replace('<th>', '<th style="border:solid;">')
        table4_html = table4_html.replace('<td>', '<td style="border:solid;">')

        cursor.close()
        conn.close()

        # Concatenate the tables into a single string
        email_body_html = f"<html><body><h3>Atlas Total</h3>{table2_html}<br><h3>Toluna Total</h3>{table4_html}<br><h3>Atlas Breakdown</h3>{table1_html}<br><h3>Toluna Breakdown</h3>{table3_html}</body></html>"
        

        date_now = datetime.now()
        date_now_string = date_now.strftime("%Y-%m-%d")

         
        email_kwargs = {
            'sender':'xxx',
            'to':'xxx',
            'subject':f'2024 SMAS W1 Survey Status Report {date_now_string}',
            'body':email_body_html,
            'is_html':True
        }

        s.send_email(**email_kwargs)

    except Exception as e:
        error_body = f"There is an exception: {e}\nPlease check the log at 'D:\\05. Data Production\\SPH\\IncentiveEmailAutomation\\TolunaPartialStatus\\log'"
        error_email_kwargs = {
            'sender':'xxx',
            'to':'xxx',
            #'to':'SIRIKORN.CHATPHATTHANANAN@gfk.com',
            'subject':f'[ERROR] 2024 SMAS W1 Survey Status Report {date_now_string}',
            'body':error_body,
            'is_html':False
        }

        s.send_email(**error_email_kwargs)   
    finally:
        print('Finally clause executed')

# Main place where codes will execute
if __name__ == '__main__':
    send_survey_status_email()