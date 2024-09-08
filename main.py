import pandas as pd
from sqlalchemy import create_engine
import sys
import logging
import datetime
from datetime import date
import psycopg2
from vardata import *

def main():

    # DB VARIABLES
    SERVER_NAME = 'localhost'
    DATABASE_NAME = 'postgres'
    SCHEMA_NAME = 'postgres'
    PORT = '5432'
    USERNAME = 'postgres'
    PASSWORD = 'password123'

    # DB INITIALIZATION
    engine = create_engine(f'postgresql://{USERNAME}:{PASSWORD}@{SERVER_NAME}:{PORT}/{DATABASE_NAME}')

    # LOGGER SETUP
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    # FUNCTION: EXECUTE POSTGRESQL QUERY
    def exec_query(query):
        con = psycopg2.connect(database = DATABASE_NAME, user = USERNAME, host= SERVER_NAME, password = PASSWORD, port = PORT)
        cur = con.cursor()
        cur.execute(query)
        con.commit()
        cur.close()
        con.close()
    
    # FUNCTION: EXECUTE POSTGRES SELECT QUERY WITH RETURN VALUE
    def exec_squery(query):
        con = psycopg2.connect(database = DATABASE_NAME, user = USERNAME, host= SERVER_NAME, password = PASSWORD, port = PORT)
        cur = con.cursor()
        cur.execute(query)
        rows = cur.fetchall()
        con.commit()
        con.close()
        return rows

    # FUNCTION: INGESTION (ingest data from github csv source to postgres db)
    def ingest(url, table_name):
        try:
            df_01 = pd.read_csv(url, index_col=0)
            df_01.to_sql('raw_'+table_name, engine, if_exists='append', index=False)
        except:
            # - exits the program if there is inconsistency from the source data and provide some hint on where to validate
            logger.warning(f'error_message: Please check if csv data from url: {url} is consistent with table: {table_name}')
            sys.exit()


    # START INGESTION --------------------------------------------------- #
    print('STEP 1--------------')
    print('Start Ingestion..')

    # - ingest UID_ISO_FIPS_LookUp_Table (drop given table first)
    exec_query('drop table if exists postgres."raw_'+'UID_ISO_FIPS_LookUp_Table'+'"')
    ingest('https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/UID_ISO_FIPS_LookUp_Table.csv', 'UID_ISO_FIPS_LookUp_Table')

    # - ingest csse_covid_19_daily_reports_us with logic to loop through specific date coverage (drop given table first)
    exec_query('drop table if exists postgres."raw_'+'csse_covid_19_daily_reports_us'+'"')
    date_start = date(2021, 1, 1)
    while (date_start < date(2023, 1, 1)):
        ingest(f'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_daily_reports_us/{date_start.strftime("%m-%d-%Y")}.csv', 'csse_covid_19_daily_reports_us')
        date_start += datetime.timedelta(days=1)

    # DONE INGESTION ---------------------------------------------------- #
    print('..Done Ingestion')
    print('--------------------')


    # START CLEANUP AND BUILD FOR FINAL TABLES -------------------------- #
    print('STEP 2--------------')
    print('Start Cleanup and Build for Final Tables..')

    # - create function: final_build
    exec_query(final_build)

    # - execute function: final_build
    exec_query('SELECT postgres.final_build()')

    # DONE CLEANUP AND BUILD FOR FINAL TABLES --------------------------- #
    print('..Done Cleanup and Build for Final Tables..')
    print('--------------------')
    

    # ANALYSIS #1
    print('STEP 3.1------------')
    print('ANALYSIS #1')
    print('Below are the top 5 most common values using metric: Deaths with their frequency: (note: values with same frequency are ranked on the same level)')
    print(' - My thoughts: Identifying common values of a metric could help find data inconsistencies specially on this types of data.')
    print(' - It is given that small values would have commonality but how often would a large value be exactly the same? Specially if data are coming from different locations.')
    print('Table:')
    print('Rank, Value, Frequency')
    for row in exec_squery(analysis_1):
        print(row)
    print('--------------------')
    

    # ANALYSIS #2
    print('STEP 3.2------------')
    print('ANALYSIS #2')
    print('Using metric: Case_Fatality_Ratio, below shows the change in value over time on an aggregated monthly basis.')
    print(' - Below data shows that Fatality Ratio did not have much change within the first 6 months of 2021 then starts to decline on the last 6 months of 2021.')
    print(' - On 2022 we get to see a continuous steady decline of Fatality Ratio.')
    print(' - My thoughts: I believe that based on this data and my research it was around middle of 2021 that COVID Vaccine has been rolling out.')
    print(' - The world started to feel the effect of these Vaccines as it appears that Fatality Rate starts to decline.')
    print('Table:')
    print('Date, Value')
    for row in exec_squery(analysis_2):
        print(row)
    print('--------------------')


    # ANALYSIS #3
    print('STEP 3.3------------')
    print('ANALYSIS #3')
    print('Using metric: Confirmed as Value A and metric: Deaths as Value B, below (Table A) shows the values between the 2 metrics over time on an aggregated monthly basis.')
    print(' - The 2 metrics has a Correlation Coefficent of 0.98 (Table B) which means that they have a Positive Correlation.')
    print(' - It means that as Confirmed values increases, Deaths values also increases.')
    print(' - My thoughts: It is expected that Confirmed and Deaths would have a Positive Correlation because the more people have COVID, the higher the chamce of Death would be.')
    print('Table A:')
    print('Date, Value A, Value B')
    for row in exec_squery(analysis_3a):
        print(row)
    print('Table B:')
    print('Corr Coef')
    for row in exec_squery(analysis_3b):
        print(row)
    print('--------------------')


main()