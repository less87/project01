# Readme


### Setup instruction:

**1. Setup localhost for postgres database**
 - download and install postgres (https://www.enterprisedb.com/downloads/postgres-postgresql-downloads)
 - installation process would also setup a localhost by default, to check: click windows search and open "SQL Shell (psql)"
 - default database credentials would be: server: localhost, database: postgres, port: 5432, user: postgres, pw: <created password upon postgres installation>
 - download and install dbeaver (https://dbeaver.io/) as my choice of IDE for database
 - using dbeaver, open db: localhost then create a schema as "postgres"
 - take note of the below updated db credentials as they will be used later on python setup:
     - server: localhost
     - database: postgres
     - schema: postgres
     - port: 5432
     - user: postgres
     - pw: (created password upon postgres installation)

**2. Setup python code**
 - download and install python (https://www.python.org/downloads/) if needed
 - download and install vscode as my chosen IDE (https://code.visualstudio.com/download) if needed
 - download or clone my solution from github repo url: https://github.com/less87/project01.git
 - open project01 via vscode
 - notes: make sure to update "DB VARIABLES" within main.py to ensure the correct variables to be used upon database access
 - run below codes on Terminal:
     - pip3 install pipenv (to install pipenv if system has new python installation)
     - pipenv shell (to create virtial environment)
     - pipenv install -r ./requirements.txt (to install needed packages)
     - python main.py (to run the solution with analytics output provided also on the terminal)

<br />

### Design and technologies used:

**Design:**
 - I used python coding to run the whole solution in a single code as long as the setup instructions were followed
 - The python code would process: Ingestion, Cleanup and Build and Analysis with output showing on the terminal on every step of the process
 - (note: Cleanup and Build solution script: query_final_build.sql or vardata.py variable: final_build)
 - Analysis 1 to 3 will be available on the terminal as output with explanation and sample tables
 - Script used on each cleanup build and analysis is available to be reviewed as variables on vardata.py, they are also availale as .sql file on the root folder
 - I chose to design my solution this way so that I can focus on my strength which is postgesql scripting (for data cleanup/build, data processing) and python as part of the requirements and also to challenge myself to provide the output based on my current knowledge and experience (one time python project experience)

 **Technology:**
 - I mainly used python for coding as it is mentioned on the tech constraints and also because based on my research, python is currently being used as one of the latest tech in data integration within Data Engineering
 - I used PostgreSQL for database as it is what I am mostly experienced and familiar with as of the moment
 - I also used PostgreSQL (instead of dbt) for Data Cleanup, Build and Processing as it is my current strength and experienced when it comes to processing raw data for a usable output
 - I do not have any experience using data orchestration tools and containers though I have enough knowledge for their usage, I can probably be able to provide a solution with these technologies but I would have to do some research and I'm afraid I will not be able to provide a solution as early as possible

<br />

### Answers to Data Analysis questions:
**Notes:**
 - below analysis is also an output result via terminal
 - ingestion data covers 2021-2022 based on my chosen data to process thus my analysis is also based from

**ANALYSIS #1**  
Question: What are the top 5 most common values in a particular column, and what is their frequency?  
(solution script: query_analysis_1.sql or vardata.py variables: analysis_1)  
Below are the top 5 most common values using metric: Deaths with their frequency: (note: values with same frequency are ranked on the same level)  
 - My thoughts: Identifying common values of a metric could help find data inconsistencies specially on this types of data.
 - It is given that small values would have commonality but how often would a large value be exactly the same? Specially if data are coming from different locations.  
Table:  
Rank, Value, Frequency  
(1, 0, 35)  
(2, 3, 24)  
(3, 2, 9)  
(4, 34, 4)  
(5, 1665, 3)  
(5, 2671, 3)  

**ANALYSIS #2**  
Question: How does a particular metric change over time within the dataset?  
(solution script: query_analysis_2.sql or vardata.py variables: analysis_2)  
Using metric: Case_Fatality_Ratio, below shows the change in value over time on an aggregated monthly basis.  
 - Below data shows that Fatality Ratio did not have much change within the first 6 months of 2021 then starts to decline on the last 6 months of 2021.
 - On 2022 we get to see a continuous steady decline of Fatality Ratio.
 - My thoughts: I believe that based on this data and my research it was around middle of 2021 that COVID Vaccine has been rolling out.
 - The world started to feel the effect of these Vaccines as it appears that Fatality Rate starts to decline.  
Table:  
Date, Value  
('2021-01-01', 1.63)  
('2021-02-01', 1.68)  
('2021-03-01', 1.67)  
('2021-04-01', 1.62)  
('2021-05-01', 1.62)  
('2021-06-01', 1.63)  
('2021-07-01', 1.6)  
('2021-08-01', 1.51)  
('2021-09-01', 1.48)  
('2021-10-01', 1.46)  
('2021-11-01', 1.45)  
('2021-12-01', 1.36)  
('2022-01-01', 1.09)  
('2022-02-01', 1.1)  
('2022-03-01', 1.12)  
('2022-04-01', 1.13)  
('2022-05-01', 1.11)  
('2022-06-01', 1.08)  
('2022-07-01', 1.05)  
('2022-08-01', 1.03)  
('2022-09-01', 1.03)  

**ANALYSIS #3**  
Question: Is there a correlation between two specific columns? Explain your findings.  
(solution script: query_analysis_3a.sql and query_analysis_3b.sql or vardata.py variables: analysis_3a and analysis_3b)  
Using metric: Confirmed as Value A and metric: Deaths as Value B, below (Table A) shows the values between the 2 metrics over time on an aggregated monthly basis.  
 - The 2 metrics has a Correlation Coefficent of 0.98 (Table B) which means that they have a Positive Correlation.
 - It means that as Confirmed values increases, Deaths values also increases.
 - My thoughts: It is expected that Confirmed and Deaths would have a Positive Correlation because the more people have COVID, the higher the chamce of Death would be.  
Table A:  
Date, Value A, Value B  
('2021-01-01', 26357091, 7700)  
('2021-02-01', 28764318, 8825)  
('2021-03-01', 30579513, 9454)  
('2021-04-01', 32468089, 9865)  
('2021-05-01', 33386477, 10177)  
('2021-06-01', 33782334, 10357)  
('2021-07-01', 35107429, 10508)  
('2021-08-01', 39395073, 10996)  
('2021-09-01', 43539654, 12015)  
('2021-10-01', 46053432, 12843)  
('2021-11-01', 48605948, 13438)  
('2021-12-01', 54907805, 14232)  
('2022-01-01', 75244271, 15326)  
('2022-02-01', 79182144, 16385)  
('2022-03-01', 80221740, 16951)  
('2022-04-01', 81466601, 17174)  
('2022-05-01', 84327712, 17363)  
('2022-06-01', 87675433, 17541)  
('2022-07-01', 91345438, 17760)  
('2022-08-01', 94528872, 18039)  
('2022-09-01', 95771141, 18176)  
Table B:  
Corr Coef  
(0.98,)  

