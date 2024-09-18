# -*- coding: utf-8 -*-
"""
Created on Mon Sep 16 11:38:51 2024

@author: carlw
"""

####Import Pands and Import sqlalchemy####
import pandas as pd
from sqlalchemy import create_engine

####Checking the ODBC Driver###
import pyodbc
# List all installed ODBC drivers
drivers = pyodbc.drivers()
print("Installed ODBC Drivers:")
for driver in drivers:
    print(driver)
    
######################################## LOAD/DATATYPES PROCESS ########################################

##Category File##

#JSON File source and normalization
Raw_CA_Category = pd.read_json('C:/Users/carlw/OneDrive/Documents/YoutubeData/CA_category_id.json')
Raw_CA_Category.drop(columns=['kind', 'etag'], inplace=True)
Raw_Load_CA_Category_NormaliseColumns = pd.json_normalize(Raw_CA_Category['items'])
Load_CA_Category = pd.concat([Raw_CA_Category, Raw_Load_CA_Category_NormaliseColumns], axis=1)
Load_CA_Category.drop(columns=['items'], inplace=True)

#Check datatypes and convert if neccessary
Load_CA_Category.info()
Load_CA_Category['id'] = pd.to_numeric(Load_CA_Category['id'])

#Creating a function to take tableName and dataFrame as arguments to save space
def loadIntoSQLServer(tableName, dataFrame):
    server = 'PROTOTYPE-6'
    database = 'DWH_Python'
    ##username = 'your_username' - Don't Have - If available, will need to be added into the connection string
    ###password = 'your_password' - Don't Have - If available, will need to be added into the connection string
    table_name = tableName
      
    # Create a connection string # Removed trusted connection - connection_string = f'mssql+pyodbc://{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server;Trusted_Connection=yes'
    connection_string = f'mssql+pyodbc://{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server'
    engine = create_engine(connection_string)
    print(connection_string)
      
    # Write DataFrame to SQL Server - If_exists deleted and recreates the table
    dataFrame.to_sql(table_name, con=engine, if_exists='replace', index=False)
   
#Calling the function with the table name for the database and the table variable in Python to pass through
loadIntoSQLServer("Load_CA_Category", Load_CA_Category)

##Videos File##

#Storing the CSV File in a Dataframe    
RAW_CA_Videos = pd.read_csv('C:/Users/carlw/OneDrive/Documents/YoutubeData/CAvideos.csv')
RAW_CA_Videos.info()

#Dealing with string date values in an order yy-dd-mm to mm-dd-yy to allow conversion
from datetime import datetime

RAW_CA_Videos['trending_date'] = RAW_CA_Videos['trending_date'].str.replace('.', '-', regex=False)

def convert_date_format(date_string):
    date_obj = datetime.strptime(date_string, '%y-%d-%m')
    return date_obj.strftime('%m-%d-%y')


RAW_CA_Videos['trending_date'] = RAW_CA_Videos['trending_date'].apply(convert_date_format)

RAW_CA_Videos['trending_date'] = pd.to_datetime(RAW_CA_Videos['trending_date'])


#Calling the function with the table name for the database and the table variable in Python to pass through
loadIntoSQLServer("Load_CA_Videos", RAW_CA_Videos)


###################################### INNER JOIN PROCESS FYI ######################################

#Ensure both datatypes are int to join
RAW_CA_Videos.info()
Load_CA_Category.info()

inner_join_CA_VideoCategory = pd.merge(Load_CA_Category, RAW_CA_Videos, left_on='id', right_on='category_id', how='inner')
outer_join_CA_VideoCategory = pd.merge(Load_CA_Category, RAW_CA_Videos, left_on='id', right_on='category_id', how='outer')
left_join_CA_VideoCategory = pd.merge(Load_CA_Category, RAW_CA_Videos, left_on='id', right_on='category_id', how='left')
right_join_CA_VideoCategory = pd.merge(Load_CA_Category, RAW_CA_Videos, left_on='id', right_on='category_id', how='right')

################################## STAGING/TRANSFORMATION PROCESS ##################################

#Checking for nulls
Load_CA_Category.isnull().sum()
RAW_CA_Videos.isnull().sum()

##Staging the Category file##

#Making a copy for a cleaner variable for the staging load into SQL
Staging_CA_Category = Load_CA_Category.copy()
#Calling the function with the table name for the database and the table variable in Python to pass through
loadIntoSQLServer("Staging_CA_Category", Staging_CA_Category)      

##Staging the Videos file##

#Copy of the data
Staging_CA_Videos = RAW_CA_Videos.copy()
#Spliting the data by | which forces the data into a list i.e. []
Staging_CA_Videos['tags_cleaned'] = Staging_CA_Videos['tags'].str.split('|')    
#Removing all uneccesary double quotes
Staging_CA_Videos['tags_cleaned'] = Staging_CA_Videos['tags_cleaned'].apply(lambda x: [item.replace('""','').strip('"') for item in x])                                                                 
#Create a new column with the number of occuring tags in the list with a condition. If none then 0.
Staging_CA_Videos['No_of_tags'] = Staging_CA_Videos['tags_cleaned'].apply(lambda x: 0 if x == ['[none]'] else len(x))
#Dropping the uncleaned column from the dataframe
Staging_CA_Videos.drop(columns=['tags','tags_cleaned'], inplace=True)

#***#Issue 1 Solve - Exclude id=29 as no reference in Category file#***#
Staging_CA_Videos = Staging_CA_Videos[Staging_CA_Videos['category_id'] != 29]
                                                                      
##Converting Publich Time into DateTime- SQL Does not support this datatime for some reason##                                                                     
#Staging_CA_Videos['publish_time'] = pd.to_datetime(Staging_CA_Videos['publish_time'])
##More TimeStamp Attempts which does not work##                                                                                                                                 
#Staging_CA_Videos['publish_time'] = str(Staging_CA_Videos['publish_time'])                                                               
#Staging_CA_Videos['publish_time'] = datetime.strptime(Staging_CA_Videos['publish_time'], '%Y-%m-%dT%H:%M:%SZ')                                                              
                                                                      
                                                       
#Calling the function with the table name for the database and the table variable in Python to pass through
loadIntoSQLServer("Staging_CA_Videos", Staging_CA_Videos)                                                        
                                                                      

############################## REPETITION for the remainding files/tables ##############################
                                                                      
######################################## LOAD/DATATYPES PROCESS ########################################

##Category File##

#JSON File source and normalization
Raw_DE_Category = pd.read_json('C:/Users/carlw/OneDrive/Documents/YoutubeData/DE_category_id.json')
Raw_DE_Category.drop(columns=['kind', 'etag'], inplace=True)
Raw_Load_DE_Category_NormaliseColumns = pd.json_normalize(Raw_DE_Category['items'])
Load_DE_Category = pd.concat([Raw_DE_Category, Raw_Load_DE_Category_NormaliseColumns], axis=1)
Load_DE_Category.drop(columns=['items'], inplace=True)

#Check datatypes and convert if neccessary
Load_DE_Category.info()
Load_DE_Category['id'] = pd.to_numeric(Load_DE_Category['id'])
   
#Calling the function with the table name for the database and the table variable in Python to pass through
loadIntoSQLServer("Load_DE_Category", Load_DE_Category)

##Videos File##

#Storing the CSV File in a Dataframe    
RAW_DE_Videos = pd.read_csv('C:/Users/carlw/OneDrive/Documents/YoutubeData/DEvideos.csv')
RAW_DE_Videos.info()

#Dealing with string date values in an order yy-dd-mm to mm-dd-yy to allow conversion
from datetime import datetime

RAW_DE_Videos['trending_date'] = RAW_DE_Videos['trending_date'].str.replace('.', '-', regex=False)

def convert_date_format(date_string):
    date_obj = datetime.strptime(date_string, '%y-%d-%m')
    return date_obj.strftime('%m-%d-%y')


RAW_DE_Videos['trending_date'] = RAW_DE_Videos['trending_date'].apply(convert_date_format)

RAW_DE_Videos['trending_date'] = pd.to_datetime(RAW_DE_Videos['trending_date'])


#Calling the function with the table name for the database and the table variable in Python to pass through
loadIntoSQLServer("Load_DE_Videos", RAW_DE_Videos)   

######################################## STAGING/TRANSFORMATION PROCESS ########################################

#Checking for nulls
Load_DE_Category.isnull().sum()
RAW_DE_Videos.isnull().sum()

##Staging the Category file##

#Making a copy for a cleaner variable for the staging load into SQL
Staging_DE_Category = Load_DE_Category.copy()
#Calling the function with the table name for the database and the table variable in Python to pass through
loadIntoSQLServer("Staging_DE_Category", Staging_DE_Category)      

##Staging the Videos file##

#Copy of the data
Staging_DE_Videos = RAW_DE_Videos.copy()
#Spliting the data by | which forces the data into a list i.e. []
Staging_DE_Videos['tags_cleaned'] = Staging_DE_Videos['tags'].str.split('|')    
#Removing all uneccesary double quotes
Staging_DE_Videos['tags_cleaned'] = Staging_DE_Videos['tags_cleaned'].apply(lambda x: [item.replace('""','').strip('"') for item in x])                                                                 
#Create a new column with the number of occuring tags in the list with a condition. If none then 0.
Staging_DE_Videos['No_of_tags'] = Staging_DE_Videos['tags_cleaned'].apply(lambda x: 0 if x == ['[none]'] else len(x))
#Dropping the uncleaned column from the dataframe
Staging_DE_Videos.drop(columns=['tags','tags_cleaned'], inplace=True)

#***#Issue 1 Solve - Exclude id=29 as no reference in Category file#***#
Staging_DE_Videos = Staging_DE_Videos[Staging_DE_Videos['category_id'] != 29]
                                                                                                                            
#Calling the function with the table name for the database and the table variable in Python to pass through
loadIntoSQLServer("Staging_DE_Videos", Staging_DE_Videos)                                                        
                                                                      

######################################## LOAD/DATATYPES PROCESS ########################################

##Category File##

#JSON File source and normalization
Raw_FR_Category = pd.read_json('C:/Users/carlw/OneDrive/Documents/YoutubeData/FR_category_id.json')
Raw_FR_Category.drop(columns=['kind', 'etag'], inplace=True)
Raw_Load_FR_Category_NormaliseColumns = pd.json_normalize(Raw_FR_Category['items'])
Load_FR_Category = pd.concat([Raw_FR_Category, Raw_Load_FR_Category_NormaliseColumns], axis=1)
Load_FR_Category.drop(columns=['items'], inplace=True)

#Check datatypes and convert if neccessary
Load_FR_Category.info()
Load_FR_Category['id'] = pd.to_numeric(Load_FR_Category['id'])
   
#Calling the function with the table name for the database and the table variable in Python to pass through
loadIntoSQLServer("Load_FR_Category", Load_FR_Category)

##Videos File##

#Storing the CSV File in a Dataframe    
RAW_FR_Videos = pd.read_csv('C:/Users/carlw/OneDrive/Documents/YoutubeData/FRvideos.csv')
RAW_FR_Videos.info()

#Dealing with string date values in an order yy-dd-mm to mm-dd-yy to allow conversion
from datetime import datetime

RAW_FR_Videos['trending_date'] = RAW_FR_Videos['trending_date'].str.replace('.', '-', regex=False)

def convert_date_format(date_string):
    date_obj = datetime.strptime(date_string, '%y-%d-%m')
    return date_obj.strftime('%m-%d-%y')


RAW_FR_Videos['trending_date'] = RAW_FR_Videos['trending_date'].apply(convert_date_format)

RAW_FR_Videos['trending_date'] = pd.to_datetime(RAW_FR_Videos['trending_date'])


#Calling the function with the table name for the database and the table variable in Python to pass through
loadIntoSQLServer("Load_FR_Videos", RAW_FR_Videos)   

######################################## STAGING/TRANSFORMATION PROCESS ########################################

#Checking for nulls
Load_FR_Category.isnull().sum()
RAW_FR_Videos.isnull().sum()


##Staging the Category file##

#Making a copy for a cleaner variable for the staging load into SQL
Staging_FR_Category = Load_FR_Category.copy()
#Calling the function with the table name for the database and the table variable in Python to pass through
loadIntoSQLServer("Staging_FR_Category", Staging_FR_Category)      

##Staging the Videos file##

#Copy of the data
Staging_FR_Videos = RAW_FR_Videos.copy()
#Spliting the data by | which forces the data into a list i.e. []
Staging_FR_Videos['tags_cleaned'] = Staging_FR_Videos['tags'].str.split('|')    
#Removing all uneccesary double quotes
Staging_FR_Videos['tags_cleaned'] = Staging_FR_Videos['tags_cleaned'].apply(lambda x: [item.replace('""','').strip('"') for item in x])                                                                 
#Create a new column with the number of occuring tags in the list with a condition. If none then 0.
Staging_FR_Videos['No_of_tags'] = Staging_FR_Videos['tags_cleaned'].apply(lambda x: 0 if x == ['[none]'] else len(x))
#Dropping the uncleaned column from the dataframe
Staging_FR_Videos.drop(columns=['tags','tags_cleaned'], inplace=True)
                                                      
#***#Issue 1 Solve - Exclude id=29 as no reference in Category file#***#
Staging_FR_Videos = Staging_FR_Videos[Staging_FR_Videos['category_id'] != 29]
                                                                                                                            
#Calling the function with the table name for the database and the table variable in Python to pass through
loadIntoSQLServer("Staging_FR_Videos", Staging_FR_Videos)                                                                       

######################################## LOAD/DATATYPES PROCESS ########################################

##Category File##

#JSON File source and normalization
Raw_GB_Category = pd.read_json('C:/Users/carlw/OneDrive/Documents/YoutubeData/GB_category_id.json')
Raw_GB_Category.drop(columns=['kind', 'etag'], inplace=True)
Raw_Load_GB_Category_NormaliseColumns = pd.json_normalize(Raw_GB_Category['items'])
Load_GB_Category = pd.concat([Raw_GB_Category, Raw_Load_GB_Category_NormaliseColumns], axis=1)
Load_GB_Category.drop(columns=['items'], inplace=True)

#Check datatypes and convert if neccessary
Load_GB_Category.info()
Load_GB_Category['id'] = pd.to_numeric(Load_GB_Category['id'])

   
#Calling the function with the table name for the database and the table variable in Python to pass through
loadIntoSQLServer("Load_GB_Category", Load_GB_Category)

##Videos File##

#Storing the CSV File in a Dataframe    
RAW_GB_Videos = pd.read_csv('C:/Users/carlw/OneDrive/Documents/YoutubeData/GBvideos.csv')
RAW_GB_Videos.info()

#Dealing with string date values in an order yy-dd-mm to mm-dd-yy to allow conversion
from datetime import datetime

RAW_GB_Videos['trending_date'] = RAW_GB_Videos['trending_date'].str.replace('.', '-', regex=False)

def convert_date_format(date_string):
    date_obj = datetime.strptime(date_string, '%y-%d-%m')
    return date_obj.strftime('%m-%d-%y')


RAW_GB_Videos['trending_date'] = RAW_GB_Videos['trending_date'].apply(convert_date_format)

RAW_GB_Videos['trending_date'] = pd.to_datetime(RAW_GB_Videos['trending_date'])


#Calling the function with the table name for the database and the table variable in Python to pass through
loadIntoSQLServer("Load_GB_Videos", RAW_GB_Videos)   

######################################## STAGING/TRANSFORMATION PROCESS ########################################

#Checking for nulls
Load_GB_Category.isnull().sum()
RAW_GB_Videos.isnull().sum()

##Staging the Category file##

#Making a copy for a cleaner variable for the staging load into SQL
Staging_GB_Category = Load_GB_Category.copy()
#Calling the function with the table name for the database and the table variable in Python to pass through
loadIntoSQLServer("Staging_GB_Category", Staging_GB_Category)      

##Staging the Videos file##

#Copy of the data
Staging_GB_Videos = RAW_GB_Videos.copy()
#Spliting the data by | which forces the data into a list i.e. []
Staging_GB_Videos['tags_cleaned'] = Staging_GB_Videos['tags'].str.split('|')    
#Removing all uneccesary double quotes
Staging_GB_Videos['tags_cleaned'] = Staging_GB_Videos['tags_cleaned'].apply(lambda x: [item.replace('""','').strip('"') for item in x])                                                                 
#Create a new column with the number of occuring tags in the list with a condition. If none then 0.
Staging_GB_Videos['No_of_tags'] = Staging_GB_Videos['tags_cleaned'].apply(lambda x: 0 if x == ['[none]'] else len(x))
#Dropping the uncleaned column from the dataframe
Staging_GB_Videos.drop(columns=['tags','tags_cleaned'], inplace=True)
                                                      
#***#Issue 1 Solve - Exclude id=29 as no reference in Category file#***#
Staging_GB_Videos = Staging_GB_Videos[Staging_GB_Videos['category_id'] != 29]
                                                                                                                            
#Calling the function with the table name for the database and the table variable in Python to pass through
loadIntoSQLServer("Staging_GB_Videos", Staging_GB_Videos)                                                                       

######################################## LOAD/DATATYPES PROCESS ########################################

##Category File##

#JSON File source and normalization
Raw_IN_Category = pd.read_json('C:/Users/carlw/OneDrive/Documents/YoutubeData/IN_category_id.json')
Raw_IN_Category.drop(columns=['kind', 'etag'], inplace=True)
Raw_Load_IN_Category_NormaliseColumns = pd.json_normalize(Raw_IN_Category['items'])
Load_IN_Category = pd.concat([Raw_IN_Category, Raw_Load_IN_Category_NormaliseColumns], axis=1)
Load_IN_Category.drop(columns=['items'], inplace=True)

#Check datatypes and convert if neccessary
Load_IN_Category.info()
Load_IN_Category['id'] = pd.to_numeric(Load_IN_Category['id'])

   
#Calling the function with the table name for the database and the table variable in Python to pass through
loadIntoSQLServer("Load_IN_Category", Load_IN_Category)

##Videos File##

#Storing the CSV File in a Dataframe    
RAW_IN_Videos = pd.read_csv('C:/Users/carlw/OneDrive/Documents/YoutubeData/INvideos.csv')
RAW_IN_Videos.info()

#Dealing with string date values in an order yy-dd-mm to mm-dd-yy to allow conversion
from datetime import datetime

RAW_IN_Videos['trending_date'] = RAW_IN_Videos['trending_date'].str.replace('.', '-', regex=False)

def convert_date_format(date_string):
    date_obj = datetime.strptime(date_string, '%y-%d-%m')
    return date_obj.strftime('%m-%d-%y')


RAW_IN_Videos['trending_date'] = RAW_IN_Videos['trending_date'].apply(convert_date_format)

RAW_IN_Videos['trending_date'] = pd.to_datetime(RAW_IN_Videos['trending_date'])


#Calling the function with the table name for the database and the table variable in Python to pass through
loadIntoSQLServer("Load_IN_Videos", RAW_IN_Videos)   

######################################## STAGING/TRANSFORMATION PROCESS ########################################

#Checking for nulls
Load_IN_Category.isnull().sum()
RAW_IN_Videos.isnull().sum()

##Staging the Category file##

#Making a copy for a cleaner variable for the staging load into SQL
Staging_IN_Category = Load_IN_Category.copy()
#Calling the function with the table name for the database and the table variable in Python to pass through
loadIntoSQLServer("Staging_IN_Category", Staging_IN_Category)      

##Staging the Videos file##

#Copy of the data
Staging_IN_Videos = RAW_IN_Videos.copy()
#Spliting the data by | which forces the data into a list i.e. []
Staging_IN_Videos['tags_cleaned'] = Staging_IN_Videos['tags'].str.split('|')    
#Removing all uneccesary double quotes
Staging_IN_Videos['tags_cleaned'] = Staging_IN_Videos['tags_cleaned'].apply(lambda x: [item.replace('""','').strip('"') for item in x])                                                                 
#Create a new column with the number of occuring tags in the list with a condition. If none then 0.
Staging_IN_Videos['No_of_tags'] = Staging_IN_Videos['tags_cleaned'].apply(lambda x: 0 if x == ['[none]'] else len(x))
#Dropping the uncleaned column from the dataframe
Staging_IN_Videos.drop(columns=['tags','tags_cleaned'], inplace=True)
                                                      
#***#Issue 1 Solve - Exclude id=29 as no reference in Category file#***#
Staging_IN_Videos = Staging_IN_Videos[Staging_IN_Videos['category_id'] != 29]
                                                                                                                            
#Calling the function with the table name for the database and the table variable in Python to pass through
loadIntoSQLServer("Staging_IN_Videos", Staging_IN_Videos)

######################################## LOAD/DATATYPES PROCESS ########################################

##Category File##

#JSON File source and normalization
Raw_US_Category = pd.read_json('C:/Users/carlw/OneDrive/Documents/YoutubeData/US_category_id.json')
Raw_US_Category.drop(columns=['kind', 'etag'], inplace=True)
Raw_Load_US_Category_NormaliseColumns = pd.json_normalize(Raw_US_Category['items'])
Load_US_Category = pd.concat([Raw_US_Category, Raw_Load_US_Category_NormaliseColumns], axis=1)
Load_US_Category.drop(columns=['items'], inplace=True)

#Check datatypes and convert if neccessary
Load_US_Category.info()
Load_US_Category['id'] = pd.to_numeric(Load_US_Category['id'])

   
#Calling the function with the table name for the database and the table variable in Python to pass through
loadIntoSQLServer("Load_US_Category", Load_US_Category)

##Videos File##

#Storing the CSV File in a Dataframe    
RAW_US_Videos = pd.read_csv('C:/Users/carlw/OneDrive/Documents/YoutubeData/USvideos.csv')
RAW_US_Videos.info()

#Dealing with string date values in an order yy-dd-mm to mm-dd-yy to allow conversion
from datetime import datetime

RAW_US_Videos['trending_date'] = RAW_US_Videos['trending_date'].str.replace('.', '-', regex=False)

def convert_date_format(date_string):
    date_obj = datetime.strptime(date_string, '%y-%d-%m')
    return date_obj.strftime('%m-%d-%y')


RAW_US_Videos['trending_date'] = RAW_US_Videos['trending_date'].apply(convert_date_format)

RAW_US_Videos['trending_date'] = pd.to_datetime(RAW_US_Videos['trending_date'])


#Calling the function with the table name for the database and the table variable in Python to pass through
loadIntoSQLServer("Load_US_Videos", RAW_US_Videos)   

######################################## STAGING/TRANSFORMATION PROCESS ########################################

#Checking for nulls
Load_US_Category.isnull().sum()
RAW_US_Videos.isnull().sum()

##Staging the Category file##

#Making a copy for a cleaner variable for the staging load into SQL
Staging_US_Category = Load_US_Category.copy()
#Calling the function with the table name for the database and the table variable in Python to pass through
loadIntoSQLServer("Staging_US_Category", Staging_US_Category)      

##Staging the Videos file##

#Copy of the data
Staging_US_Videos = RAW_US_Videos.copy()
#Spliting the data by | which forces the data into a list i.e. []
Staging_US_Videos['tags_cleaned'] = Staging_US_Videos['tags'].str.split('|')    
#Removing all uneccesary double quotes
Staging_US_Videos['tags_cleaned'] = Staging_US_Videos['tags_cleaned'].apply(lambda x: [item.replace('""','').strip('"') for item in x])                                                                 
#Create a new column with the number of occuring tags in the list with a condition. If none then 0.
Staging_US_Videos['No_of_tags'] = Staging_US_Videos['tags_cleaned'].apply(lambda x: 0 if x == ['[none]'] else len(x))
#Dropping the uncleaned column from the dataframe
Staging_US_Videos.drop(columns=['tags','tags_cleaned'], inplace=True)
                                                      
#***#Issue 1 Solve - Exclude id=29 as no reference in Category file#***#
Staging_US_Videos = Staging_US_Videos[Staging_US_Videos['category_id'] != 29]
                                                                                                                            
#Calling the function with the table name for the database and the table variable in Python to pass through
loadIntoSQLServer("Staging_US_Videos", Staging_US_Videos)

# Not Used 507 - 744  ################################################ DIM TABLES ###################################################

##Import Data using the below libraries
import pandas as pd
import pyodbc

#Creating a function to take tableName and dataFrame as arguments to save space
def sourceSQLServer(server_name, database_name, sql_query):
    server = server_name
    database = database_name
    ##username = 'your_username' - Don't Have - If available, will need to be added into the connection string
    ###password = 'your_password' - Don't Have - If available, will need to be added into the connection string
    
    query = sql_query
    
    # Create a connection string # Removed trusted connection - connection_string = f'mssql+pyodbc://{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server;Trusted_Connection=yes'
    connection_string = f'mssql+pyodbc://{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server'
    engine = create_engine(connection_string)
       
    # Read data into a DataFrame
    df = pd.read_sql(query, con=engine)
    return df

#Querying data from SQL database by the functioned defined above 
merged_df = sourceSQLServer("PROTOTYPE-6", "DWH_Python", 
 """WITH dimenstion_columns_with_row_numbers AS (
SELECT DISTINCT
	scv.video_id,
	scv.title,
	scv.thumbnail_link,
	scv.description,
	scv.category_id,
	scc.etag,
	scc.[snippet.assignable],
	scc.[snippet.title],
	ROW_NUMBER() OVER(PARTITION BY scv.video_id ORDER BY scv.video_id) as Row_No
FROM
	[dbo].[Staging_CA_Videos] scv
	INNER JOIN [dbo].[Staging_CA_Category] scc ON scc.id = scv.category_id
)
SELECT *
FROM
	dimenstion_columns_with_row_numbers
WHERE Row_No = 1""")

#Adding an auto increase Identifier key
merged_df['Dim_CAVideosKey'] = range(1, len(merged_df) + 1)

#Creating a Dimension Copy of the merged assigned query
Dim_CAVideos = merged_df.copy()
Dim_CAVideos.drop(columns=['Row_No'], inplace=True)

############################### Dim Tables Continued #####################################################

##### DE

#Querying data from SQL database by the functioned defined above 
merged_df_de = sourceSQLServer("PROTOTYPE-6", "DWH_Python", 
 """WITH dimenstion_columns_with_row_numbers AS (
SELECT DISTINCT
	sdv.video_id,
	sdv.title,
	sdv.thumbnail_link,
	sdv.description,
	sdv.category_id,
	sdc.etag,
	sdc.[snippet.assignable],
	sdc.[snippet.title],
	ROW_NUMBER() OVER(PARTITION BY sdv.video_id ORDER BY sdv.video_id) as Row_No
FROM
	[dbo].[Staging_DE_Videos] sdv
	INNER JOIN [dbo].[Staging_DE_Category] sdc ON sdc.id = sdv.category_id
)
SELECT *
FROM
	dimenstion_columns_with_row_numbers
WHERE Row_No = 1""")

#Adding an auto increase Identifier key
merged_df_de['Dim_DEVideosKey'] = range(1, len(merged_df_de) + 1)

#Creating a Dimension Copy of the merged assigned query
Dim_DEVideos = merged_df_de.copy()
Dim_DEVideos.drop(columns=['Row_No'], inplace=True)

##### FR

#Querying data from SQL database by the functioned defined above 
merged_df_fr = sourceSQLServer("PROTOTYPE-6", "DWH_Python", 
 """WITH dimenstion_columns_with_row_numbers AS (
SELECT DISTINCT
	sfv.video_id,
	sfv.title,
	sfv.thumbnail_link,
	sfv.description,
	sfv.category_id,
	sfc.etag,
	sfc.[snippet.assignable],
	sfc.[snippet.title],
	ROW_NUMBER() OVER(PARTITION BY sfv.video_id ORDER BY sfv.video_id) as Row_No
FROM
	[dbo].[Staging_FR_Videos] sfv
	INNER JOIN [dbo].[Staging_FR_Category] sfc ON sfc.id = sfv.category_id
)
SELECT *
FROM
	dimenstion_columns_with_row_numbers
WHERE Row_No = 1""")

#Adding an auto increase Identifier key
merged_df_fr['Dim_FRVideosKey'] = range(1, len(merged_df_fr) + 1)

#Creating a Dimension Copy of the merged assigned query
Dim_FRVideos = merged_df_fr.copy()
Dim_FRVideos.drop(columns=['Row_No'], inplace=True)

##### GB

#Querying data from SQL database by the functioned defined above 
merged_df_gb = sourceSQLServer("PROTOTYPE-6", "DWH_Python", 
 """WITH dimenstion_columns_with_row_numbers AS (
SELECT DISTINCT
	sgv.video_id,
	sgv.title,
	sgv.thumbnail_link,
	sgv.description,
	sgv.category_id,
	sgc.etag,
	sgc.[snippet.assignable],
	sgc.[snippet.title],
	ROW_NUMBER() OVER(PARTITION BY sgv.video_id ORDER BY sgv.video_id) as Row_No
FROM
	[dbo].[Staging_GB_Videos] sgv
	INNER JOIN [dbo].[Staging_GB_Category] sgc ON sgc.id = sgv.category_id
)
SELECT *
FROM
	dimenstion_columns_with_row_numbers
WHERE Row_No = 1""")

#Adding an auto increase Identifier key
merged_df_gb['Dim_GBVideosKey'] = range(1, len(merged_df_gb) + 1)

#Creating a Dimension Copy of the merged assigned query
Dim_GBVideos = merged_df_gb.copy()
Dim_GBVideos.drop(columns=['Row_No'], inplace=True)

##### IN

#Querying data from SQL database by the functioned defined above 
merged_df_in = sourceSQLServer("PROTOTYPE-6", "DWH_Python", 
 """WITH dimenstion_columns_with_row_numbers AS (
SELECT DISTINCT
	siv.video_id,
	siv.title,
	siv.thumbnail_link,
	siv.description,
	siv.category_id,
	sic.etag,
	sic.[snippet.assignable],
	sic.[snippet.title],
	ROW_NUMBER() OVER(PARTITION BY siv.video_id ORDER BY siv.video_id) as Row_No
FROM
	[dbo].[Staging_IN_Videos] siv
	INNER JOIN [dbo].[Staging_IN_Category] sic ON sic.id = siv.category_id
)
SELECT *
FROM
	dimenstion_columns_with_row_numbers
WHERE Row_No = 1""")

#Adding an auto increase Identifier key
merged_df_in['Dim_INVideosKey'] = range(1, len(merged_df_in) + 1)

#Creating a Dimension Copy of the merged assigned query
Dim_INVideos = merged_df_in.copy()
Dim_INVideos.drop(columns=['Row_No'], inplace=True)

##### US

#Querying data from SQL database by the functioned defined above 
merged_df_us = sourceSQLServer("PROTOTYPE-6", "DWH_Python", 
 """WITH dimenstion_columns_with_row_numbers AS (
SELECT DISTINCT
	suv.video_id,
	suv.title,
	suv.thumbnail_link,
	suv.description,
	suv.category_id,
	suc.etag,
	suc.[snippet.assignable],
	suc.[snippet.title],
	ROW_NUMBER() OVER(PARTITION BY suv.video_id ORDER BY suv.video_id) as Row_No
FROM
	[dbo].[Staging_US_Videos] suv
	INNER JOIN [dbo].[Staging_US_Category] suc ON suc.id = suv.category_id
)
SELECT *
FROM
	dimenstion_columns_with_row_numbers
WHERE Row_No = 1""")

#Adding an auto increase Identifier key
merged_df_us['Dim_USVideosKey'] = range(1, len(merged_df_us) + 1)

#Creating a Dimension Copy of the merged assigned query
Dim_USVideos = merged_df_us.copy()
Dim_USVideos.drop(columns=['Row_No'], inplace=True)

############################ Creating a Fact Table based off of one Dimenstion Table ######################

fact_df = sourceSQLServer("PROTOTYPE-6", "DWH_Python", 
 """SELECT 
    scv.trending_date,
 	scv.publish_time,
	scv.comment_count,
	scv.comments_disabled,
	scv.dislikes,
	scv.likes,
	scv.No_of_tags,
	scv.views,
	scv.video_id
FROM
	[dbo].[Staging_CA_Videos] scv
ORDER BY
	video_id,
	views""")

#Merging Dim table to get the Dim Key with the common column
merged_fact_df = pd.merge(fact_df, Dim_CAVideos, left_on='video_id', right_on='video_id', how='inner')

#Adding an auto increase Identifier key
merged_fact_df['Fact_CAVideoKey'] = range(1, len(merged_fact_df) + 1)
merged_fact_df.drop(columns=['video_id', 'title', 'thumbnail_link', 'description',
                    'category_id', 'etag', 'snippet.assignable', 'snippet.title'], inplace=True)

#Storing the fact table in their own variable
Fact_VideoInfo = merged_fact_df.copy()

# Not Used 507 - 744  

################### Simplified

##Import Data using the below libraries
import pandas as pd
import pyodbc

#Creating a function to take tableName and dataFrame as arguments to save space
def sourceSQLServer(server_name, database_name, sql_query):
    server = server_name
    database = database_name
    ##username = 'your_username' - Don't Have - If available, will need to be added into the connection string
    ###password = 'your_password' - Don't Have - If available, will need to be added into the connection string
    
    query = sql_query
    
    # Create a connection string # Removed trusted connection - connection_string = f'mssql+pyodbc://{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server;Trusted_Connection=yes'
    connection_string = f'mssql+pyodbc://{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server'
    engine = create_engine(connection_string)
       
    # Read data into a DataFrame
    df = pd.read_sql(query, con=engine)
    return df

#### Dim Table

#Query to UNION all common tables and only display the unique columns without 2 names without an ID.
centralised_dim_table = sourceSQLServer("PROTOTYPE-6", "DWH_Python", 
 """WITH UnionVideo_InnerJoinCategory AS (
SELECT *, ROW_NUMBER() OVER(PARTITION BY scv.video_id ORDER BY scv.video_id) as Row_No FROM [dbo].[Staging_CA_Videos] scv INNER JOIN [dbo].[Staging_CA_Category] scc ON scc.id = scv.category_id
UNION
SELECT *, ROW_NUMBER() OVER(PARTITION BY sdv.video_id ORDER BY sdv.video_id) as Row_No FROM [dbo].[Staging_DE_Videos] sdv INNER JOIN [dbo].[Staging_DE_Category] sdc ON sdc.id = sdv.category_id
UNION
SELECT *, ROW_NUMBER() OVER(PARTITION BY sfv.video_id ORDER BY sfv.video_id) as Row_No FROM [dbo].[Staging_FR_Videos] sfv INNER JOIN [dbo].[Staging_FR_Category] sfc ON sfc.id = sfv.category_id
UNION
SELECT *, ROW_NUMBER() OVER(PARTITION BY sgv.video_id ORDER BY sgv.video_id) as Row_No FROM [dbo].[Staging_GB_Videos] sgv INNER JOIN [dbo].[Staging_GB_Category] sgc ON sgc.id = sgv.category_id
UNION
SELECT *, ROW_NUMBER() OVER(PARTITION BY siv.video_id ORDER BY siv.video_id) as Row_No FROM [dbo].[Staging_IN_Videos] siv INNER JOIN [dbo].[Staging_IN_Category] sic ON sic.id = siv.category_id
UNION
SELECT *, ROW_NUMBER() OVER(PARTITION BY suv.video_id ORDER BY suv.video_id) as Row_No FROM [dbo].[Staging_US_Videos] suv INNER JOIN [dbo].[Staging_US_Category] suc ON suc.id = suv.category_id
)
SELECT DISTINCT
	video_id,
	title,
	thumbnail_link,
	description,
	[snippet.assignable],
	[snippet.title]
FROM
	UnionVideo_InnerJoinCategory
WHERE 
	Row_No = 1 
	AND video_id != '#NAME?'
	AND video_id != '#VALUE!'
""")

#Adding an auto increase Identifier key
centralised_dim_table['Dim_VideoKey'] = range(1, len(centralised_dim_table) + 1)

#Creating a Dimension Copy of the merged assigned query
Dim_Video = centralised_dim_table.copy()

#Pipeline into SQL Server
loadIntoSQLServer("Dim_Video", Dim_Video)  

############################ Creating a Fact Table based off of one Dimenstion Table ######################

centralised_fact_table = sourceSQLServer("PROTOTYPE-6", "DWH_Python", 
 """WITH UnionVideos AS (
SELECT * FROM [dbo].[Staging_CA_Videos]
UNION
SELECT * FROM [dbo].[Staging_DE_Videos]
UNION
SELECT * FROM [dbo].[Staging_FR_Videos]
UNION
SELECT * FROM [dbo].[Staging_GB_Videos]
UNION
SELECT * FROM [dbo].[Staging_IN_Videos]
UNION
SELECT * FROM [dbo].[Staging_US_Videos]
)
SELECT
	comment_count,
	comments_disabled,
	trending_date,
	publish_time,
	dislikes,
	likes,
	No_of_tags,
	views,
	video_id
FROM
	UnionVideos
WHERE
    video_id != '#NAME?'
	AND video_id != '#VALUE!'
ORDER BY
	video_id,
	views
""")

#Merging Dim table to get the Dim Key with the common column
merged_centralised_fact_table = pd.merge(centralised_fact_table, Dim_Video, left_on='video_id', right_on='video_id', how='inner')

#Adding an auto increase Identifier key
merged_centralised_fact_table['Fact_VideoInfoKey'] = range(1, len(merged_centralised_fact_table) + 1)

merged_centralised_fact_table.drop(columns=['video_id', 'title', 'thumbnail_link', 'description',
                    'snippet.assignable', 'snippet.title'], inplace=True)

#Storing the fact table in their own variable
Fact_VideoInfo = merged_centralised_fact_table.copy()

#Pipeline into SQL Server
loadIntoSQLServer("Fact_VideoInfo", Fact_VideoInfo)  

###############################################################################################################

#Dim Date 

Dim_Date = pd.read_csv('C:/Users/carlw/Downloads/Date_Dimension_2000-2025.csv')
Dim_Date.info()

#Dealing with string date values in an order yy-dd-mm to mm-dd-yy to allow conversion
from datetime import datetime

Dim_Date['FullDateYDM'] = Dim_Date['FullDate']

#Replacing / with -
Dim_Date['FullDateMDY'] = Dim_Date['FullDateMDY'].str.replace('/', '-', regex=False)

#Converts to date type
Dim_Date['FullDateMDY'] = pd.to_datetime(Dim_Date['FullDateMDY'], format='%m-%d-%Y', errors='coerce')

#Dim_Date['FullDateMDY'] = Dim_Date['FullDateMDY'].combine_first(pd.to_datetime(Dim_Date['FullDateMDY'], format='%m-%d-%y', errors='coerce'))


#Calling the function with the table name for the database and the table variable in Python to pass through
loadIntoSQLServer("Dim_Date", Dim_Date)   

####################################### Fact + Dim Date #################################################


merged_centralised_fact_table2 = pd.merge(merged_centralised_fact_table, Dim_Date, left_on='trending_date', right_on='FullDateMDY', how='inner')

merged_centralised_fact_table2.drop(columns=["FullDate","DateName","DayOfWeek","DayNameOfWeek"
                                             ,"DayOfMonth","DayOfYear","WeekdayWeekend","WeekOfYear"
                                             ,"MonthName","MonthOfYear","IsLastDayOfMonth","CalendarQuarter"
                                             ,"CalendarYear","CalendarYearMonth","CalendarYearQtr","FiscalMonthOfYear"
                                             ,"FiscalQuarter","FiscalYear","FiscalYearMonth"
                                             ,"FiscalYearQtr","FullDateMDY"], inplace=True)


Fact_VideoInfo = merged_centralised_fact_table2.copy()

#Pipeline into SQL Server - Relationship in SQL Breaks when re-run & Fact Key have to be reasigned 
loadIntoSQLServer("Fact_VideoInfo", Fact_VideoInfo)  


#########################################################################################################

######Handling Exceptions#########

#ValueError Exception Handling   
abc = input("Please input a value :")
try:
    result = int(abc)
#except Exception as e:
except ValueError:
    print("You have entered in the incorrect value. Please try again.")

#IndexError Exception Handling      
list_ = [3, 5, 7, 3, 5]
try:
    print(list_[5])
#except Exception as e:
except IndexError:
    print("You are trying to print an index that does not exist. Please try again.")    


#TypeError and ZeroDivisionError Exception Handling      
list_ = [3, 5, 7, 3, "5", 0]
try:
    result = list_[2]/list_[5]
    print("Great! The result is : ",result)
except TypeError:
    print("One of the inputs is not the correct data type. Please try again.")    
except ZeroDivisionError:
    print("You are trying to divide by 0. Please try again.")
finally:
    print("For any technical support, please contact our hotline.")

#Creating the same code as above, but having the errors in one line of code
list_ = [3, 5, 7, 3, "5", 0]
try:
    result = list_[2]/list_[4]
    print("Great! The result is : ",result)
except (TypeError, ZeroDivisionError) as e:
    print(f"An error has occured {e}. Please try again.")    
finally:
    print("For any technical support, please contact our hotline.")

##########################################################################################

### Will only work with Pyspark defined dataframes, not pandas defined dataframes. ###

## Requires Java installation + Environmental Variable - Check ChatGPT for further info
# Importing the necessary modules from PySpark
from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder \
    .appName("MyPySparkApp") \
    .getOrCreate()

# Verify that Spark is working
print("Spark version:", spark.version)

# Stop the Spark session when done
spark.stop()

###########################################################################################

from pyspark.sql.functions import *

merged_df = Staging_CA_Videos.join(Staging_CA_Category, Staging_CA_Category['id'] == Staging_CA_Videos['category_id'], how='inner')

merged_df = Staging_CA_Videos.join(Staging_CA_Category, Staging_CA_Videos['category_id'] == Staging_CA_Category['id'], 'inner')

###########################################################################################



