# -*- coding: utf-8 -*-
"""
Created on Thu Jul 20 06:29:08 2017
@author: swaters
"""

##########################
# Imports and setup code #
##########################


# Python version - 3.5.2  :: Anaconda 4.2.0 (64-bit)

import pandas as pd # version 0.18.1
import os 
import pymysql # version 0.7.10.None
import pyodbc #
import sqlalchemy as sa # version 1.0.13
from statistics import mode
import datetime

directory = 'dirPath'
archiveDirectory = 'archDirPath'

variables = {}

counter = 1



############################################
# Define functions for loading into pandas #
############################################

# CSV Load
def csvload(x):
    variables[counter] = pd.read_csv(x)
    
    
# .txt load - Assumes Tab Delimited   
def txtload(x):
    variables[counter] = pd.read_table(x)

# .txt load - Assumes comma delimited
def txtloadcomma(x):
    variables[counter] = pd.read_table(x, sep = ',')
    
# .json load   
def jsonload(x):
    variables[counter] = pd.read_json(x)
    

# HTML Table load:
def htmlload(url):
    variables[counter] = pd.read_html(url)
    
    
# .xlsx load - Assuming single tab (right now):
def excelload(x):
    variables[counter] = pd.read_excel(x, sheetname = 'Sheet1')
    
    
# stata load
def stataload(x):
    variables[counter] = pd.read_stata(x)
    
    
# SAS load
def sasload(x):
    variables[counter] = pd.read_sas(x)
    
    
##############################################
# Define Type Then load Data into dictionary #
##############################################

def fileLoad(file):
    if file.lower().endswith('.csv'):
        csvload(file)
    elif file.lower().endswith('.txt'):
        txtload(file)
    elif file.lower().endswith('.json'):
        jsonload(file)
    elif file.lower().endswith(tuple(['.xlsx', '.xls'])):
        excelload(file)
    elif file.lower().endswith('.dta'):
        stataload(file)
    elif file.lower().endswith(tuple(['.sas7bdat', '.xpt'])):
        sasload(file)
    elif file.lower().endswith(tuple(['html', 'htm'])):
        htmlload(file)
    else:
        print('File does not match any available formats')

    
####################################################################
# Loop through files to load into dictionary and then Archive them #
####################################################################


for filename in os.listdir(directory):
    fileLoad(directory + '//' + filename) # Load the file
    os.rename(directory + '//'+ filename, archiveDirectory + "//" + filename) # Archive the file
    counter += 1 # increment counter


#######################################
# Load data in dictionary into new DB #
#######################################

# RDBMS In example is MySQL
    
# Create Schema on the fly

dt = datetime.datetime.now()
d = dt.date()

schemaName = 'Project' + str(d)


host = 'localhost'
user = 'username'
password = 'password'
conn = pymysql.connect( host=host, user=user, passwd=password)
cursor = conn.cursor()
createSchema = ('Create Database ' + schemaName+';')
cursor.execute(createSchema)
conn.close()

# Load each key:value pair in dictionary into tables in new schema

engine = sa.create_engine('mysql+pymysql://' + user+':'+ password+'@'+host+'/'+schemaName, echo=True)

connection = engine.connect()


for k, v in variables.items():
    name = 'a'+ str(k)
    v.to_sql(name = name, con = connection, index = False)

connection.close()



#######################
# Database Connectors #
#######################


dataVariables = {}
dataCounter = 1

# MS Access (untested)
[x for x in pyodbc.drivers() if x.startswith('Microsoft Access Driver')]
# If you see an empty list then you are running 64-bit Python and you need to install
    # the 64-bit version of the "ACE" driver. If you only see  ['Microsoft Access
    # Driver (*.mdb)']  and you need to work with an  .accdb  file then you need 
    # to install the 32-bit version of the "ACE" driver

accessPath = 'C:\path\to\mydb.accdb'

conn_str = (
    r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};'
    r'DBQ=' + accessPath+';'
    )
cnxn = pyodbc.connect(conn_str)
crsr = cnxn.cursor()

sql = """
select *
from table1
"""

crsr.execute(sql)
data = crsr.fetchall()


cnxn.close()

# MySQL

host = 'localhost'
user = 'user'
password = 'password'
database = 'dbName'

conn = pymysql.connect( host=host, user=user, passwd=password, db=database )

sql = """
select *
from users
"""

data = pd.read_sql(sql, conn)
#crsr = conn.cursor()
#crsr.execute(sql)
#data = crsr.fetchall()

conn.close()




# SQL Server:
# {SQL Server} - released with SQL Server 2000
# {SQL Native Client} - released with SQL Server 2005 (also known as version 9.0)
# {SQL Server Native Client 10.0} - released with SQL Server 2008
# {SQL Server Native Client 11.0} - released with SQL Server 2012
# {ODBC Driver 11 for SQL Server} - supports SQL Server 2005 through 2014
# {ODBC Driver 13 for SQL Server} - supports SQL Server 2005 through 2016
# Note that the "SQL Server Native Client ..." and earlier drivers are deprecated and should not be used for new development.


# MS SQL Server - Windows - NonDSN (untested)

conn = pyodbc.connect(
    r'DRIVER={ODBC Driver 11 for SQL Server};'
    r'SERVER=test;'
    r'DATABASE=test;'
    r'UID=user;'
    r'PWD=password'
    )


sql = """
select *
from users
"""

data = pd.read_sql(sql, conn)


conn.close()


# MS SQL Server - Windows - DSN - Trusted Connection (untested)

conn = pyodbc.connect(
    r'DRIVER={ODBC Driver 11 for SQL Server};'
    r'SERVER=test;'
    r'DATABASE=test;'
    r'Trusted_Connection=yes;'
    )

sql = """
select *
from users
"""

data = pd.read_sql(sql, conn)

conn.close()



# MS SQL Server - Windows - DSN - Credentials (untested)
conn = pyodbc.connect(r'DSN=mynewdsn;UID=user;PWD=password')


sql = """
select *
from users
"""

data = pd.read_sql(sql, conn)

conn.close()


# MS SQL Server - Mac/OS X (Untested)
import pyodbc
# the DSN value should be the name of the entry in odbc.ini, not freetds.conf
conn = pyodbc.connect('DSN=MYMSSQL;UID=myuser;PWD=mypassword')
crsr = conn.cursor()
sql = """
select *
from users
"""

data = pd.read_sql(sql, conn)
conn.close()


# Postgres (untested)

server = 'localhost'
database = 'databaseName'
uid = 'UID'
pwd = 'password'
connection = pyodbc.connect("DRIVER={psqlOBDC};SERVER="+server+";DATABASE="+database+";UID="+uid+";PWD="+pwd)

crsr = conn.cursor()
sql = """
select *
from users
"""

data = pd.read_sql(sql, conn)
conn.close()



dataVariables[dataCounter] = data


# Create Schema on the fly

dt = datetime.datetime.now()
d = dt.date()

schemaName = 'Project' + str(d)

host = 'localhost'
user = 'username'
password = 'password'

conn = pymysql.connect( host=host, user=user, passwd=password)
cursor = conn.cursor()
createSchema = ('Create Database ' + schemaName+';')
cursor.execute(createSchema)
conn.close()

# Load each key:value pair in dictionary into tables in new schema

engine = sa.create_engine('mysql+pymysql://'+user+':'+ password+'@'+host+'/'+schemaName, echo=True)

connection = engine.connect()


for k, v in dataVariables.items():
    name = 'a'+ str(k)
    v.to_sql(name = name, con = connection, index = False)

connection.close()
