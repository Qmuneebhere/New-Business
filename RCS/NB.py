import os
import glob
import pandas as pd
from sqlalchemy import create_engine


section1 = '\n\n' + '***********************************************************' + '\n\n'


##########################################################################################
##                                                                                      ##
##           This function gets account list from given filename, and returns           ##
##                 a string of accounts which can be used by SQL query.                 ##
##                                                                                      ##
##########################################################################################

def Accounts(path, name):

    df = pd.read_csv(path + name, dtype=str)
    
    # Converts column of AcctID to a list
    # with inverted commas around AcctID's

    AcctIDList = (df['AcctID'].apply(lambda id: f"'{id}'")).tolist()

    # Converts the list to a string for SQL query

    AcctIDs = ','.join(AcctIDList)

    return AcctIDs


##########################################################################################
##                                                                                      ##
##          This function reads data from csv file, combines first & last name          ##
##                                and returns dataframe.                                ##
##                                                                                      ##
##########################################################################################

def dataCSV(path, name, cols):

    df = pd.read_csv(path + name, dtype=str)

    # Filters only required column

    df = df[cols]

    # Concatename First and Last name column into a new column NAME

    df['NAME'] = df.apply(lambda row: f"{row['LastName']},{row['FirstName']}", axis=1)
    
    # Sets AcctID as index
    
    df.set_index('AcctID', inplace=True)
    
    # Rearranges column
    
    df = df[SQLCols]
    
    # sorts dataframe according to index
    
    df.sort_index(inplace=True)
    
    return df

    
##########################################################################################
##                                                                                      ##
##           This function fetches data from SQL for required account numbers           ##
##           & given date. Creates a dataframe for specific columns required.           ##
##                                                                                      ##
##########################################################################################

def dataSQL(date, accString):

    # Configuration for SQL engine

    server = 'unifin-sql'
    usr = 'read_only'
    pwd = 'Neustar01'
    db = 'tiger'
    driver = 'ODBC+Driver+17+for+SQL+Server'

    config = f"mssql+pyodbc://{usr}:{pwd}@{server}/{db}?driver={driver}"

    engine = create_engine(config)

    # -----------------------DataFrame One----------------------- #

    # Query for fetching Account Number

    query1 = "SELECT DBR_CLI_REF_NO AS 'AcctID', \
              RIGHT(ADR_NAME, LEN(ADR_NAME) - 1) AS 'Acctnumber' \
              FROM CDS.DBR INNER JOIN CDS.ADR ON DBR_NO = ADR_DBR_NO \
              WHERE DBR_CLI_REF_NO IN (" + accString + ") AND ADR_SEQ_NO = 'R2' \
              AND DBR_ASSIGN_DATE_O = " + f"'{date}'"

    dfOne = pd.read_sql(query1, engine, dtype=str)

    dfOne.set_index('AcctID', inplace=True)

    # -----------------------DataFrame Two----------------------- #

    # Query for fetching Name, SSN, Address, DOB

    query2 = "SELECT DBR_CLI_REF_NO AS 'AcctID', \
              DBR_NAME1 AS 'NAME', \
              ADR_TAX_ID AS 'SSN', \
              ADR_ADDR1 AS 'Address', \
              ADR_ADDR2 AS 'Address2', \
              ADR_CITY AS 'City', \
              ADR_STATE AS 'State', \
              ADR_ZIP_CODE AS 'Zip', \
              ADR_DOB_O AS 'DateOfBirth' \
              FROM CDS.DBR INNER JOIN CDS.ADR ON DBR_NO = ADR_DBR_NO \
              WHERE DBR_CLI_REF_NO IN (" + accString + ") AND ADR_SEQ_NO = '01' \
              AND DBR_ASSIGN_DATE_O = " + f"'{date}'"
    
    dfTwo = pd.read_sql(query2, engine, dtype=str)

    dfTwo.set_index('AcctID', inplace=True)

    # ----------------------DataFrame Three---------------------- #

    # Query for Home Phone & Cell Phone

    query3 = "SELECT DBR_CLI_REF_NO AS 'AcctID', \
              ADR_PHONE1 AS 'HomePhone', \
              ADR_PHONE2 AS 'CellPhone' \
              FROM CDS.DBR INNER JOIN CDS.ADR ON DBR_NO = ADR_DBR_NO \
              WHERE DBR_CLI_REF_NO IN (" + accString + ") AND ADR_SEQ_NO = '01' \
              AND DBR_ASSIGN_DATE_O = " + f"'{date}'"

    dfThree = pd.read_sql(query3, engine, dtype=str)

    dfThree.set_index('AcctID', inplace=True)

    # -----------------------DataFrame Four---------------------- #

    # Query for Work Phone & Other Phone

    query4 = "SELECT DBR_CLI_REF_NO AS 'AcctID', \
              ADR_PHONE1 AS 'WorkPhone', \
              ADR_PHONE2 AS 'OtherPhone' \
              FROM CDS.DBR INNER JOIN CDS.ADR ON DBR_NO = ADR_DBR_NO \
              WHERE DBR_CLI_REF_NO IN (" + accString + ") AND ADR_SEQ_NO = '11' \
              AND DBR_ASSIGN_DATE_O = " + f"'{date}'"

    dfFour = pd.read_sql(query4, engine, dtype=str)

    dfFour.set_index('AcctID', inplace=True)
    
    # -----------------------DataFrame Five---------------------- #

    # Query for Other DBR columns

    query5 = "SELECT DBR_CLI_REF_NO AS 'AcctID', \
              DBR_ASSIGN_DATE_O AS 'EffectiveDate', \
              DBR_ASSIGN_AMT AS 'CurrBalance', \
              DBR_CL_DATES_3_O AS 'LastPayDate', \
              DBR_CL_CODES_3 AS 'LastPayAmt', \
              DBR_CL_MISC_3 AS 'PlaceBatchID', \
              DBR_CL_CODES_1 AS 'ChgOffBalance', \
              DBR_LAST_CHG_DATE_O AS 'ChgOffDate', \
              DBR_CL_MISC_1 AS 'OwnerName', \
              DBR_CL_MISC_2 AS 'OriginalCreditor', \
              DBR_CL_DATES_2_O AS 'OutOfStatuteDate', \
              DBR_NegNotice_FLAG AS 'SendCBRNegNotice', \
              DBR_2_FLAG AS 'SendOOSNotice', \
              DBR_CL_MISC_2 AS 'ChargeOffCreditorName' \
              FROM CDS.DBR WHERE DBR_CLI_REF_NO IN (" + accString + ") \
              AND DBR_ASSIGN_DATE_O = " + f"'{date}'"

    dfFive = pd.read_sql(query5, engine, dtype=str)

    dfFive.set_index('AcctID', inplace=True)

    # ---------------------Merging DataFrames-------------------- #

    merged_df = pd.concat([dfOne, dfTwo, dfThree, dfFour, dfFive], axis=1)
    
    # sorts dataframe's columns
    
    sorted_df = merged_df[SQLCols]
    
    # sorts dataframe according to index
    
    sorted_df.sort_index(inplace=True)

    return sorted_df


##########################################################################################
##                                                                                      ##
##               This function coompares data from SQL & CSV and returns                ##
##                               a comparison dataframe.                                ##
##                                                                                      ##
##########################################################################################

def Comparison(dfCSV, dfSQL):

    # Renames both Dataframes

    df1 = dfCSV.add_suffix('_CSV')

    df2 = dfSQL.add_suffix('_SQL')

    # Columns for comparison

    cols = []

    for col in SQLCols: cols = cols + [col + '_CSV', col + '_SQL']

    comparison_df = pd.concat([df1, df2], axis=1)
    
    # Rearranges columns

    compare_df = comparison_df[cols]
    
    compare_df.reset_index(inplace=True)
    
    return compare_df


# ---------------------------------List of All Columns----------------------------------- #


CSVCols = ["AcctID", "Acctnumber", "SSN", 
           "FirstName", "LastName", "Address", "Address2", "City", "State", "Zip", 
           "DateOfBirth", "HomePhone", "WorkPhone", "OtherPhone", "CellPhone", 
           "EffectiveDate", "CurrBalance", "LastPayDate", "LastPayAmt", "PlaceBatchID", 
           "ChgOffBalance", "ChgOffDate", "OwnerName", "OriginalCreditor", 
           "OutOfStatuteDate", "SendCBRNegNotice", "SendOOSNotice", 
           "ChargeOffCreditorName"]


SQLCols = ["Acctnumber", "SSN", 
           "NAME", "Address", "Address2", "City", "State", "Zip", 
           "DateOfBirth", "HomePhone", "WorkPhone", "OtherPhone", "CellPhone", 
           "EffectiveDate", "CurrBalance", "LastPayDate", "LastPayAmt", "PlaceBatchID", 
           "ChgOffBalance", "ChgOffDate", "OwnerName", "OriginalCreditor", 
           "OutOfStatuteDate", "SendCBRNegNotice", "SendOOSNotice", 
           "ChargeOffCreditorName"]


# ----------------------------------Gets today's date------------------------------------ #

print(section1)

print('Welcome To RCS New Business QA.'.center(60))
print('Enter the date of files you want to check.'.center(60))

date = input('\nDate (YYYY-MM-DD): ')

# splits date into year, month, day

dateInfo = date.split('-')

year = dateInfo[0]
month = dateInfo[1]
day = dateInfo[2]

# Folder name in which file is placed

folderName = f'{month}-{day}-{year}'


# ---------------------------------Folder path - Analytics------------------------------- #


# Path to NB folder in Analytics

NBPath = f'N:\\Analytics\\New Business Notifications\\New Business Files\\NB\\'

# Path to current folder in NB

currFolder = f'{NBPath}Resurgent\\{folderName}\\'


# -------------------------------Gets Filename of Given Date----------------------------- #


pattern = '*.csv'

# Changes directory to current folder

os.chdir(currFolder)

# Gets List of csv Files

fileNames = glob.glob(f'{pattern}')


# -------------------------Creates a dictionary of files with index---------------------- #


# Creates a list of numbers equal to files in folder

numFiles = list(range(1, len(fileNames) + 1))

# Creates a dictionary of filenames with index

dictFiles = dict(zip(numFiles, fileNames))


# -------------------------Gets FileName for which QA is to be done---------------------- #


print(section1)

print('Enter the No. of File on which you want to perform QA:'.center(60) + '\n')

for index, name in dictFiles.items():

    print(f'{index}. {name}'.center(60))

num = input('\nFile #: ')

# Gets the fileName

fileName = dictFiles[int(num)]


# ----------------------------Gets Accounts List from given File------------------------- #


accID = Accounts(currFolder, fileName)


# ------------------------------Gets DataFrame from SQL table---------------------------- #


dfSQL = dataSQL(date, accID)


# ------------------------------Gets DataFrame from CSV Files---------------------------- #


dfCSV = dataCSV(currFolder, fileName, CSVCols)


# --------------------------------Gets Comparison DataFrame------------------------------ #


dfCompare = Comparison(dfCSV, dfSQL)


# --------------------------------Save Comparison DataFrame------------------------------ #


dfCompare.to_csv(f'{currFolder}{fileName}-Comparison.csv', index=False)