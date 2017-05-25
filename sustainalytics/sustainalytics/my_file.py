

#Import Modules
import numpy as np
import pandas as pd
import re
import os
from datetime import datetime,date

#General functions
def check_blanks(df):
    """
    return rows that contain blanks in the provided dataframe.
    """
    if df[df.isnull().any(axis=1)].empty: #Check if there exist a blank in the df if none exist it t
        return False
    else:
        df=df[df.isnull().any(axis=1)]
        col=[1,0]+list(set(list(np.where(pd.isnull(df))[1]))) #Get rows and columns containing blanks
        return df.iloc[:,col]


#General functions
def check_no_data(df):
    """
    return rows that contain 'No data' in the provided dataframe.
    """
    row_mask = df.isin(['No data']).any(1) #Find columns where with 'No data' value.
    return df[row_mask]


nalist = ['No data']
valid = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789'
def cusipCheck(x):
    """
    ensure the length of values in the CUSIP column is 9 and string is 7.
    """
    if type(x) == int:
        if len(str(x)) > 5: #previously len(str(x)) == 9:
            return False
        else:
            return True
    elif type(x) == str and len(x) == 9:
                if(all([(i in valid) for i in x])):
                    if(any([(i in valid) for i in x])):
                        return False
    if x in nalist:
        return False
    else:
        return True


def coverage_vs_research_summary(df):
    """Makes a mapping with company management file and reporting on summary of what
    company is under research and which is under research.
    """
    # df_cmf = pd.read_excel("data/Companies Management Template 18 April.xlsx",sheetname="Company",skiprows=[0,1,2,4]) #Company Management file
    # df_cmf.drop('Action', axis=1, inplace=True)
    # df_cmf.set_index('Company Id', inplace=True) #Set index
    # df['Entity Type'] = df['Company ID'].map(df_cmf['Entity Type']) #Perform the mapping
    return df['Research Entity'].value_counts()  # returns the info


def core_vs_comprehensive_summary(df):
    """
    Makes a mapping with Report generated from GA containing field with 'Research Type' on summary of what
    company is core or comprehensive.
    """
    #df_cr_cmp = pd.read_excel("data/core_orcomprehensive_20042017.xlsx",sheetname="Results") #
    #df_cr_cmp.set_index('Company ID', inplace=True)
    #df['Research Type'] = df['Company ID'].map(df_cr_cmp['Research Type'])
    df_research_type_summary= df['Research Type'].value_counts()
    #df_research_type_summary['Research Type Summary'] = df_research_type_summary.index
    return df_research_type_summary

def convert_ticker_type(df):
    """
    Convert the column ticker to Text.
    """
    df['Ticker'] =  df['Ticker'].astype(str)
    return df

def summary_total_esg_score(df):
    """
    Returns a df of summary of the Total ESG Score column and this only applies to Raw Relative Analysis Report data file.
    """
    no_data_count = len(df[df['Total ESG Score']=='No data'])
    numeric_data = len(df['Total ESG Score']) - no_data_count
    #build a DataFrame
    summary_df = pd.DataFrame({'Counts':[no_data_count,numeric_data]}, index=['No data Counts in Total ESG','Numeric data Counts in Total ESG'])
    return summary_df

def parse_approximate_year(date):
    """
    check format and check if any year < 2011.
    """
    try:
        if datetime.strptime(str(date), "%Y-%m-%d %H:%M:%S"):
            d=datetime.strptime(str(date), "%Y-%m-%d %H:%M:%S")
            return int(d.year)
    except ValueError:
        return 'No data'


def differences(dfOld, dfNew, cols):
    """
    Return differences between 2 DataFrames
    """
    indexField = 'Company ID'
    dfOld = dfOld[cols]  # Taking specified cols
    dfNew = dfNew[cols]  # Taking specified cols
    # set index
    dfOld = dfOld.set_index(indexField)
    dfNew = dfNew.set_index(indexField)
    ##Getting indexes that intersects between both rows
    dfNew = dfNew[dfNew.index.isin(dfOld.index)]
    dfOld = dfOld[dfOld.index.isin(dfNew.index)]
    # Sort
    dfNew.sort_index(inplace=True)
    dfOld.sort_index(inplace=True)
    df = dfOld != dfNew
    linii_mod_stacked = df.stack()  # Converts row to column
    changed = linii_mod_stacked[linii_mod_stacked]
    changed.index.names = ['Company ID', 'in column']  # Prepares new index
    difference_locations = np.where(dfOld != dfNew)
    changed_from = dfOld.values[difference_locations]
    changed_to = dfNew.values[difference_locations]
    data = pd.DataFrame({'in old file': changed_from, 'in new file': changed_to}, index=changed.index)
    data = data.reset_index()

    return data


def get_required_columns(df):
    """
    Returns the desired list of columns Containing "Level of Involvement" and "Category of Involvement"
    """

    cols_containing_level = [col for col in df.columns if
                             'Level of Involvement' in str(col)]  # search for columns with 'Level of Involvement'
    cols_containing_category = [col for col in df.columns if 'Category of Involvement' in str(
        col)]  # search for columns with 'Category of Involvement'
    all_cols = cols_containing_level + cols_containing_category  # concat the both columns

    return ['Company ID'] + sorted(all_cols)

def summary_report_lookup(dfOld,dfNew):
    """
    Creates a report for Core Vs Comprehesive OR Coverage VS Research
    """
    df=pd.concat([dfNew,dfOld],axis=1)
    df.columns = ['Current Month','Previous Month']
    return df

def summary_report_lookup_qtrs(dfOld,dfNew):
    """
    Creates a report for Core Vs Comprehesive OR Coverage VS Research
    """
    df=pd.concat([dfNew,dfOld],axis=1)
    df.columns = ['Current Quarter','Previous Quarter']
    return df

def replaceScore(x):
    """
    Replace score to 0 if the type is string.
    """
    if type(x) == str:
        return 0
    else:
        return x

delta = 5
def bigdiff(x):
    """
    Return True is absolute score difference is greater than delta
    """
    if np.abs(x) > delta:
        return True
    elif np.abs(x) < delta:
        return False
    else:
        return True

def get_company_name(ID,df):
    """
    Get company name given ID.
    """
    result = df[df['Company ID']==ID]
    return result['Company Name']

def minus_df(dfsuper,dfsub):
    """
    dfsuper - dfsub : returns rows that exist in dfsuper and not in dfsub
    """
    dfsuper=dfsuper.set_index('Company ID')
    dfsub=dfsub.set_index('Company ID')
    df = dfsuper[~dfsuper.index.isin(dfsub.index)]
    return df
