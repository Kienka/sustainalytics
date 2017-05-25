
"""
This file manages QA operations only on a given column and in a single file.
"""

#import modules to be used
import numpy as np
import pandas as pd

from sustainalytics.core_validations import is_cusip,is_isin,is_sedol,get_default_column_strings


def isin_check(df):
    """
    Returns the count of wrong ISINs and the Dataframe
    :param df: df['ISIN']
    :return: len(df),dfContents
    """
    report_length = len(df[df.map(is_isin)])
    report_df = df[df.map(is_isin)]
    return report_length,report_df


def cusip_check(df):
    """
    Returns the count of wrong CUSIPs and the Dataframe
    :param df:df['CUSIP']
    :return: len(df),dfContents
    """
    report_length = len(df[df.map(is_cusip)])
    report_df = df[df.map(is_cusip)]
    return report_length,report_df

def sedol_check(df):
    """
    Returns the count of wrong Sedols and the Dataframe
    :param df: df['Sedol']
    :return: len(df),dfContents
    """
    report_length = len(df[df.map(is_sedol)])
    report_df = df[df.map(is_sedol)]
    return report_length,report_df




def ticker_check(df):
    """
    Validates that Ticker Column is in the right format
    :param df: df['Ticker']
    :return: len(df),dfContents
    """

def check_no_data(df):
    """
    Returns the count of rows containing rows No data, Research in progress...........
    :param df: df[xxxx]
    :return: validation flag
    """
    conditions = get_default_column_strings()
    row_mask = df.isin(conditions).any(1) #Find columns where the condition satisfies
    report_df = df[row_mask]
    report_length = len(report_df)
    return report_length,report_df

def unique_check(df):
    """
    Returns the count of duplicate rows and the Dataframe
    :param df: df['Column']
    :return:len(duplicate),dfDuplicates
    """
    report_df=df[df.duplicated(keep=False)] #Checks for duplicates
    report_length = len(report_df)
    return report_length,report_df


def blank_checks(df):
    """
    Returns the count of blanks and the Dataframe
    :param df:
    :return: len(dfContent),dfContent
    """
    if df[df.isnull().any(axis=1)].empty:  # Check if there exist a blank in the df if none exist it t
        return False
    else:
        df = df[df.isnull().any(axis=1)]
        col = [1, 0] + list(set(list(np.where(pd.isnull(df))[1])))  # Get rows and columns containing blanks
        return len(df.iloc[:, col]),df.iloc[:, col]

def general_column_report(df):
    """
    Returns a summary report of the column
    :param df: df.value_counts()
    :param percentage: %of the counts
    :return:
    """
    df_no_data = df[df.isin(get_default_column_strings())] #No data, Research in progress etc.
    df_no_data = pd.DataFrame(df_no_data.value_counts()) #Get the non-value counts
    df_values = len(df[~df.isin(get_default_column_strings())]) #scores if any exist
    #Create a dataframe for values
    if df_values > 0:
        df_values = pd.DataFrame({'Counts':[df_values]},index=['Available values'])
        df_values.columns = list(df_no_data.columns) #Assign the default column name to ease appending

        #Append to dataframes
        return df_no_data.append(df_values)
    else:
        return  df_no_data

def categorical_column_report(df):
    """
    Should be used only columns with finite categories
    :param df:
    :return: df.value_counts()
    """
    dfReport = pd.DataFrame(df.value_counts())

    return dfReport


def perceentage_summation_check(df):
    """
    Validates a column summation is 100%
    :param df: df[XXXX] % Column
    :return: Boolean,df.sum()
    """
    if round(df.sum(),2)==1.0 or round(df.sum(),2)==100.0:
                return True,round(df.sum(),2)
    else:
        return False,round(df.sum(),2)





