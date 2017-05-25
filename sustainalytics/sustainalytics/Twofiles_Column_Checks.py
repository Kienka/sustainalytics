
"""
This file manages QA operations only on columns between 2 files.
"""
import pandas as pd
import numpy as np
from sustainalytics.core_validations import replaceScore,bigdiff
from sustainalytics.Onefile_Column_Checks import general_column_report,categorical_column_report



def get_company_name(ID,df):
    """
    Get company name given ID.
    """
    result = df[df['Company ID']==ID]
    return result['Company Name']


def column_difference(dfOld,dfNew,cols,indexField):
    """
    Returns the length of Column differences and the column differences
    :param dfOld: Previous deliverable
    :param dfNew: Current deliverable
    :param cols: Columns of Interest
    :param indexField : Index columns
    :return: len(dfReport), dfReport , uniqueChanges
    """
    ##Getting indexes that intersects between both rows
    #concating cols with indexField
    newcols = [indexField]+cols
    dfOld1 = dfOld[newcols]  # Taking specified cols
    dfNew1 = dfNew[newcols]  # Taking specified cols
    # set index
    dfNew1 = dfNew1[dfNew1.index.isin(dfOld1.index)]
    dfOld1 = dfOld1[dfOld1.index.isin(dfNew1.index)]
    #Sort index
    dfNew1.sort_index(inplace=True)
    dfOld1.sort_index(inplace=True)
    try: # Handling this ValueError: Can only compare identically-labeled DataFrame objects

        df = dfOld1 != dfNew1 #changes check

        linii_mod_stacked = df.stack()  # Converts row to column
        changed = linii_mod_stacked[linii_mod_stacked]
        changed.index.names = [indexField, 'in column']  # Prepares new index
        difference_locations = np.where(dfOld1 != dfNew1)
        changed_from = dfOld1.values[difference_locations]
        changed_to = dfNew1.values[difference_locations]
        data = pd.DataFrame({'in old file': changed_from, 'in new file': changed_to}, index=changed.index)
        data = data.reset_index()
        return len(data),data, len(data[indexField].unique()),len(dfNew1)

    except ValueError:
        return 0,"Ensure files have same length and does'nt contain duplicates",0,0





def score_differences(dfOld,dfNew,col,indexField,delta=5):
    """
    Returns the length of Column differences and the column differences
    :param dfOld: old score
    :param dfNew: new score
    :param col: score column
    :param indexField: index field
    :param delta: delta difference and default = 5
    :return: len(scores_data),data, percentage_change
    """
    _,scores_data,_ ,report_size= column_difference(dfOld,dfNew,col,indexField) #Gets the changes on that column

    scores_data['delta'] = np.nan
    #replace scores
    scores_data['in new file'] = scores_data['in new file'].map(replaceScore)
    scores_data['in old file'] = scores_data['in old file'].map(replaceScore)

    #Obtain delta values
    scores_data['delta'] = scores_data['in new file'] - scores_data['in old file']
    scores_data['bigdiff'] = np.vectorize(bigdiff)(scores_data['delta'],delta) #Applying the function bigdiff

    #Get the scores with difference more than the delta threshold
    scores_data =  scores_data[scores_data['bigdiff'] == True]

    #Drop the bigdiff Column
    scores_data = scores_data.drop('bigdiff',axis=1)

    return len(scores_data), scores_data, (len(scores_data)/report_size)*100 #return: len(scores_data),data, percentage_change


def summary_general_difference(dfOld,dfNew):
    """
    Concats the report for column summary comparison
    :param dfOld: Old file
    :param dfNew: New file
    :return: dfReport
    """
    dfOld_report = general_column_report(dfOld) #Get Columns report old file
    dfNew_report = general_column_report(dfNew) #Get Columns report for new file

    #Report
    dfReport = pd.concat([dfNew_report,dfOld_report], axis=1)
    dfReport.columns=['Current File', 'Previous File']

    return dfReport


def summary_categorical_difference(dfOld,dfNew):
    """
    Concats the report for column summary comparison
    :param dfOld:
    :param dfNew:
    :return:
    """
    dfOld_report = categorical_column_report(dfOld) #Get Columns report old file
    dfNew_report = categorical_column_report(dfNew) #Get Columns report for new file

    #Report
    dfReport = pd.concat([dfNew_report,dfOld_report], axis=1)
    dfReport.columns=['Current File', 'Previous File']

    return dfReport




















