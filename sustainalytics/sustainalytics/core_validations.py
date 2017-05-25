"""
Manages  core functions the module
"""

#import modules to be used
import pandas as pd
import numpy as np
from datetime import datetime



#nalist = ['No data', 'N/A'] #Blank list..
#valid = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789' #alpha-numeric

def valid_characters():
    """
    Returns values the valid characters accepted by Identifiers
    :return: string character
    """
    valid = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789'  # alpha-numeric
    return valid


def get_default_column_strings():
    """
    Returns a list of all expected default strings in the GA
    :return: list of default strings
    """
    default_strings = ['No data','Research in progress','Framework not applicable','No Access']
    return default_strings

def is_isin(isin_value):
    """
    Validates that ISIN Column is in the right format.
    12 digits for ISIN – apply this rule on the file.
    :param isin_value: df['ISIN']
    :return: validation flag
    """
    if type(isin_value) != str:
        return True
    elif type(isin_value) == str and len(isin_value) == 12:
        if (all([(i in valid_characters()) for i in isin_value])):
            if (any([(i in valid_characters()) for i in isin_value])):
                return False
    if isin_value in get_default_column_strings():
        return False
    else:
        return True




def is_cusip(cusip_value):
    """
    Validates that CUSIP Column is in the right format.
    9 digits for CUSIP – apply this rule on the file
    :param df: df['CUSIP']
    :return: validation flag
    """
    if type(cusip_value) == int:
        if 5 < len(str(cusip_value)) < 10: #previously len(str(x)) == 9:
            return False
        else:
            return True
    elif type(cusip_value) == str and len(cusip_value) == 9:
                if(all([(i in valid_characters()) for i in cusip_value])):
                    if(any([(i in valid_characters()) for i in cusip_value])):
                        return False
    if cusip_value in get_default_column_strings():
        return False
    else:
        return True


def is_sedol(sedol_value):
    """
    Validates that Sedol Column is in the right format
    :param df: df['Sedol']
    :return: validation flag
    """
    if type(sedol_value) == int:
        if len(str(sedol_value)) == 7:
            return False
        else:
            return True
    elif type(sedol_value) == str and len(sedol_value) == 7:
                if(all([(i in valid_characters()) for i in sedol_value])):
                    if(any([(i in valid_characters()) for i in sedol_value])):
                        return False
    if sedol_value in get_default_column_strings():
        return False
    else:
        return True

def replaceScore(x):
    """
    Replace score to 0 if the type is string.
    """
    if type(x) == str:
        return 0
    else:
        return x

def bigdiff(x,delta):
    """
    Return True is absolute score difference is greater than delta
    """
    if np.abs(x) > delta:
        return True
    elif np.abs(x) < delta:
        return False
    else:
        return True

def get_today_date():
    """
    Returns the date as a string
    :return: string(date)
    """
    today = datetime.today()
    today = today.strftime('%d-%m-%y')
    return str(today)








