
""" 2files Checks
This File manages the differences between files. For example what was added to the new file
and what was deleted in the new file
"""

import pandas as pd
import numpy as np

def file_difference(dfsuper,dfsub,indexField):
    """
    dfsuper - dfsub : returns rows that exist in dfsuper and not in dfsub
    Returns rows that exist in dfsuper and not dfsub. This can be used for knowing the additions and deletions.
    :param df1: dataframe to make the subtract
    :param df2: dataframe to be subtracted.
    :param indexField: The index of the dataframe example company ID
    :return: len(dfReport),dfReport
    """
    dfsuper=dfsuper.set_index(indexField)
    dfsub=dfsub.set_index(indexField)
    dfReport = dfsuper[~dfsuper.index.isin(dfsub.index)]
    return len(dfReport), dfReport


