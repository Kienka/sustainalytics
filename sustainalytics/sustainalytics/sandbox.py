
"""
import pandas as pd
import sustainalytics_qa_tool.Onefile_Column_Checks as occ
import sustainalytics_qa_tool.Twofiles_Column_Checks as tcc
from sustainalytics_qa_tool.reporting import Report,Sheet

df = pd.read_excel('data_test/QA Test_10052017.xlsx', sheetname='Results')

#print(df.head())

#count,dfNew = occ.isin_check(df['ISIN'])
df1=tcc.summary_categorical_difference(df['Company type'],df['Company type'])
sheet = Sheet('Overall Difference',df1,keep_index=True)
report = Report('MyText3')
report.add_to_report(sheet)
report.save_report()


print(df1)
"""