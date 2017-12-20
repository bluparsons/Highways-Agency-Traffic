import pandas as pd
import urllib2
from zipfile import ZipFile
from StringIO import StringIO
import datetime
import sqlite3
import csv

#with zipfile.ZipFile("http://data.dft.gov.uk.s3.amazonaws.com/SRNData/SRNDataJan2009.zip") as z:
#with zipfile.ZipFile("/Users/bluparsons/Documents/Python/ve/tfa/TrafficFlowAnalysis/SRNDataJan2009.zip") as z:
#    with z.open("SRNDataJan2009.txt") as f:
#        train = pd.read_csv(f, header=1, delimiter="\t")
#        print(train.head())    # print the first 5 rows

def read_csv_from_url(url_address, file_name):
    r = urllib2.urlopen(url_address).read()
    file = ZipFile(StringIO(r))
    salaries_csv = file.open(file_name)
    df = pd.read_csv(salaries_csv)
    #Filter on specific motorway and time of day
    df_filtered = df[((df['LinkRef'] == "LM418") | (df['LinkRef'] == "LM420")) &
                     ((df['TimePeriod'] >= 24) & (df['TimePeriod'] <= 36))]

    return df_filtered
#    return(df_filtered.head())
    #    df['Date'] = df['Date'].apply(lambda v: datetime.datetime.strptime(v, '%Y-%m-%d %H %M %S'))
#return df['Date'].min(), df['Date'].max()
#    groupby_df_filtered = df_filtered['Flow'].groupby(df_filtered['TimePeriod'])
#    print groupby_df_filtered.mean() this works

def load_data_into_table(con, data_set):
    data_set.to_sql("t", con, if_exists='append', index=False)

#def del_data_from_table


con = sqlite3.connect(":memory:")
cur = con.cursor()
cur.execute("CREATE TABLE t (LinkRef, LinkDescription, Date, TimePeriod, AverageJT, AverageSpeed, DataQuality, LinkLength, Flow);")
con.commit()

#This process goes to the Highway website, downloads the traffic data into a database then analyses the data.
month_source = ["Jan", "Feb"]
year_source = [2009, 2010, 2011, 2012]
for i in year_source:
    for j in month_source:
        url_address = "http://data.dft.gov.uk.s3.amazonaws.com/SRNData/SRNData" + j + str(i) + ".zip"
        
        #Highways datatypes are not consistent, pre-2010 .txt is used, now it's .csv.
        if i == 2009:
            file_name = "SRNData" + j + str(i) + ".txt"
        else if i == 2012
            file_name = "SRNData" + j + str(i) + ".txt"
        else: file_name = "SRNData" + j + str(i) + ".csv"
        
        print url_address
        print file_name
        data_set = read_csv_from_url(url_address, file_name)
        load_data_into_table(con, data_set)

#del_data_from_table()

print pd.read_sql_query("SELECT \
                        CAST (STRFTIME('%Y', DATE) AS STRING) AS YEAR,\
                        CAST (STRFTIME('%w', DATE) AS INTEGER) AS DAY,\
                        CASE CAST (STRFTIME('%w', DATE) AS INTEGER) \
                        WHEN 0 THEN 'SUN'\
                        WHEN 1 THEN 'MON'\
                        WHEN 2 THEN 'TUE'\
                        WHEN 3 THEN 'WED'\
                        WHEN 4 THEN 'THU'\
                        WHEN 5 THEN 'FRI'\
                        WHEN 6 THEN 'SAT'\
                        ELSE 'OTH' END AS DAYOFWEEK, \
                        LinkRef, \
                        AVG(Flow) \
                        FROM t \
                        WHERE LinkRef = 'LM420' AND DAY NOT IN (0, 6)\
                        GROUP BY 1, 2, 3, 4\
                        ORDER BY 1, 2", con)
con.close()
