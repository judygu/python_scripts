import pandas as pd
import numpy as np
import pyodbc
from datetime import datetime

# pd.options.display.max_columns = None

def createTable(prod_con):

    create_TCA_sql = "CREATE TABLE SCPWAD_US.dbo.TCA \
                                    ( Symbol nvarchar(64) not null, \
                                      OrderID nvarchar(64) not null, \
                                      Side nvarchar(16) not null, \
                                      queryTime datetime not null, \
                                      orderStart datetime not null, \
                                      TCATime datetime not null, \
                                      tickStart datetime not null, \
                                      tickEnd datetime not null, \
                                      volumeTraded int not null, \
                                      tickCount int not null, \
                                      vwap float not null, \
                                      twap float not null, \
                                      avgPrice float not null, )"

    print (create_TCA_sql)

    prod_con.execute(create_TCA_sql)
    prod_con.commit()


def insertTCA(tca_df, prod_con):

    for row in tca_df.iterrows():
        insert_TCA_sql = 'INSERT INTO SCPWAD_US.dbo.TCA VALUES(\'{:s}\',\'{:s}\',\'{:s}\',\'{:s}\',\'{:s}\',\'{:s}\',\'{:s}\',\'{:s}\',{:d},{:d},{:f},{:f},{:f})' \
            .format(str(row[1]['Symbol']),
                    str(row[1]['OrderID']),
                    str(row[1]['Side']),
                    str(row[1]['queryTime']),
                    str(row[1]['orderStart']),
                    str(row[1]['TCATime']),
                    str(row[1]['tickStart']),
                    str(row[1]['tickEnd']),
                    row[1]['volumeTraded'],
                    row[1]['tick_count'],
                    row[1]['vwap'],
                    row[1]['twap'],
                    row[1]['avgPrice'],
                    )
    print (insert_TCA_sql)
    prod_con.execute(insert_TCA_sql)
    prod_con.commit()


if __name__ == '__main__':

    tick_df = pd.read_csv("Z:\sb_tick_v2.csv")
    tca_df = pd.read_csv("Z:\sb_TCA_v2.csv")

    tca_df['queryTime'] =  tca_df['queryTime'].apply(lambda x : x[:-5])
    tca_df['orderStart'] =  tca_df['orderStart'].apply(lambda x : x[:-5])
    tca_df['TCATime'] =  tca_df['TCATime'].apply(lambda x : x[:-5])
    tca_df['tickStart'] =  tca_df['tickStart'].apply(lambda x : x[:-5])
    tca_df['tickEnd'] =  tca_df['tickEnd'].apply(lambda x : x[:-5])

    # tca_df['queryTime'] = tca_df['queryTime'].apply(lambda x : datetime.strptime(x, "%Y-%m-%d %H:%M:%S.%f"))
    # tca_df['orderStart'] = tca_df['orderStart'].apply(lambda x : datetime.strptime(x, "%Y-%m-%d %H:%M:%S.%f"))
    # tca_df['TCATime'] = tca_df['TCATime'].apply(lambda x : datetime.strptime(x, "%Y-%m-%d %H:%M:%S.%f"))
    # tca_df['tickStart'] = tca_df['tickStart'].apply(lambda x : datetime.strptime(x, "%Y-%m-%d %H:%M:%S.%f"))
    # tca_df['tickEnd'] = tca_df['tickEnd'].apply(lambda x : datetime.strptime(x, "%Y-%m-%d %H:%M:%S.%f"))

    insertTCA(tca_df,prod_con)