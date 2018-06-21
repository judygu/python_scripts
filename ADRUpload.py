import pandas as pd
import pyodbc as pyodbc
from datetime import datetime
import numpy as np


def getADR():
    dateStr = datetime.now().strftime("%Y%m%d")
    file = "Z:\MSD_BASE_ADR_{}.csv".format(dateStr)
    ADR_df = pd.read_csv(file)
    ADR_df['adr_sh_per_adr'] = ADR_df['adr_sh_per_adr'].map(str)
    ADR_df['name'] = ADR_df['name'].apply(lambda x: str.replace(x, '\'', ''))
    ADR_df = ADR_df[~(ADR_df['adr_sh_per_adr'].str.contains('M')) & ~(ADR_df['adr_sh_per_adr'].str.contains('S'))]
    ADR_df['adr_sh_per_adr'] = ADR_df['adr_sh_per_adr'].map(np.float)
    ADR_df['date'] = dateStr

    return ADR_df


def prodConn():
    return pyodbc.connect(
        'DRIVER={SQL Server};SERVER=eessql.gss.scotia-capital.com,5150;DATABASE=SCPWAD_US;UID=dmamso;PWD=abc1234$6',
        autocommit=True)


def createTable():
    prodConn_ = prodConn();

    create_ADR_sql = "CREATE TABLE SCPWAD_US.dbo.ADR\
    (Date  date not null,\
    Ric   nvarchar(128) not null,\
    Cusip nvarchar(128) not null,\
    ISIN  nvarchar(128) not null,\
    Sedol nvarchar(128) not null,\
    Symbol nvarchar(128) not null,\
    Ticker nvarchar(128) not null,\
    Name   nvarchar(256) not null,\
    Country  nvarchar(128) not null,\
    SecurityType  nvarchar(128) not null,\
    TickerExchCode nvarchar(128) not null,\
    ADRSharePerADR float not null, \
    ADRUnderlyer   nvarchar(128) not null,\
    ADRUnderlyerRIC   nvarchar(128) not null,\
    PRIMARY KEY (Date,Ric),\
    )"

    print create_ADR_sql

    prodConn_.execute(create_ADR_sql)
    prodConn_.commit()


def insertRow(ADR_df):
    for row in ADR_df.iterrows():
        insert_ADR_sql = 'INSERT INTO SCPWAD_US.dbo.ADR VALUES(\
                            \'{:s}\',\'{:s}\',\'{:s}\',\'{:s}\',\
                            \'{:s}\',\'{:s}\',\'{:s}\',\'{:s}\',\
                            \'{:s}\',\'{:s}\',\'{:s}\',\'{:f}\',\
                            \'{:s}\',\'{:s}\')'

        insert_ADR_sql = insert_ADR_sql.format(str(row[1]['date']),
                                               str(row[1]['ric']),
                                               str(row[1]['id_cusip']),
                                               str(row[1]['id_isin']),
                                               str(row[1]['id_sedol1']),
                                               str(row[1]['id_exch_symbol']),
                                               str(row[1]['ticker']),
                                               str(row[1]['name']),
                                               str(row[1]['cntry_issue_iso']),
                                               str(row[1]['security_typ']),
                                               str(row[1]['ticker_and_exch_code']),
                                               float(row[1]['adr_sh_per_adr']),
                                               str(row[1]['adr_undl_ticker']),
                                               str(row[1]['re_depository_asset_undl']),
                                               )

        print insert_ADR_sql
        prodConn().execute(insert_ADR_sql)
        prodConn().commit()


if __name__ == '__main__':
    insertRow(getADR())
