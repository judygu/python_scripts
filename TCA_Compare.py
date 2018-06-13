import pandas as pd
import numpy as np
import pyodbc
from datetime import datetime
from IPython.display import display, HTML


def getTCART(dateStr, prodConn):
    tcaRT_sql = "SELECT * FROM SCPWAD_US.dbo.TCA where TradeDate = '{}'".format(dateStr)
    return pd.read_sql(tcaRT_sql, prodConn)


def getTCALiquidMetrix(dateStr, prodConn):
    tcaLiquidMetrix_sql = "SELECT * FROM SCPWAD_US.dbo.bestEx where TradingDay = '{}'".format(dateStr)
    return pd.read_sql(tcaLiquidMetrix_sql, prodConn)


def scotiaVWAPTCA(row):
    if row['Side'] == "1":
        return (1 - row['avgPrice'] / row['vwap']) * 10000
    else:
        return (row['avgPrice'] / row['vwap'] - 1) * 10000


if __name__ == '__main__':

    dateStr = [datetime.now().strftime("%Y-%m-%d")]
    dateStr = '2018-06-11'

    tcaRT = getTCART(dateStr, prodConn)
    tcaRT = tcaRT[['TradeDate', 'OrderID', 'Side', 'Symbol', 'vwap', 'avgPrice', 'orderStart', 'TCATime']]

    tcaLiquidMetrix = getTCALiquidMetrix(dateStr, prodConn)
    tcaLiquidMetrix = tcaLiquidMetrix[['ParentOrderID', 'VWAP_ALF']]
    tcaLiquidMetrix.rename(columns={'VWAP_ALF': 'liquidMetrixVWAPTCA', 'ParentOrderID': 'OrderID'}, inplace=True)
    tcaJoin = pd.merge(tcaRT, tcaLiquidMetrix, how='inner', on=['OrderID'])

    tcaJoin['scotiaVWAPTCA'] = tcaJoin.apply(lambda row: scotiaVWAPTCA(row), axis=1)
    tcaJoin['diffBPS'] = np.abs(tcaJoin['scotiaVWAPTCA'] - tcaJoin['liquidMetrixVWAPTCA'])
    tcaJoin.sort_values(['diffBPS'], inplace=True, ascending=False)

    tcaJoin.to_csv("Z:\TCA_Compare.csv")
