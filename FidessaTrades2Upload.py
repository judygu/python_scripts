import pandas as pd
import pyodbc
from datetime import datetime
import datetime as dt

from pandas.tseries.holiday import AbstractHolidayCalendar, Holiday, nearest_workday, \
    USMartinLutherKingJr, USPresidentsDay, GoodFriday, USMemorialDay, \
    USLaborDay, USThanksgivingDay


class USTradingCalendar(AbstractHolidayCalendar):
    rules = [
        Holiday('NewYearsDay', month=1, day=1, observance=nearest_workday),
        USMartinLutherKingJr,
        USPresidentsDay,
        GoodFriday,
        USMemorialDay,
        Holiday('USIndependenceDay', month=7, day=4, observance=nearest_workday),
        USLaborDay,
        USThanksgivingDay,
        Holiday('Christmas', month=12, day=25, observance=nearest_workday)
    ]


def deQuote(x):
    return x[1:len(x) - 1]


def deMillisecond(x):
    return x[:-7]


def get_trading_close_holidays(year):
    inst = USTradingCalendar()

    return inst.holidays(dt.datetime(year - 1, 12, 31), dt.datetime(year, 12, 31))


def dropTable():
    drop_trades_sql = "Drop Table SCPWAD_US.dbo.trades"
    prodConn().execute(drop_trades_sql)
    prodConn().commit()


def createTable():
    create_trades_sql = "CREATE TABLE SCPWAD_US.dbo.trades\
    (ParentOrderID  nvarchar(256) not null,\
    ChildOrderID   nvarchar(256) not null,\
    SecurityID     nvarchar(128) not null,\
    ISIN           nvarchar(128) not null,\
    Side           nvarchar(16) not null,\
    OrderedShares   int null,\
    LastQty        int null,\
    LastPx         float null,\
    ExecutionTime  datetime null,\
    Currency       nvarchar(16) null,\
    TRADER         nvarchar(128)null,\
    LastMarket     nvarchar(32) null,\
    LastCapacity   nvarchar(32) null,\
    Broker         nvarchar(128)null,\
    Strategy       nvarchar(128)null,\
    Liquidity      nvarchar(64)null,\
    BrokerPickupTime  datetime null,\
    ClientName     nvarchar(256)null,\
    Client         nvarchar(128)null,\
    ParentOrderLimitPrice float null,\
    OrderArrivalTime  datetime null,\
    BasketID          nvarchar(128) null, \
    Country           nvarchar(32) null,\
    TradeDate         date not null, \
    )"

    print create_trades_sql

    prodConn().execute(create_trades_sql)
    prodConn().commit()


def insertRow(trades):
    for row in trades.iterrows():
        insert_trade_sql = 'INSERT INTO SCPWAD_US.dbo.trades VALUES(\
                            \'{:s}\',\'{:s}\',\'{:s}\',\'{:s}\',\
                            \'{:s}\',{:d},{:d},{:f},\
                            \'{:s}\',\'{:s}\',\'{:s}\',\'{:s}\',\
                            \'{:s}\',\'{:s}\',\'{:s}\',\'{:s}\',\
                            \'{:s}\',\'{:s}\',\'{:s}\',{:f},\
                            \'{:s}\',\'{:s}\',\'{:s}\',\'{:s}\')'

        insert_trade_sql = insert_trade_sql.format(str(row[1]['ParentOrderID']),
                                                   str(row[1]['ChildOrderID']),
                                                   str(row[1]['SecurityID']),
                                                   str(row[1]['ISIN']),
                                                   str(row[1]['Side']),
                                                   int(row[1]['OrderedShares']),
                                                   int(row[1]['LastQty']),
                                                   float(row[1]['LastPx']),
                                                   str(row[1]['ExecutionTime']),
                                                   str(row[1]['Currency']),
                                                   str(row[1]['TRADER']),
                                                   str(row[1]['LastMarket']),
                                                   str(row[1]['LastCapacity']),
                                                   str(row[1]['Broker']),
                                                   str(row[1]['Strategy']),
                                                   str(row[1]['Liquidity']),
                                                   str(row[1]['BrokerPickupTime']),
                                                   str(row[1]['ClientName']),
                                                   str(row[1]['Client']),
                                                   float(row[1]['ParentOrderLimitPrice']),
                                                   str(row[1]['OrderArrivalTime']),
                                                   str(row[1]['BasketID']),
                                                   str(row[1]['Country']),
                                                   str(row[1]['TradeDate']), )

        print insert_trade_sql
        prodConn().execute(insert_trade_sql)
        prodConn().commit()


def insertBulk(trades):
    table_cols = ['ParentOrderID', 'ChildOrderID', 'SecurityID', 'ISIN', 'Side', 'OrderedShares', 'LastQty', 'LastPx',
                  'ExecutionTime',
                  'Currency', 'TRADER', 'LastMarket', 'LastCapacity', 'Broker', 'Strategy', 'Liquidity',
                  'BrokerPickupTime',
                  'ClientName', 'Client', 'ParentOrderLimitPrice', 'OrderArrivalTime', 'BasketID', 'Country',
                  'TradeDate']

    insert_query = "insert into SCPWAD_US.dbo.trades ({}) values ({})".format(", ".join(table_cols),
                                                                              ", ".join(["?" for col in table_cols]))

    tradeData = [[str(row[1]['ParentOrderID']),
                  str(row[1]['ChildOrderID']),
                  str(row[1]['SecurityID']),
                  str(row[1]['ISIN']),
                  str(row[1]['Side']),
                  int(row[1]['OrderedShares']),
                  int(row[1]['LastQty']),
                  float(row[1]['LastPx']),
                  str(row[1]['ExecutionTime']),
                  str(row[1]['Currency']),
                  str(row[1]['TRADER']),
                  str(row[1]['LastMarket']),
                  str(row[1]['LastCapacity']),
                  str(row[1]['Broker']),
                  str(row[1]['Strategy']),
                  str(row[1]['Liquidity']),
                  str(row[1]['BrokerPickupTime']),
                  str(row[1]['ClientName']),
                  str(row[1]['Client']),
                  float(row[1]['ParentOrderLimitPrice']),
                  str(row[1]['OrderArrivalTime']),
                  str(row[1]['BasketID']),
                  str(row[1]['Country']),
                  str(row[1]['TradeDate'])] for row in trades.iterrows()]

    crsr = prodConn().cursor()
    crsr.executemany(insert_query, tradeData)


if __name__ == '__main__':
    dateRange = pd.date_range('20180228', '20180425', freq='B')
    dateStrs = dateRange.strftime('%Y%m%d')

    holidays = get_trading_close_holidays(2018)
    holidayStrs = [x.strftime('%Y%m%d') for x in holidays]

    for dateStr in dateStrs:
        if dateStr in holidayStrs:
            continue

        trades = getTrades(dateStr)
        print "running {} of size {} at {:%Y-%m-%d %H:%M }".format(dateStr, len(trades), datetime.now())
        insertBulk(trades)
