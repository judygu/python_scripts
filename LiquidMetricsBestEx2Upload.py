import pandas as pd
import numpy as np
import pyodbc
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


def strExLast2(x):
    return x[:-2]


def deMillisecond(x):
    return x[:-7]


def dropTable():
    drop_bestEx_sql = "Drop Table SCPWAD_US.dbo.bestEx"
    dbConn = prodConn()
    dbConn.execute(drop_bestEx_sql)
    dbConn.commit()


def createTable():
    create_bestEx_sql = "CREATE TABLE SCPWAD_US.dbo.bestEx \
        (ParentOrderID  nvarchar(256) not null,\
        OrderRef       nvarchar(256) not null,\
        Arrival        datetime null,\
        Instrument     nvarchar(128) not null,\
        ISIN           nvarchar(128) null,\
        Algo           nvarchar(128) null,\
        Volume         float not null,\
        Trader         nvarchar(128) null,\
        Client         nvarchar(128)null,\
        APSFminusVWAPALF          float null,\
        ArrivalPriceShortfall     float null,\
        ArrivalPx                 float null,\
        AvgExecutionPx            float null,\
        ClosePriceShortfall       float null,\
        IntradeSpread             float null,\
        IntradeVolume_ALF         float null,\
        LastFill                  datetime null,\
        MarketPart_ALF_perc       float null,\
        MorningStar_MktCap        float null,\
        MorningStar_Sector        nvarchar(128) null,\
        OpenPriceShortfall        float null,\
        PercADV                   float null,\
        PercSpreadCapture         float null,\
        PrevClosePriceShortfall   float null,\
        T10                       float null,\
        T15                       float null,\
        T30                       float null,\
        T5                        float null,\
        T60                       float null,\
        TWAM1                     float null,\
        TWAM15                    float null,\
        TWAM5                    float null,\
        TradingCountry           nvarchar(32) null,\
        TradingDay               date not null,\
        VWAP_ALF                 float null,\
        Value                   float null,\
        Volatility              float null,\
        pwp10                  float null,\
        pwp20                 float null,)"

    dbConn = prodConn()
    dbConn.execute(create_bestEx_sql)
    dbConn.commit()


def insertRow(bestEx):
    dbConn = prodConn()
    for row in bestEx.iterrows():
        insert_bestEx_sql = 'INSERT INTO SCPWAD_US.dbo.bestEx VALUES(\
                                \'{:s}\',\'{:s}\',\'{:s}\',\'{:s}\',\
                                \'{:s}\',\'{:s}\', {:f}, \'{:s}\',\
                                \'{:s}\', {:f}, {:f}, {:f},\
                                {:f}, {:f}, {:f}, {:f},\
                                \'{:s}\', {:f}, {:f}, \'{:s}\',\
                                {:f}, {:f}, {:f}, {:f},\
                                {:f}, {:f}, {:f}, {:f},\
                                {:f}, {:f}, {:f}, {:f},\
                                \'{:s}\',\'{:s}\',{:f},{:f},\
                                {:f}, {:f}, {:f} )'
        insert_bestEx_sql = insert_bestEx_sql.format(str(row[1]['ParentOrderID']),
                                                     str(row[1]['OrderRef']),
                                                     str(row[1]['Arrival']),
                                                     str(row[1]['Instrument']),
                                                     str(row[1]['ISIN']),
                                                     str(row[1]['Algo']),
                                                     float(row[1]['Volume']),
                                                     str(row[1]['Trader']),
                                                     str(row[1]['Client']),
                                                     float(row[1]['APSFminusVWAPALF']),
                                                     float(row[1]['ArrivalPriceShortfall']),
                                                     float(row[1]['ArrivalPx']),
                                                     float(row[1]['AvgExecutionPx']),
                                                     float(row[1]['ClosePriceShortfall']),
                                                     float(row[1]['IntradeSpread']),
                                                     float(row[1]['IntradeVolume_ALF']),
                                                     str(row[1]['LastFill']),
                                                     float(row[1]['MarketPart_ALF_perc']),
                                                     float(row[1]['MorningStar MktCap']),
                                                     str(row[1]['MorningStar Sector']),
                                                     float(row[1]['OpenPriceShortfall']),
                                                     float(row[1]['PercADV']),
                                                     float(row[1]['PercSpreadCapture']),
                                                     float(row[1]['PrevClosePriceShortfall']),

                                                     float(row[1]['T10']),
                                                     float(row[1]['T15']),
                                                     float(row[1]['T30']),
                                                     float(row[1]['T5']),

                                                     float(row[1]['T60']),
                                                     float(row[1]['TWAM1']),
                                                     float(row[1]['TWAM15']),
                                                     float(row[1]['TWAM5']),

                                                     str(row[1]['TradingCountry']),
                                                     str(row[1]['TradingDay']),
                                                     float(row[1]['VWAP_ALF']),
                                                     float(row[1]['Value']),

                                                     float(row[1]['Volatility']),
                                                     float(row[1]['pwp10']),
                                                     float(row[1]['pwp20']),

                                                     )
        # print insert_bestEx_sql
        dbConn.execute(insert_bestEx_sql)
        dbConn.commit()


def get_trading_close_holidays(year):
    inst = USTradingCalendar()

    return inst.holidays(dt.datetime(year - 1, 12, 31), dt.datetime(year, 12, 31))


if __name__ == '__main__':
    dateRange = pd.date_range('20180303', '20180420', freq='B')
    dateStrs = dateRange.strftime('%Y%m%d')

    holidays = get_trading_close_holidays(2018)
    holidayStrs = [x.strftime('%Y%m%d') for x in holidays]

    for dateStr in dateStrs:
        if dateStr in holidayStrs:
            continue

        bestEx = getBestEx(dateStr)
        print("running {} of size {}".format(dateStr, len(bestEx)))

        bestEx['ParentOrderID'] = bestEx['OrderRef'].map(strExLast2)
        bestEx['Arrival'] = bestEx['Arrival'].map(deMillisecond)
        bestEx['LastFill'] = bestEx['LastFill'].map(deMillisecond)
        bestEx = bestEx.rename(columns={'PortfolioManager': 'Client', 'Index': 'Instrument'})
        bestEx.fillna(0.00, inplace=True)

        insertRow(bestEx)
