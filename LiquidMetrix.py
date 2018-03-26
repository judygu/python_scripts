import datetime
import pandas as pd
import cx_Oracle
import time

def rawData(dateStr, table):
    db = "dummy"

    query = """SELECT * 
               FROM {0}
               where EMIS_RUN_DATE = to_date('{1}',  'YYYYMMDD')
            """
    query = query.format(table, dateStr)

    raw_df = pd.read_sql(query, con=db)

    #     cols = list(raw_df.columns)
    # cols.sort()
    # print cols

    return raw_df


def parentOrder(order_df):
    po_df = order_df[order_df['PARENT_ORDER_ID'].isnull()]

    po_df = po_df[['ORDER_ID',
                   'TRADING_QUANTITY',
                   'LIMIT_PRICE',
                   'COUNTERPARTY_CODE',
                   'COUNTERPARTY_ID',
                   'LIMIT_PRICE',
                   'VERSION',
                   'ENTERED_DATETIME',
                   'BASKET_ID',
                   'COUNTRY_CODE',
                   ]]
    po_df.sort_values(['ORDER_ID', 'VERSION'], inplace=True)
    po_max_df = po_df.groupby(['ORDER_ID'], as_index=False).agg({'VERSION': 'max', })
    po_df = pd.merge(po_max_df, po_df, on=['ORDER_ID', 'VERSION'], how='left')

    po_df = po_df.rename(columns={'ORDER_ID': 'ParentOrderID',
                                  'TRADING_QUANTITY': 'OrderedShares',
                                  'LIMIT_PRICE': 'ParentOrderLimitPrice',
                                  'COUNTERPARTY_CODE': 'ClientName',
                                  'COUNTERPARTY_ID': 'Client',
                                  'VERSION': 'ParentOrderVersion',
                                  'ENTERED_DATETIME': 'OrderArrivalTime',
                                  'BASKET_ID': 'BasketID',
                                  'COUNTRY_CODE': 'Country',
                                  })

    return po_df


def childOrder(order_df):
    co_df = order_df[['ROOT_ORDER_ID',
                      'ORDER_ID',
                      'VERSION',
                      'ENTERED_DATETIME']]
    co_df.sort_values(['ORDER_ID', 'VERSION'], inplace=True)
    co_df = co_df.rename(columns={'ROOT_ORDER_ID': 'ParentOrderID',
                                  'ORDER_ID': 'ChildOrderID',
                                  'VERSION': 'ORDER_VERSION',
                                  'ENTERED_DATETIME': 'BrokerPickupTime'})

    return co_df


def trade(trade_df):
    def _broker_cleanup(row):
        if row['BUSINESS_TRANSACTION'] == 'HE' or row['BUSINESS_TRANSACTION'] == 'OX':
            return ''
        else:
            return row['Broker']

    # Trades / Fills
    # 1. Filter Business Transactions
    trade_df = trade_df[~trade_df['BUSINESS_TRANSACTION'].isin(['PF', 'OB', 'TR', 'CT'])]

    # 2. Filter out Trades without Order ID
    trade_df = trade_df[~trade_df['ORDER_ID'].isnull()]

    trade_df = trade_df[['TRADE_ID',
                         'ORDER_ID',
                         'ORDER_VERSION',
                         'INSTRUMENT_CODE',
                         'ISIN_CODE',
                         'MARKET_ID',
                         'BUY_SELL',
                         'QUANTITY',
                         'GROSS_PRICE',
                         'TRADE_DATETIME',
                         'DEALT_CURRENCY_ID',
                         'TRADER',
                         'LAST_MKT',
                         'DEALING_CAPACITY',
                         'COUNTERPARTY_CODE',
                         'EXECUTOR_ID',
                         'BUSINESS_TRANSACTION',
                         'LIQUIDITY_INDICATOR',
                         ]]

    trade_df = trade_df.rename(columns={'QUANTITY': 'LastQty',
                                        'GROSS_PRICE': 'LastPx',
                                        'BUY_SELL': 'Side',
                                        'TRADE_DATETIME': 'ExecutionTime',
                                        'DEALT_CURRENCY_ID': 'Currency',
                                        'LAST_MKT': 'LastMarket',
                                        'DEALING_CAPACITY': 'LastCapacity',
                                        'COUNTERPARTY_CODE': 'Broker',
                                        'EXECUTOR_ID': 'Strategy',
                                        'INSTRUMENT_CODE': 'SecurityID',
                                        'ISIN_CODE': 'ISIN',
                                        'LIQUIDITY_INDICATOR': 'Liquidity'
                                        })

    # 3. Delete Broker for HE, OX
    trade_df['Broker'] = trade_df.apply(_broker_cleanup, axis=1)

    return trade_df


def tradeOrder(trade_df, co_df, po_df):
    trade_co_df = pd.merge(trade_df, co_df, left_on=['ORDER_ID', 'ORDER_VERSION'],
                           right_on=['ChildOrderID', 'ORDER_VERSION'])

    trade_po_df = pd.merge(trade_co_df, po_df, on=['ParentOrderID'])
    trade_po_df['TradeDate'] = trade_po_df['OrderArrivalTime'].map(lambda x: x.strftime('%Y%m%d'))

    # clean up columns and reorder them
    trade_po_df = trade_po_df.drop(['TRADE_ID', 'ORDER_ID', 'ORDER_VERSION', 'BUSINESS_TRANSACTION',
                                    'MARKET_ID', 'TRADER', 'ClientName',
                                    'ParentOrderVersion'], axis=1)

    return trade_po_df


def _is_matched(row):
    # Sanity Check - sum of trade quantity matches with PO quantity.
    if row['OrderedShares'] == row['LastQty']:
        return True
    else:
        return False


def sanity_check(trade_order_df):
    qty_check_df = trade_order_df[['ParentOrderID', 'SecurityID', 'LastQty', 'OrderedShares']]
    qty_check_df = qty_check_df.groupby(['ParentOrderID', 'SecurityID'], as_index=False).agg({'LastQty': 'sum',
                                                                                              'OrderedShares': 'last',
                                                                                              })
    qty_check_df['matched'] = qty_check_df.apply(_is_matched, axis=1)
    matches = len(qty_check_df[qty_check_df['matched']])
    mismatches = len(qty_check_df[~qty_check_df['matched']])

    return {'matched': matches, 'mismatched': mismatches}


def _add_quotes(field):
    return '\'{}\''.format(field)


def run():
    datestr = datetime.datetime.now().strftime("%Y%m%d")
    datestr = "20180323"
    order_df = rawData(datestr, 'FUS_ORDER')
    if len(order_df) == 0:
        print "data not loaded for {} at {}".format(datestr, datetime.datetime.now().time())
        return False

    trade_df = rawData(datestr, 'FUS_TRADE_SUMMARY')
    trade_order_df = tradeOrder(trade(trade_df), childOrder(order_df), parentOrder(order_df))
    trade_order_df['Liquidity'] = trade_order_df['Liquidity'].apply(_add_quotes)

    check = sanity_check(trade_order_df)
    print check['mismatched'], check['matched']
    assert (check['mismatched'] * 1.0 / check['matched'] * 1.0 < 0.2 )

    trade_order_df.to_csv("Z:\DailyFidasseaOrderTrades\{}.csv".format(datestr))
    print "extract created for {} at {}".format(datestr, datetime.datetime.now().time())
    return True

while( run() == False):
    time.sleep(600)
