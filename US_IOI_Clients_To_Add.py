import pandas as pd
import numpy as np
import pandas
import pyodbc
import string

pd.options.display.max_columns = None


def get_conn_prod():
    return pyodbc.connect(
        'DRIVER={SQL Server};SERVER=eessql.gss.scotia-capital.com,5150;DATABASE=Portfolio;UID=dmamso;PWD=abc1234$6')


def get_conn_uat():
    return pyodbc.connect('DRIVER={SQL Server};SERVER=T65-EES-UAT\EES_UAT;DATABASE=Portfolio;UID=sa;PWD=123456Dma')


def client_name_encode(x):
    x = x.encode('utf-8').strip()
    return string.upper(x)


def string_remove_whitespace(s):
    return s.translate(None, string.whitespace)


def client_name_decode(x):
    x = x.decode('utf-8', 'ignore')
    return string.upper(x)


def string_remove_punctuation(s):
    return s.translate(None, string.punctuation)


def first_two_name(row, field):
    sub_names = row[field].split()

    if field == 'CLIENT_NAME' and row[field] == 'WILLBLAIR':
        sub_name = 'WILLIAM BLAIR'
    if field == 'CPTY_DESC' and row[field] == 'CITADEL (SURVEYOR)':
        sub_name = 'Citadel Investment'
    if field == 'CPTY_DESC' and row[field] == 'CAPGROUP (CAPITAL RESEARCH & MGMT)':
        sub_name = 'CAPITAL RESEARCH'

    elif len(sub_names) == 1:
        sub_name = sub_names[0]
    else:
        sub_name = " ".join([sub_names[0], sub_names[1]])

    sub_name = string_remove_punctuation(sub_name)
    sub_name = string_remove_whitespace(sub_name)

    return sub_name.upper()


def get_us_fidessa_clients():
    CUSTOMER_DETAIL_REPORT_df = pd.read_csv("C:\Temp\US_Fidessa_Clients.csv")
    CUSTOMER_DETAIL_REPORT_df = CUSTOMER_DETAIL_REPORT_df[['CPTY_DESC', 'VIEW_CODE', ]]
    CUSTOMER_DETAIL_REPORT_df = CUSTOMER_DETAIL_REPORT_df[
        ~CUSTOMER_DETAIL_REPORT_df['VIEW_CODE'].str.startswith('TEST')]
    CUSTOMER_DETAIL_REPORT_df = CUSTOMER_DETAIL_REPORT_df.sort_values(['CPTY_DESC', 'VIEW_CODE'])

    CUSTOMER_DETAIL_REPORT_df['SUB_NAME'] = CUSTOMER_DETAIL_REPORT_df.apply(lambda x: first_two_name(x, "CPTY_DESC"),
                                                                            axis=1)

    return CUSTOMER_DETAIL_REPORT_df


def get_clients():
    client_sql = "SELECT CLIENT_ID" \
                 ",NAME" \
                 " FROM CLIENT" \
                 " order by CLIENT_ID, NAME"

    client_df = pd.read_sql(client_sql, get_conn_prod())
    client_df = client_df[client_df['NAME'].str.len() > 0]
    client_df['NAME'] = client_df['NAME'].apply(client_name_encode)
    client_df['SUB_NAME'] = client_df.apply(lambda x: first_two_name(x, "NAME"), axis=1)

    return client_df


def get_fidessa():
    fidessa_sql = "SELECT FIDESSA_ID" \
                  ",FIDESSA_ACCOUNT" \
                  " FROM FIDESSA " \
                  " order by FIDESSA_ACCOUNT, FIDESSA_ID"

    fidessa_df = pd.read_sql(fidessa_sql, get_conn_prod())

    return fidessa_df


def get_missing_fidessa(us_fidessa_df, fidessa_df):
    # Matching on VIEW CODE to find out what's missing FIDESSA Account

    us_missing_fidessa_df = pd.merge(us_fidessa_df, fidessa_df, how='left', left_on=['VIEW_CODE'],
                                     right_on=['FIDESSA_ACCOUNT'])
    us_missing_fidessa_df = us_missing_fidessa_df[(us_missing_fidessa_df['FIDESSA_ACCOUNT'].isnull())]

    return us_missing_fidessa_df[['SUB_NAME', 'CPTY_DESC', 'VIEW_CODE']]


def get_missing_client(us_missing_fidessa_df, client_df):
    missing_us_clients_df = pd.merge(us_missing_fidessa_df, client_df, how='left', on=['SUB_NAME'])
    missing_us_clients_df = missing_us_clients_df[missing_us_clients_df['CLIENT_ID'].isnull()]
    missing_us_clients_df = missing_us_clients_df.rename(columns={'CPTY_DESC': 'CLIENT',
                                                                  'SUB_NAME': 'SHORT_NAME',
                                                                  'VIEW_CODE': 'FIDESSA_IOI'})
    missing_us_clients_df = missing_us_clients_df.drop(['CLIENT_ID'], axis=1)

    return missing_us_clients_df


def _calc_tier(dollar_limit):
    if dollar_limit >= 350000000.0:
        return 1
    elif dollar_limit >= 150000000.0:
        return 2
    else:
        return 9


def get_tiers():
    client_tiers_df = pd.read_csv("C:\Temp\US_Fidessa_Clients_Tiers.csv")

    client_tiers_df = client_tiers_df.rename(columns={'Cpty': 'VIEW_CODE',
                                                      'Counterparty or group description': 'CLIENT',
                                                      'Combined daily consideration limit': 'DOLLAR_LIMIT'
                                                      })

    client_tiers_df = client_tiers_df.groupby(['CLIENT'], as_index=False).agg({'DOLLAR_LIMIT': 'sum'})

    client_tiers_df['TIER'] = client_tiers_df['DOLLAR_LIMIT'].apply(_calc_tier)

    client_tiers_df = client_tiers_df.drop(['DOLLAR_LIMIT'], axis=1)

    return client_tiers_df


def get_missing_client(us_missing_fidessa_df, client_df):
    missing_us_clients_df = get_missing_client(us_missing_fidessa_df, client_df)
    clients_tier_df = get_tiers()

    missing_us_clients_df = pd.merge(missing_us_clients_df, clients_tier_df, how='left', on=['CLIENT'])

    # delete when TIER is missing - no TIER means there is Trading Limits with the VIEW CODE
    missing_us_clients_df = missing_us_clients_df[~missing_us_clients_df['TIER'].isnull()]

    missing_us_clients_df = missing_us_clients_df.sort_values(['CLIENT', 'FIDESSA_IOI'])

    return missing_us_clients_df


def _max_add_1(idx, base):
    return idx + 1 + base


def _max_client_id():
    client_sql_1 = "SELECT max(CLIENT_ID) max_client_id" \
                   " FROM CLIENT "

    client_sql_2 = "SELECT max(CLIENT_ID) max_client_id" \
                   " FROM CLIENT_FIDESSA"

    max_client_id_1 = pd.read_sql(client_sql_1, get_conn_prod())

    max_client_id_2 = pd.read_sql(client_sql_2, get_conn_prod())

    return max(max_client_id_1['max_client_id'][0], max_client_id_2['max_client_id'][0])


def create_client_adds(us_missing_client_df):
    cols = ['CLIENT_ID', 'NAME', 'FIDESSA_IOI', 'TRADE_CHAT', 'EMAIL', 'SHORT_NAME', 'STATUS', 'TM_NAME', 'TIER']
    us_missing_client_to_add = us_missing_client_df.groupby(['CLIENT'], as_index=False).agg({'SHORT_NAME': 'first',
                                                                                             'FIDESSA_IOI': 'first',
                                                                                             'TIER': 'first'})

    index_series = us_missing_client_to_add.index.to_series()

    max_client_id = _max_client_id()
    us_missing_client_to_add['CLIENT_ID'] = index_series.apply(
        lambda ( idx, max_client_id): _max_add_1(idx, max_client_id))
    us_missing_client_to_add['TRADE_CHAT'] = ''
    us_missing_client_to_add['EMAIL'] = ''
    us_missing_client_to_add['STATUS'] = ''
    us_missing_client_to_add['TM_NAME'] = ''

    us_missing_client_to_add = us_missing_client_to_add.rename(columns={'CLIENT': 'NAME'})
    us_missing_client_to_add = us_missing_client_to_add[cols]
    us_missing_client_to_add['NAME'] = us_missing_client_to_add['NAME'].apply(client_name_decode)
    us_missing_client_to_add['SHORT_NAME'] = us_missing_client_to_add['SHORT_NAME'].apply(client_name_decode)

    return us_missing_client_to_add


def _max_fidessa_id():
    fidessa_sql = "SELECT max(FIDESSA_ID) max_fidessa_id" \
                  " FROM FIDESSA "

    max_fidessa_id = pd.read_sql(fidessa_sql, get_conn_prod())
    max_fidessa_id = max_fidessa_id['max_fidessa_id'][0]

    return max_fidessa_id


def create_fidessa_adds(us_fidessa_df, fidessa_df):
    cols = ['FIDESSA_ID', 'FIDESSA_ACCOUNT', 'CTP', 'USTP']
    missing_fidessa_df = get_missing_fidessa(us_fidessa_df, fidessa_df)

    fidessa_to_add = missing_fidessa_df[['FIDESSA_IOI']]
    fidessa_to_add.reset_index(inplace=True)
    index_series = fidessa_to_add.index.to_series()
    max_fidessa_id = _max_fidessa_id()
    fidessa_to_add['FIDESSA_ID'] = index_series.apply(lambda (idx, max_fidessa_id ): _max_add_1(idx, max_fidessa_id))

    fidessa_to_add['CTP'] = 0
    fidessa_to_add['USTP'] = 1

    fidessa_to_add = fidessa_to_add.rename(columns={'FIDESSA_IOI': 'FIDESSA_ACCOUNT'})
    fidessa_to_add = fidessa_to_add[cols]

    return fidessa_to_add


def create_client_inserts(us_missing_client_df, isUAT=True):
    for row in us_missing_client_df.iterrows():
        if row[1]['NAME'] == 'AXA FOR IOI\'S':

            insert_client_sql = 'INSERT INTO Portfolio.dbo.[CLIENT_x] VALUES({:d},concat(\'AXA for IOI\',char(39),\'s\'),\'{:s}\',\'{:s}\',\'{:s}\',\'{:s}\',\'{:s}\',\'{:s}\',{:d})' \
                .format(int(row[1]['CLIENT_ID']),
                        row[1]['FIDESSA_IOI'],
                        row[1]['TRADE_CHAT'],
                        row[1]['EMAIL'],
                        row[1]['SHORT_NAME'],
                        row[1]['STATUS'],
                        row[1]['TM_NAME'],
                        int(row[1]['TIER']))

        elif row[1]['NAME'] == 'MOODY\'S CORPORATION':

            insert_client_sql = 'INSERT INTO Portfolio.dbo.[CLIENT_x] VALUES({:d},concat(\'MOODY\',char(39),\'s CORPORATION\'),\'{:s}\',\'{:s}\',\'{:s}\',\'{:s}\',\'{:s}\',\'{:s}\',{:d})' \
                .format(int(row[1]['CLIENT_ID']),
                        row[1]['FIDESSA_IOI'],
                        row[1]['TRADE_CHAT'],
                        row[1]['EMAIL'],
                        row[1]['SHORT_NAME'],
                        row[1]['STATUS'],
                        row[1]['TM_NAME'],
                        int(row[1]['TIER']))
        elif row[1]['NAME'] == 'SCI FOR ETF\'S':

            insert_client_sql = 'INSERT INTO Portfolio.dbo.[CLIENT_x] VALUES({:d},concat(\'SCI FOR ETF\',char(39),\'s\'),\'{:s}\',\'{:s}\',\'{:s}\',\'{:s}\',\'{:s}\',\'{:s}\',{:d})' \
                .format(int(row[1]['CLIENT_ID']),
                        row[1]['FIDESSA_IOI'],
                        row[1]['TRADE_CHAT'],
                        row[1]['EMAIL'],
                        row[1]['SHORT_NAME'],
                        row[1]['STATUS'],
                        row[1]['TM_NAME'],
                        int(row[1]['TIER']))

        else:
            insert_client_sql = 'INSERT INTO Portfolio.dbo.[CLIENT_x] VALUES({:d},\'{:s}\',\'{:s}\',\'{:s}\',\'{:s}\',\'{:s}\',\'{:s}\',\'{:s}\',{:d})' \
                .format(int(row[1]['CLIENT_ID']),
                        row[1]['NAME'],
                        row[1]['FIDESSA_IOI'],
                        row[1]['TRADE_CHAT'],
                        row[1]['EMAIL'],
                        row[1]['SHORT_NAME'],
                        row[1]['STATUS'],
                        row[1]['TM_NAME'],
                        int(row[1]['TIER']))

        print insert_client_sql

        if isUAT:
            con = get_conn_uat()
        else:
            cont = get_conn_prod()

        con.execute(insert_client_sql)
        con.commit()


def create_fidessa_inserts(missing_fidessa_df, isUAT=True):
    for row in missing_fidessa_df.iterrows():

        insert_fidessa_sql = 'INSERT INTO Portfolio.dbo.[FIDESSA] VALUES({:d},\'{:s}\',{:d},{:d})' \
            .format(int(row[1]['FIDESSA_ID']), row[1]['FIDESSA_ACCOUNT'], int(row[1]['CTP']), int(row[1]['USTP']))
        print insert_fidessa_sql

        if isUAT:
            con = get_conn_uat()
        else:
            con = get_conn_prod()

        con.execute(insert_fidessa_sql)
        con.commit()


def client_fidessa_to_add(missing_client_df, client_to_add_df, fidessa_to_add_df):

    # missing_client_df is 1 client name to many fidessa account
    # client_to_add_dif is 1 client name to 1 fidessa, and it also has the right clientID

    missing_client_df['CLIENT'] = missing_client_df['CLIENT'].apply(client_name_decode)
    client_to_fidessa = pd.merge(fidessa_to_add_df,client_to_add_df,how='left',
                                    left_on=['FIDESSA_ACCOUNT'],
                                    right_on=['FIDESSA_IOI'])

    client_to_fidessa = client_to_fidessa[['FIDESSA_ID','CLIENT']]

    client_to_fidessa = pd.merge(client_to_fidessa,
                                    client_to_add_df,
                                    how='left',
                                    left_on=['CLIENT'],
                                    right_on=['NAME'])

    return client_to_fidessa[['CLIENT_ID','FIDESSA_ID']]


def create_client_fidessa_inserts(client_to_fidessa, isUAT=True):

    for row in client_to_fidessa.iterrows():
        insert_client_fidessa_sql = 'INSERT INTO Portfolio.dbo.CLIENT_FIDESSA VALUES({:d},{:d})' \
            .format(int(row[1]['CLIENT_ID']),int(row[1]['FIDESSA_ID']))
        print insert_client_fidessa_sql

        if isUAT:
            con = get_conn_uat()
        else:
            con = get_conn_prod()

        con.execute(insert_client_fidessa_sql)
        con.commit()



