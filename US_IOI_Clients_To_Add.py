import pandas as pd
import numpy as np
import pyodbc
import string
import re

pd.options.display.max_columns = None


def get_conn_prod():
    return 'dummy'

def get_conn_uat():
    return 'dummy'

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
    elif field == 'CLIENT' and row[field] == 'Blackrock Financial Management':
        sub_name = 'Blackrock Inc.'
    elif field == 'CPTY_DESC' and row[field] == 'CITADEL (SURVEYOR)':
        sub_name = 'Citadel Investment'
    elif field == 'CPTY_DESC' and row[field] == 'CAPGROUP (CAPITAL RESEARCH & MGMT)':
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
    # CUSTOMER_DETAIL_REPORT_df['CPTY_DESC'] = CUSTOMER_DETAIL_REPORT_df['CPTY_DESC'].apply(client_name_decode)
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
    # client_df = client_df[client_df['CLIENT_ID']<1008]

    return client_df


def get_clients_all_cols(con):
    client_sql = "SELECT *" \
                 " FROM CLIENT" \
                 " order by CLIENT_ID, NAME"

    client_df = pd.read_sql(client_sql, con)
    client_df = client_df[client_df['NAME'].str.len() > 0]
    client_df['NAME'] = client_df['NAME'].apply(client_name_encode)
    client_df['SUB_NAME'] = client_df.apply(lambda x: first_two_name(x, "NAME"), axis=1)
    # client_df = client_df[client_df['CLIENT_ID']<1008]

    return client_df



def get_fidessa():
    fidessa_sql = "SELECT FIDESSA_ID" \
                  ",FIDESSA_ACCOUNT" \
                  " FROM FIDESSA " \
                  " order by FIDESSA_ACCOUNT, FIDESSA_ID"

    fidessa_df = pd.read_sql(fidessa_sql, get_conn_prod())

    fidessa_df = fidessa_df[fidessa_df['FIDESSA_ID'] < 1806]

    return fidessa_df


def get_missing_fidessa(us_fidessa_df, fidessa_df):
    # Matching on VIEW CODE to find out what's missing FIDESSA Account

    us_missing_fidessa_df = pd.merge(us_fidessa_df, fidessa_df, how='left', left_on=['VIEW_CODE'],
                                     right_on=['FIDESSA_ACCOUNT'])
    us_missing_fidessa_df = us_missing_fidessa_df[(us_missing_fidessa_df['FIDESSA_ACCOUNT'].isnull())]
    # us_missing_fidessa_df = us_missing_fidessa_df[(us_missing_fidessa_df['FIDESSA_ID'] > 1805)]

    return us_missing_fidessa_df[['SUB_NAME', 'CPTY_DESC', 'VIEW_CODE']]


def get_missing_client(us_missing_fidessa_df, client_df):
    missing_us_clients_df = pd.merge(us_missing_fidessa_df, client_df, how='left', on=['SUB_NAME'])
    missing_us_clients_df = missing_us_clients_df[missing_us_clients_df['CLIENT_ID'].isnull()]
    # missing_us_clients_df = missing_us_clients_df[missing_us_clients_df['CLIENT_ID'] >= 1008]
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


def get_missing_client_with_tier(us_missing_fidessa_df, client_df):
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
    max_client_id = 1007
    us_missing_client_to_add['CLIENT_ID'] = index_series.apply(
        lambda ( idx ): _max_add_1(idx, max_client_id))
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

    fidessa_to_add = missing_fidessa_df[['VIEW_CODE']]
    fidessa_to_add.reset_index(inplace=True)
    index_series = fidessa_to_add.index.to_series()
    # max_fidessa_id = _max_fidessa_id()
    max_fidessa_id = 1805
    print 'max fidessa id is {:d}'.format(max_fidessa_id)
    fidessa_to_add['FIDESSA_ID'] = index_series.apply(lambda (idx): _max_add_1(idx, max_fidessa_id))

    fidessa_to_add['CTP'] = 0
    fidessa_to_add['USTP'] = 1

    fidessa_to_add = fidessa_to_add.rename(columns={'VIEW_CODE': 'FIDESSA_ACCOUNT'})
    fidessa_to_add = fidessa_to_add[cols]

    return fidessa_to_add


def insert_client(us_missing_client_df, isUAT=True):
    for row in us_missing_client_df.iterrows():
        if row[1]['NAME'] == 'AXA FOR IOI\'S':

            insert_client_sql = 'INSERT INTO Portfolio.dbo.[CLIENT] (CLIENT_ID, NAME, FIDESSA_IOI, TRADE_CHAT,EMAIL,SHORT_NAME,STATUS,TM_NAME,TIER) VALUES({:d},concat(\'AXA for IOI\',char(39),\'s\'),\'{:s}\',\'{:s}\',\'{:s}\',\'{:s}\',\'{:s}\',\'{:s}\',{:d})' \
                .format(int(row[1]['CLIENT_ID']),
                        row[1]['FIDESSA_IOI'],
                        row[1]['TRADE_CHAT'],
                        row[1]['EMAIL'],
                        row[1]['SHORT_NAME'],
                        row[1]['STATUS'],
                        row[1]['TM_NAME'],
                        int(row[1]['TIER']))

        elif row[1]['NAME'] == 'MOODY\'S CORPORATION':

            insert_client_sql = 'INSERT INTO Portfolio.dbo.[CLIENT] (CLIENT_ID, NAME, FIDESSA_IOI, TRADE_CHAT,EMAIL,SHORT_NAME,STATUS,TM_NAME,TIER) VALUES({:d},concat(\'MOODY\',char(39),\'s CORPORATION\'),\'{:s}\',\'{:s}\',\'{:s}\',\'{:s}\',\'{:s}\',\'{:s}\',{:d})' \
                .format(int(row[1]['CLIENT_ID']),
                        row[1]['FIDESSA_IOI'],
                        row[1]['TRADE_CHAT'],
                        row[1]['EMAIL'],
                        row[1]['SHORT_NAME'],
                        row[1]['STATUS'],
                        row[1]['TM_NAME'],
                        int(row[1]['TIER']))
        elif row[1]['NAME'] == 'SCI FOR ETF\'S':

            insert_client_sql = 'INSERT INTO Portfolio.dbo.[CLIENT] (CLIENT_ID, NAME, FIDESSA_IOI, TRADE_CHAT,EMAIL,SHORT_NAME,STATUS,TM_NAME,TIER) VALUES({:d},concat(\'SCI FOR ETF\',char(39),\'s\'),\'{:s}\',\'{:s}\',\'{:s}\',\'{:s}\',\'{:s}\',\'{:s}\',{:d})' \
                .format(int(row[1]['CLIENT_ID']),
                        row[1]['FIDESSA_IOI'],
                        row[1]['TRADE_CHAT'],
                        row[1]['EMAIL'],
                        row[1]['SHORT_NAME'],
                        row[1]['STATUS'],
                        row[1]['TM_NAME'],
                        int(row[1]['TIER']))

        else:
            insert_client_sql = 'INSERT INTO Portfolio.dbo.[CLIENT] (CLIENT_ID, NAME, FIDESSA_IOI, TRADE_CHAT,EMAIL,SHORT_NAME,STATUS,TM_NAME,TIER) VALUES({:d},\'{:s}\',\'{:s}\',\'{:s}\',\'{:s}\',\'{:s}\',\'{:s}\',\'{:s}\',{:d})' \
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
            con.execute(insert_client_sql)
            con.commit()

        else:
            con = get_conn_prod()
            id_on = "SET IDENTITY_INSERT CLIENT ON"
            id_off = "SET IDENTITY_INSERT CLIENT OFF"
            con.execute(id_on)
            con.execute(insert_client_sql)
            con.execute(id_off)
            con.commit()


def insert_fidessa(missing_fidessa_df, isUAT=True):
    for row in missing_fidessa_df.iterrows():

        if isUAT:
            insert_fidessa_sql = 'INSERT INTO Portfolio.dbo.[FIDESSA] VALUES({:d},\'{:s}\',{:d},{:d})' \
                .format(int(row[1]['FIDESSA_ID']), row[1]['FIDESSA_ACCOUNT'], int(row[1]['CTP']), int(row[1]['USTP']))
            print insert_fidessa_sql

            con = get_conn_uat()

            con.execute(insert_fidessa_sql)
            con.commit()
        else:

            insert_fidessa_sql = 'INSERT INTO Portfolio.dbo.[FIDESSA] (FIDESSA_ID, FIDESSA_ACCOUNT,CTP,USTP ) VALUES({:d},\'{:s}\',{:d},{:d})' \
                .format(int(row[1]['FIDESSA_ID']), str(row[1]['FIDESSA_ACCOUNT']), int(row[1]['CTP']),
                        int(row[1]['USTP']))
            print insert_fidessa_sql

            con = get_conn_prod()

            id_on = "SET IDENTITY_INSERT FIDESSA ON"
            id_off = "SET IDENTITY_INSERT FIDESSA OFF"
            con.execute(id_on)
            con.execute(insert_fidessa_sql)
            con.execute(id_off)
            con.commit()


def client_fidessa_to_add(missing_client_df, client_to_add_df, fidessa_to_add_df):
    # missing_client_df is 1 client name to many fidessa account
    # client_to_add_dif is 1 client name to 1 fidessa, and it also has the right clientID

    missing_client_df['CLIENT'] = missing_client_df['CLIENT'].apply(client_name_decode)
    client_to_fidessa = pd.merge(fidessa_to_add_df, missing_client_df, how='left',
                                 left_on=['FIDESSA_ACCOUNT'],
                                 right_on=['FIDESSA_IOI'])

    client_to_fidessa = client_to_fidessa[['FIDESSA_ID', 'CLIENT']]

    client_to_fidessa = pd.merge(client_to_fidessa,
                                 client_to_add_df,
                                 how='left',
                                 left_on=['CLIENT'],
                                 right_on=['NAME'])

    return client_to_fidessa[['CLIENT_ID', 'FIDESSA_ID']]


def insert_client_fidessa(client_to_fidessa, isUAT=True):
    for row in client_to_fidessa.iterrows():
        insert_client_fidessa_sql = 'INSERT INTO Portfolio.dbo.CLIENT_FIDESSA (CLIENT_ID, FIDESSA_ID) VALUES({:d},{:d})' \
            .format(int(row[1]['CLIENT_ID']), int(row[1]['FIDESSA_ID']))
        print insert_client_fidessa_sql

        if isUAT:
            con = get_conn_uat()
            con.execute(insert_client_fidessa_sql)
            con.commit()

        else:
            con = get_conn_prod()
            id_on = "SET IDENTITY_INSERT CLIENT_FIDESSA ON"
            id_off = "SET IDENTITY_INSERT CLIENT_FIDESSA OFF"
            con.execute(id_on)
            con.execute(insert_client_fidessa_sql)
            con.execute(id_off)
            con.commit()


def table_backup(db, table, isUAT=True):
    select_sql = 'select * from {:s}.dbo.{:s}'.format(db, table)

    if isUAT:
        con = get_conn_uat()
    else:
        con = get_conn_prod()

    df = pd.read_sql(select_sql, con)

    file_name = 'C:\Temp\{:s}.csv'.format(table)

    print len(df), file_name
    df.to_csv(file_name)
    return


def ioiTargets():
    US_IOI_Targets_df = pd.read_csv("C:\Temp\IOI_targets.csv")
    US_IOI_Targets_df = US_IOI_Targets_df[~US_IOI_Targets_df['Counterparty'].isnull()]
    US_IOI_Targets_df = US_IOI_Targets_df[US_IOI_Targets_df['Active'] == 1]
    US_IOI_Targets_df = US_IOI_Targets_df[['Description', 'Service', 'Terminal']]
    US_IOI_Targets_df = US_IOI_Targets_df.rename(columns={'Description': 'CLIENT',
                                                          'Service': 'VENDOR',
                                                          'Terminal': 'ROUTING_ID'
                                                          })
    US_IOI_Targets_df = US_IOI_Targets_df.sort_values(['ROUTING_ID', 'VENDOR'])
    US_IOI_Targets_df.drop_duplicates(inplace=True)

    return US_IOI_Targets_df


def current_ioiTargets():
    # Existing IOI Targets
    IOI_target_sql = "SELECT [IOITARGET_ID],[ROUTING_ID],[VENDOR] FROM [Portfolio].[dbo].[IOITARGET]"
    IOI_target_df = pd.read_sql(IOI_target_sql, get_conn_prod())
    IOI_target_df['VENDOR'] = IOI_target_df['VENDOR'].map(string.upper)

    return IOI_target_df


def ioiTarget_to_add(US_IOI_Targets_df, current_IOI_Targets_df):
    cols = ['IOITARGET_ID', 'ROUTING_ID', 'VENDOR']

    Missing_IOI_Targets_df = pd.merge(US_IOI_Targets_df, current_IOI_Targets_df, how='left',
                                      on=['ROUTING_ID', 'VENDOR'])
    Missing_IOI_Targets_df = Missing_IOI_Targets_df[Missing_IOI_Targets_df['IOITARGET_ID'].isnull()]

    IOI_Targets_To_Add = Missing_IOI_Targets_df[['ROUTING_ID', 'VENDOR']]
    IOI_Targets_To_Add.drop_duplicates(inplace=True)
    IOI_Targets_To_Add.reset_index(inplace=True, drop=True)
    index_series = IOI_Targets_To_Add.index.to_series()

    max_IOI_Target_ID = max(current_IOI_Targets_df['IOITARGET_ID'])
    IOI_Targets_To_Add['IOITARGET_ID'] = index_series.apply(lambda (idx): _max_add_1(idx, max_IOI_Target_ID))

    IOI_Targets_To_Add = IOI_Targets_To_Add[cols]

    return IOI_Targets_To_Add


def insertIOITarget(ioiTarget_to_add_df, isUAT=True):
    for row in ioiTarget_to_add_df.iterrows():
        insert_IOITARGET_sql = 'INSERT INTO Portfolio.dbo.IOITARGET (IOITARGET_ID, ROUTING_ID, VENDOR) VALUES({:d},\'{:s}\',\'{:s}\')' \
            .format(int(row[1]['IOITARGET_ID']), str(row[1]['ROUTING_ID']), str(row[1]['VENDOR']))
        print insert_IOITARGET_sql

        if isUAT:
            con = get_conn_uat()
        else:
            con = get_conn_prod()

        id_on = "SET IDENTITY_INSERT IOITARGET ON"
        id_off = "SET IDENTITY_INSERT IOITARGET OFF"
        con.execute(id_on)
        con.execute(insert_IOITARGET_sql)
        con.execute(id_off)
        con.commit()


def get_missing_Client_IOI_Target():
    missing_us_clients_df = get_clients()
    missing_us_clients_df = missing_us_clients_df[missing_us_clients_df['CLIENT_ID'] >= 1008]

    current_ioiTargets_df = current_ioiTargets()
    US_IOI_Targets_df = ioiTargets()

    current_ioiTargets_df = pd.merge(current_ioiTargets_df, US_IOI_Targets_df, how='left', on=['VENDOR', 'ROUTING_ID'])
    current_ioiTargets_df = current_ioiTargets_df[~current_ioiTargets_df['CLIENT'].isnull()]
    current_ioiTargets_df['CLIENT'] = current_ioiTargets_df['CLIENT'].map(str)
    current_ioiTargets_df['SUB_NAME'] = current_ioiTargets_df.apply(lambda x: first_two_name(x, "CLIENT"),
                                                                    axis=1)

    missing_client_ioiTargets_df = pd.merge(missing_us_clients_df, current_ioiTargets_df,
                                            how='left',
                                            on=['SUB_NAME'])

    missing_client_ioiTargets_df = missing_client_ioiTargets_df[['CLIENT_ID', 'VENDOR', 'ROUTING_ID']]
    missing_client_ioiTargets_df = missing_client_ioiTargets_df[~missing_client_ioiTargets_df['ROUTING_ID'].isnull()]
    missing_client_ioiTargets_df.drop_duplicates(inplace=True)

    return missing_client_ioiTargets_df


def client_ioiTaraget_to_add(missing_client_ioiTargets_df):
    current_ioiTargets_df = current_ioiTargets()
    client_ioitarget_to_add = pd.merge(missing_client_ioiTargets_df,
                                       current_ioiTargets_df,
                                       how='left',
                                       on=['VENDOR', 'ROUTING_ID'])

    client_ioitarget_to_add = client_ioitarget_to_add[['CLIENT_ID', 'IOITARGET_ID']]

    return client_ioitarget_to_add


def insertClientIOITarget(client_ioiTaraget_to_add_df, isUAT=True):
    for row in client_ioiTaraget_to_add_df.iterrows():
        insert_CLIENT_IOITARGET_sql = 'INSERT INTO Portfolio.dbo.CLIENT_IOITARGET (CLIENT_ID,IOITARGET_ID) VALUES({:d},{:d})' \
            .format(int(row[1]['CLIENT_ID']), int(row[1]['IOITARGET_ID']))
        print insert_CLIENT_IOITARGET_sql

        if isUAT:
            con = get_conn_uat()
            con.execute(insert_CLIENT_IOITARGET_sql)
            con.commit()
        else:
            con = get_conn_prod()
            con.execute(insert_CLIENT_IOITARGET_sql)
            con.commit()


def clientFidessaIOIMapping():
    sql = "SELECT C.NAME, C.SHORT_NAME, F.FIDESSA_ACCOUNT, F.FIDESSA_ID, F.CTP, F.USTP, C.CLIENT_ID, C.TM_NAME, I.IOITARGET_ID, I.ROUTING_ID, I.VENDOR FROM"
    sql += " [Portfolio].[dbo].CLIENT AS C"
    sql += " INNER JOIN [Portfolio].[dbo].CLIENT_FIDESSA AS CF"
    sql += " ON C.CLIENT_ID = CF.CLIENT_ID"
    sql += " INNER JOIN [Portfolio].[dbo].FIDESSA AS F"
    sql += " ON F.FIDESSA_ID = CF.FIDESSA_ID"
    sql += " INNER JOIN [Portfolio].[dbo].[CLIENT_IOITARGET] AS CIOI"
    sql += " ON CIOI.CLIENT_ID = CF.CLIENT_ID"
    sql += " INNER JOIN [Portfolio].[dbo].[IOITARGET] AS I"
    sql += " ON I.IOITARGET_ID = CIOI.IOITARGET_ID"
    sql += " and I.IOITARGET_ID > 1065"
    sql += " order by C.NAME, C.SHORT_NAME"

    client_fidesssa_ioi_df = pd.read_sql(sql, get_conn_prod())

    return client_fidesssa_ioi_df


# clientFidessaMapping = clientFidessaIOIMapping()

def clientFidessaMapping():
    sql = "SELECT C.NAME, C.SHORT_NAME, F.FIDESSA_ACCOUNT, F.FIDESSA_ID, F.CTP, F.USTP, C.CLIENT_ID, C.TM_NAME FROM"
    sql += " [Portfolio].[dbo].CLIENT AS C"
    sql += " INNER JOIN [Portfolio].[dbo].CLIENT_FIDESSA AS CF"
    sql += " ON C.CLIENT_ID = CF.CLIENT_ID"
    sql += " INNER JOIN [Portfolio].[dbo].FIDESSA AS F"
    sql += " ON F.FIDESSA_ID = CF.FIDESSA_ID"
    sql += " order by C.NAME, C.SHORT_NAME"

    client_fidesssa_df = pd.read_sql(sql, get_conn_prod())

    return client_fidesssa_df

# def dupeTierByClient():

#     SELECT C.NAME,F.FIDESSA_ACCOUNT, F.CTP, F.USTP, C.CLIENT_ID, C.TM_NAME,C.TIER, C.TIER_US, I.ROUTING_ID, I.VENDOR FROM
# [Portfolio].[dbo].CLIENT AS C
# INNER JOIN [Portfolio].[dbo].CLIENT_FIDESSA AS CF
# ON C.CLIENT_ID = CF.CLIENT_ID
# INNER JOIN [Portfolio].[dbo].FIDESSA AS F
# ON F.FIDESSA_ID = CF.FIDESSA_ID
# INNER JOIN [Portfolio].[dbo].[CLIENT_IOITARGET] AS CIOI
# ON CIOI.CLIENT_ID = CF.CLIENT_ID
# INNER JOIN [Portfolio].[dbo].[IOITARGET] AS I
# ON I.IOITARGET_ID = CIOI.IOITARGET_ID
# where I.ROUTING_ID in ('OPCE','OPPE','MFC','JHAN','TDNYFX42','TDSI','ZIFF','588','CAPG','CAPGIOI','CITA','CPRE','DBK','DEUT','DWS')
# order by I.ROUTING_ID,C.NAME, C.SHORT_NAME
#
# update [Portfolio].[dbo].CLIENT
# set TIER_US=1
# where CLIENT_ID in (1329)

def ioiTarget( x):
    x = str(x)
    if not x:
        return ''
    else:
        re_1 = re.search('_IOI',x )
        if re_1 is None:
            return ''
        else:
            endPos = re_1.start()

        re_2 = re.search('RoutingID=""""', x )
        if re_2 is None:
            return ''
        else:
            startPos = re_2.end()

        return x[startPos:endPos]

def _addQuotes(x):
    return ''.join(["'",x,"'"])

# tier_df = pd.read_csv("C:\Temp\\tier2.csv")
# tier_df = tier_df[(tier_df['Tier2'].str.startswith('RoutingID')) | (tier_df['Tier1'].str.startswith('RoutingID'))]
# tier_df['Tier2_x'] = tier_df[['Tier2']].apply(ioiTarget,axis=1)
# tier_df['Tier1_x'] = tier_df[['Tier1']].apply(ioiTarget,axis=1)
# ','.join(list(tier_df['Tier1_x'].map(_addQuotes)))

def dropTable(tableName):
    drop_table_sql = 'drop table {}'.format(tableName)
    uat_con = get_conn_uat()
    uat_con.execute(drop_table_sql)
    uat_con.commit()


def createClientTable():
    create_client_sql = "CREATE TABLE Portfolio.dbo.CLIENT_x (CLIENT_ID int not null, \
                                                                    NAME nvarchar(128) not null, \
                                                                    FIDESSA_IOI nvarchar(max) null, \
                                                                    TRADE_CHAT nvarchar(128) null, \
                                                                    EMAIL nvarchar(256) null, \
                                                                    SHORT_NAME nvarchar(128) null,\
                                                                    STATUS nvarchar(max) null, \
                                                                    TM_NAME nvarchar(max) null, \
                                                                    TIER int null, \
                                                                    PRIMARY KEY (CLIENT_ID))"

    uat_con = get_conn_uat()
    uat_con.execute(create_client_sql)
    uat_con.commit()


def updateUSTier(isUAT=True):
    def _update(client_id, tier):

        update_sql = "update [Portfolio].[dbo].[CLIENT]"
        update_sql += " set TIER_US={}".format(tier)
        update_sql += " where CLIENT_ID={}".format(client_id)

        print update_sql

        if isUAT:
            con = get_conn_uat()
        else:
            con = get_conn_prod()

        con.execute(update_sql)
        con.commit()

    tiers_df = get_tiers()
    tiers_df['SUB_NAME'] = tiers_df.apply(lambda (x): first_two_name(x, 'CLIENT'), axis=1)
    client_df = get_clients()
    client_df = pd.merge(client_df, tiers_df, how='left', on=['SUB_NAME'])

    for row in client_df.iterrows():
        if not np.isnan(row[1]['TIER']):
            _update(int(row[1]['CLIENT_ID']), int(row[1]['TIER']))

# updateUSTier()

def updateUSTier_2(isUAT=True):
    def _update(client_id, tier, con):

        update_sql = "update [Portfolio].[dbo].[CLIENT]"
        update_sql += " set TIER_US={}".format(tier)
        update_sql += " where CLIENT_ID={}".format(client_id)

        print update_sql

        con.execute(update_sql)
        con.commit()

    con = get_conn_uat() if isUAT else get_conn_prod()
    client_df = get_clients_all_cols(con)
    count = 0

    for row in client_df.iterrows():
        if (not np.isnan(row[1]['TIER'])) and (np.isnan(row[1]['TIER_US'])):
            if row[1]['TIER'] > 0 and row[1]['TIER'] <= 8:
                _update(int(row[1]['CLIENT_ID']), 2, con)
        elif row[1]['TIER_US'] == 3:
            _update(int(row[1]['CLIENT_ID']), 9, con)
            count = count+1
            print 'updated total{}'.format(count)


updateUSTier_2(isUAT=False)

def setTIER2Null(isUAT=True):

    update_sql = "update [Portfolio].[dbo].[CLIENT]"
    update_sql += " set TIER=NULL"
    update_sql += " where CLIENT_ID >= 1008"

    print update_sql

    if isUAT:
        con = get_conn_uat()
    else:
        con = get_conn_prod()

    con.execute(update_sql)
    con.commit()

# setTIER2Null(isUAT=False)

# clientFidessaMapping = clientFidessaMapping()
# run codes...

def add_fidessa_account():
    us_fidessa_df = get_us_fidessa_clients()
    fidessa_df = get_fidessa()

    fidessa_to_add_df = create_fidessa_adds(us_fidessa_df, fidessa_df)
    insert_fidessa(fidessa_to_add_df, isUAT=False)


# fidessa = add_fidessa_account()

def add_client():
    us_fidessa_df = get_us_fidessa_clients()
    fidessa_df = get_fidessa()
    raw_missing_client_df = get_missing_fidessa(us_fidessa_df, fidessa_df)
    client_df = get_clients()
    missing_client_with_tiers_df = get_missing_client_with_tier(raw_missing_client_df, client_df)
    clients_to_add_df = create_client_adds(missing_client_with_tiers_df);
    insert_client(clients_to_add_df, isUAT=True)


# client = add_client()

def add_client_fidessa():
    us_fidessa_df = get_us_fidessa_clients()
    fidessa_df = get_fidessa()
    fidessa_to_add_df = create_fidessa_adds(us_fidessa_df, fidessa_df)

    # get the client name back
    fidessa_to_add_df = pd.merge(fidessa_to_add_df, us_fidessa_df, how='left', left_on=['FIDESSA_ACCOUNT'],
                                 right_on=['VIEW_CODE'])

    # get stored client
    client_df = get_clients()
    client_df.sort_values(['SUB_NAME'], inplace=True)

    client_fidessa_to_add_df = pd.merge(fidessa_to_add_df, client_df, how='left', on=['SUB_NAME'])
    client_fidessa_to_add_df = client_fidessa_to_add_df[~client_fidessa_to_add_df['CLIENT_ID'].isnull()]
    client_fidessa_to_add_df = client_fidessa_to_add_df[['CLIENT_ID', 'FIDESSA_ID']]

    insert_client_fidessa(client_fidessa_to_add_df, isUAT=False)


# client_fidessa = add_client_fidessa()

def add_ioiTarget():
    US_IOI_Targets_df = ioiTargets()
    current_IOI_Targets_df = current_ioiTargets()
    ioiTarget_to_add_df = ioiTarget_to_add(US_IOI_Targets_df, current_IOI_Targets_df)

    insertIOITarget(ioiTarget_to_add_df, isUAT=False)


# add_ioiTarget()


def add_client_ioiTarget():
    missing_client_IOITarget_df = get_missing_Client_IOI_Target()
    client_ioiTaraget_to_add_df = client_ioiTaraget_to_add(missing_client_IOITarget_df)

    insertClientIOITarget(client_ioiTaraget_to_add_df, isUAT=False)

# add_client_ioiTarget()
