import pandas as pd
import paramiko

startDate = '20180301'
endDate = '20180411'

datelist = [ x.strftime('%Y%m%d') for x in pd.date_range(startDate,endDate, freq='B').tolist()]

hostString = 'dummy'
username = 'dummy'
password = 'dummy'

# Connect to SFTP
t = paramiko.Transport(hostString)
t.connect(username=username, password=password)
sftp = paramiko.SFTPClient.from_transport(t)

for date in datelist:
    try:

        filename = "SBExtractUS - {}.csv".format(date)

        localpath = 'dummy' + filename
        remotepath = 'dummy' + filename

        sftp.get(remotepath=remotepath, localpath=localpath, callback=None)
    except:
        print date
        continue
