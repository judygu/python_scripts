import blpapi
import pandas as pd
import pyodbc
from optparse import OptionParser
from datetime import datetime
from datetime import date
import time

SECURITY_DATA = blpapi.Name("securityData")
SECURITY = blpapi.Name("security")
FIELD_DATA = blpapi.Name("fieldData")
FIELD_EXCEPTIONS = blpapi.Name("fieldExceptions")
FIELD_ID = blpapi.Name("fieldId")
ERROR_INFO = blpapi.Name("errorInfo")


def _dbCon(is_uat):
    if is_uat:
        return pyodbc.connect('DRIVER={SQL Server};SERVER=T65-EES-UAT\EES_UAT;DATABASE=Portfolio;UID=sa;PWD=123456Dma')
    else:
        return pyodbc.connect(
            'DRIVER={SQL Server};SERVER=eessql.gss.scotia-capital.com,5150;DATABASE=Portfolio;UID=dmamso;PWD=abc1234$6')


def _insertDB(vol_df):
    dbCon = _dbCon(True)

    for row in vol_df.iterrows():
        insert_vol_sql = 'INSERT INTO SCPWAD_UAT.dbo.[VOLATILITY] VALUES(\'{:s}\',\'{:s}\',\'{:s}\',\'{:s}\',\'{:s}\',\'{:s}\')' \
            .format(str(row[1]['TIME']),
                    str(row[1]['RIC']),
                    str(row[1]['BBG']),
                    str(row[1]['CALL_IMP_VOL_10D']),
                    str(row[1]['PUT_IMP_VOL_10D']),
                    str(row[1]['INTERVAL_VOLATILITY']))
        print insert_vol_sql
        dbCon.execute(insert_vol_sql)
        dbCon.commit()


def parseCmdLine():
    parser = OptionParser(description="Retrieve reference data.")
    parser.add_option("-a",
                      "--ip",
                      dest="host",
                      help="server name or IP (default: %default)",
                      metavar="ipAddress",
                      default="localhost")
    parser.add_option("-p",
                      dest="port",
                      type="int",
                      help="server port (default: %default)",

                      metavar="tcpPort",
                      default=8194)

    (options, args) = parser.parse_args()

    return options


def processMessage(msg, bbg_rics):
    if not msg.hasElement(SECURITY_DATA):
        print "Unexpected message:"
        print msg
        return

    securityDataArray = msg.getElement(SECURITY_DATA)
    vol_ar = []
    for securityData in securityDataArray.values():
        vol_dict = {}
        bbg_name = securityData.getElementAsString(SECURITY)
        # print bbg_name
        vol_dict['BBG'] = bbg_name
        vol_dict['RIC'] = bbg_rics[bbg_name]

        fieldData = securityData.getElement(FIELD_DATA)
        for field in fieldData.elements():
            if field.isValid():
                vol_dict[str(field.name())] = field.getValueAsFloat()
            else:
                print field.name(), " is NULL"

        # print vol_dict
        if vol_dict.has_key('CALL_IMP_VOL_10D') and vol_dict.has_key('PUT_IMP_VOL_10D') and vol_dict.has_key(
                'INTERVAL_VOLATILITY'):
            vol_ar.append(vol_dict)

        fieldExceptionArray = securityData.getElement(FIELD_EXCEPTIONS)
        for fieldException in fieldExceptionArray.values():
            errorInfo = fieldException.getElement(ERROR_INFO)
            print errorInfo.getElementAsString("category"), ":", \
                fieldException.getElementAsString(FIELD_ID)

    return vol_ar


def main():
    global options
    options = parseCmdLine()

    # Fill SessionOptions
    sessionOptions = blpapi.SessionOptions()
    sessionOptions.setServerHost(options.host)
    sessionOptions.setServerPort(options.port)

    print "Connecting to %s:%d" % (options.host, options.port)

    # Create a Session
    session = blpapi.Session(sessionOptions)

    # Start a Session
    if not session.start():
        print "Failed to start session."
        return

    if not session.openService("//blp/refdata"):
        print "Failed to open //blp/refdata"
        return

    refDataService = session.getService("//blp/refdata")
    request = refDataService.createRequest("ReferenceDataRequest")

    # append securities to request
    ric_df = pd.read_csv("C:\Temp\IOI.csv")
    rics = ric_df.columns.tolist()
    rics = [ric.strip() for ric in rics]
    rics = list(set(rics))
    rics.sort()

    bbgs = [ric.split(".")[0] for ric in rics]
    bbgs = [' '.join([bbg, 'US EQUITY']) for bbg in bbgs]

    assert (len(rics) == len(bbgs))
    bbg_rics = dict(zip(bbgs, rics))

    [request.append("securities", bbg) for bbg in bbgs]

    # append fields to request
    vol_fields = ["CALL_IMP_VOL_10D", "PUT_IMP_VOL_10D", "INTERVAL_VOLATILITY"]
    [request.append("fields", vol_field) for vol_field in vol_fields]

    # add overrides
    overrides = request.getElement("overrides")
    override1 = overrides.appendElement()
    override1.setElement("fieldId", "TECH_STUDIES_PARAM1_OVERRIDE")
    override1.setElement("value", "10")

    vol_ar = []

    print "Sending Request:", request
    print '{:%Y-%m-%d %H:%M }'.format(datetime.now())
    cid = session.sendRequest(request)

    try:
        mkt_close = datetime(date.today().year, date.today().month, date.today().day, 16, 0, 0)
        while (datetime.now() < mkt_close):

            # We provide timeout to give the chance to Ctrl+C handling:
            ev = session.nextEvent(500)
            for msg in ev:
                # print 'hallo'
                if cid in msg.correlationIds():
                    vol_ar.extend(processMessage(msg, bbg_rics))
            # Response completely received, so we could exit
            if ev.eventType() == blpapi.Event.RESPONSE:
                if len(vol_ar) > 0:
                    vol_df = pd.DataFrame(vol_ar)
                    vol_df['TIME'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    vol_df = vol_df[['TIME', 'RIC', 'BBG'] + vol_fields]

                    _insertDB(vol_df)

                time.sleep(5 * 60)
                vol_ar = []
                print "Sending Request:", request
                print '{:%Y-%m-%d %H:%M }'.format(datetime.now())
                cid = session.sendRequest(request)

    finally:
        # Stop the session
        session.stop()


if __name__ == "__main__":
    print "BBG_VOL"
    try:
        main()
    except KeyboardInterrupt:
        print "Ctrl+C pressed. Stopping..."
