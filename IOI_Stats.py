import pandas as pd
import numpy as np
import datetime
from datetime import date
import time

def log_parser(strToParse, fieldsToExtract):
    fname = "C:\Users\jgu\equitytrading\\trunk\ScotiaOrderTracker\ScotiaOrderTrackerEngine\log\SOTEngine.log"
    ioi_stats = []
    with open(fname) as f:
        content = f.read().splitlines()
    for x in content:
        if x.find(strToParse) != -1:
            s = x.index('[')
            e = len(x)
            ar = x[s + 1:e - 1].split(",")
            data = {}
            for r in ar:
                [k, v] = r.split("=")
                k = k.strip()
                if len(fieldsToExtract) > 0:
                    if k in fieldsToExtract:
                        data[k] = v
                else:
                    data[k] = v

            ioi_stats.append(data)

    return pd.DataFrame(ioi_stats)


def main():
    ioi_analysis_df = log_parser('US Principal IOI Sender - IOIing', [])

    if len(ioi_analysis_df)==0:
        return

    cols = ['ticker', 'time', 'vol_discount', 'ask', 'bid', 'dailyVol', 'ioiShares','trade_duration', 'remainingVolumePct',
            'targetVolumePct']
    cols_to_rename = ['ticker', 'time', 'volDiscount(bps)', 'ask', 'bid', 'dailyVol(pct)', 'shares','tradeDuration(minutes)',
                      'remainingVolume(pct)', 'targetVolume(pct)']

    cols_dict = dict((key, value) for (key, value) in zip(cols, cols_to_rename))

    ioi_analysis_df = ioi_analysis_df.sort_values(['ticker', 'time'])
    ioi_analysis_df.drop_duplicates(inplace=True)
    ioi_analysis_df['time'] = pd.to_datetime(ioi_analysis_df['time'])
    ioi_analysis_df['vol'] = ioi_analysis_df['vol'].map(np.double) * 100
    ioi_analysis_df['remainingVolumePct'] = ioi_analysis_df['remainingVolumePct'].map(np.double) * 100
    ioi_analysis_df['targetVolumePct'] = ioi_analysis_df['targetVolumePct'].map(np.double) * 100
    ioi_analysis_df = ioi_analysis_df[cols]
    ioi_analysis_df = ioi_analysis_df.rename(columns=cols_dict)

    ioi_analysis_df.to_csv("Z:\IOIStats\{}.csv".format(datetime.datetime.now().strftime("%Y%m%d%H%M%S")))
    print "stored {} ioi stats at {}".format(len(ioi_analysis_df), datetime.datetime.now().strftime("%Y%m%d%H%M%S"))


if __name__ == "__main__":
    print "IOI_Stats"
    try:
        mkt_close = datetime.datetime(date.today().year, date.today().month, date.today().day, 16, 0, 0)
        while (datetime.datetime.now() < mkt_close):
            main()
            time.sleep(2 * 60 * 60)
    except KeyboardInterrupt:
        print "Ctrl+C pressed. Stopping..."
