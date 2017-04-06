
import time
import pickle
import datetime

from common import log, get_files, commit_files, euid

import elasticsearch
from elasticsearch_dsl import Search, A, Q
import logging


logging.basicConfig(level=logging.WARN)

transfers_raw_index = 'gracc.osg-transfer.raw-*'
transfers_summary_index = 'gracc.osg-transfer.summary'

def gracc_query_transfers(es, index, starttime, endtime, interval):
    s = Search(using=es, index=index)

    s = s.query('bool',
            filter=[
             Q('range', StartTime={'gte': starttime, 'lt': endtime })
          & ~Q('terms', SiteName=['NONE', 'Generic', 'Obsolete'])
        ]
    )

    curBucket = s.aggs.bucket('StartTime', 'date_histogram',
                              field='StartTime', interval=interval)

    curBucket = curBucket.metric('Network', 'sum', field='Network')
    curBucket = curBucket.metric('Records', 'sum', field='Njobs')

    response = s.execute()
    return response


class TransferData(object):
    """
    Information about a single hour's worth of tranfers from the DB.
    Meant to be saved in the cache, keeps track of the creationtime of the
    object itself so the cache knows when to expire it.
    """

    def __init__(self):
        self.starttime = None
        self.endtime = None
        self.count = None
        self.volume_mb = None
        self.createtime = time.time()

class DataSourceTransfers(object):
    """
    A data source which queries (and caches!) hourly transfer information from
    GRACC
    """

    def __init__(self, cp):
        self.cp = cp
        self.data = {}
        self.missing = set()
        
    def run(self):
        self.connect()
        self.load_cached()
        self.determine_missing()
        self.query_missing()
       
    def disconnect(self):
        pass
 
    def connect(self):
        gracc_url = self.cp.get("GRACC Transfer", "Url")
        #gracc_url = 'https://gracc.opensciencegrid.org/q'

        try:
            self.es = elasticsearch.Elasticsearch(
                [gracc_url], timeout=300, use_ssl=True, verify_certs=True,
                ca_certs='/etc/ssl/certs/ca-bundle.crt')
        except Exception, e:
            log.exception(e)
            log.error("Unable to connect to GRACC database")
            raise

    def load_cached(self):
        try:
            data = pickle.load(open(self.cp.get("Filenames", "transfer_data") \
                % {'uid': euid}, "r"))
            # Verify we didn't get useless data
            for time, tdata in data.items():
                assert isinstance(time, datetime.datetime)
                assert isinstance(tdata, TransferData)
                assert isinstance(tdata.starttime, datetime.datetime)
                assert isinstance(tdata.endtime, datetime.datetime)
                assert tdata.count != None
                assert tdata.volume_mb != None
                assert tdata.starttime != None
            self.data = data
            log.info("Successfully loaded transfer data from cache; %i" \
                " cache entries." % len(data))
            remove_data = []
            now = globals()['time'].time()
            now_dt = datetime.datetime.now()
            for time, tdata in data.items():
                 if not hasattr(tdata, 'createtime') or not tdata.createtime:
                     log.debug("Ignoring cached data from %s as it has no " \
                         "create time info." % time)
                     remove_data.append(time)
                     continue
                 if now - tdata.createtime > 3600:
                     log.debug("Ignoring cached data from %s as it is over " \
                         "an hour old." % time)
                     remove_data.append(time)
                 age_starttime = now_dt - tdata.starttime
                 age_starttime = age_starttime.days*86400 + age_starttime.seconds
                 if (now - tdata.createtime > 1800) and (age_starttime <= 12*3600):
                     log.debug("Ignoring cached data from %s as it is over " \
                         "30 minutes old and is for a recent interval." % \
                         time)
                     remove_data.append(time)
            for time in remove_data:
                del self.data[time]
        except Exception, e:
            log.warning("Unable to load cache; it may not exist. Error: %s" % \
               str(e))

    def save_cache(self):
        now = datetime.datetime.now()
        old_keys = []
        for key in self.data.keys():
            if (now - key).days >= 7:
                old_keys.append(key)
        for key in old_keys:
            del self.data[key]
        try:
            name, tmpname = get_files(self.cp, "transfer_data")
            fp = open(tmpname, 'w')
            pickle.dump(self.data, fp)
            fp.close()
            commit_files(name, tmpname)
            log.debug("Saved data to cache.")
        except Exception, e:
            log.warning("Unable to write cache; message: %s" % str(e))

    def _timestamp_to_datetime(self, ts):
        return datetime.datetime(*time.gmtime(ts)[:6])

    def determine_missing(self):
        now = time.time()
        hour_now = now - (now % 3600)
        if (now-hour_now) < 15*60:
            hour_now -= 3600
        self.missing.add(self._timestamp_to_datetime(hour_now))
        self.missing.add(self._timestamp_to_datetime(hour_now-3600))
        self.missing.add(self._timestamp_to_datetime(hour_now-2*3600))
        self.missing.add(self._timestamp_to_datetime(hour_now-3*3600))
        cur = hour_now
        hours = int(self.cp.get("GRACC Transfer", "hours"))
        while cur >= now - hours*3600:
            cur -= 3600
            cur_dt = self._timestamp_to_datetime(cur)
            if cur_dt not in self.data:
                self.missing.add(cur_dt)

    def query_missing(self):
        now = time.time()
        log.info("Querying %i missing data entries." % len(self.missing))
        for mtime in self.missing:
            starttime = mtime
            endtime = mtime + datetime.timedelta(0, 3600)
            results = self.query_transfers(starttime, endtime)
            if not results:
                log.warning("No transfer results found for %s." % starttime)
            for result in results:
                res_time, count, volume_mb = result
                res_time = float(res_time)
                starttime = self._timestamp_to_datetime(res_time)
                if now-res_time >= 3600:
                    endtime = self._timestamp_to_datetime(res_time+3600)
                else:
                    endtime = self._timestamp_to_datetime(now)
                if res_time > now:
                    continue
                td = TransferData()
                td.starttime = starttime
                td.endtime = endtime
                td.count = count
                td.volume_mb = volume_mb
                self.data[starttime] = td
                log.debug("Successfully parsed results for %s." % starttime)
                self.save_cache()

    def query_transfers(self, starttime, endtime):
        log.info("Querying GRACC Transfer index for transfers from %s to %s." \
            % (starttime.strftime("%Y-%m-%d %H:%M:%S"),
            endtime.strftime("%Y-%m-%d %H:%M:%S")))
        params = {'interval': 'hour'}
        params['starttime'] = starttime
        params['endtime'] = endtime

        response = gracc_query_transfers(self.es, transfers_raw_index, **params)

        results = response.aggregations.StartTime.buckets

        all_results = [ (x.key / 1000,
                         x.Records.value,
                         x.Network.value / 1024**2) for x in results ]

        return all_results

    def get_json(self):
        assert self.transfer_results != None
        assert self.transfer_volume_results != None
        total_transfers = sum(self.transfer_results)
        total_transfer_volume = sum(self.transfer_volume_results)
        return {'transfers_hourly': int(total_transfers),
            'transfer_volume_mb_hourly': int(total_transfer_volume)}

    def get_data(self):
        all_times = self.data.keys()
        all_times.sort()
        all_times = all_times[-26:-1]
        results = []
        for time in all_times:
            results.append((int(self.data[time].count), self.data[time].volume_mb))
        self.transfer_results, self.transfer_volume_results = zip(*results)
        return results

    def get_volume_rates(self):
        all_times = self.data.keys()
        all_times.sort()
        all_times = all_times[-26:-1]
        results = []
        for time in all_times:
            td = self.data[time]
            interval = td.endtime - td.starttime
            interval_s = interval.days*86400 + interval.seconds
            results.append(td.volume_mb/interval_s)
        return results

    def get_rates(self):
        all_times = self.data.keys()
        all_times.sort()
        all_times = all_times[-26:-1]
        results = []
        for time in all_times:
            td = self.data[time]
            interval = td.endtime - td.starttime
            interval_s = interval.days*86400 + interval.seconds
            results.append(td.count/float(interval_s))
        return results

