
import time
import os.path
import cPickle
import MySQLdb
import datetime
import tempfile
from common import log
from monthdelta import monthdelta

import elasticsearch
from elasticsearch_dsl import Search, A, Q
import logging


logging.basicConfig(level=logging.WARN)

jobs_raw_index = 'gracc.osg.raw-*'
jobs_summary_index = 'gracc.osg.summary'

transfers_raw_index = 'gracc.osg-transfer.raw-*'
transfers_summary_index = 'gracc.osg-transfer.summary'

def gracc_query_jobs(es, index, starttime, endtime, interval):
    s = Search(using=es, index=index)

    s = s.query('bool',
            filter=[
             Q('range', EndTime={'gte': starttime, 'lt': endtime })
          &  Q('term',  ResourceType='Batch')
          & ~Q('terms', SiteName=['NONE', 'Generic', 'Obsolete'])
          & ~Q('terms', VOName=['Unknown', 'unknown', 'other'])
        ]
    )

    curBucket = s.aggs.bucket('EndTime', 'date_histogram',
                              field='EndTime', interval=interval)

    curBucket = curBucket.metric('CoreHours', 'sum', field='CoreHours')
    curBucket = curBucket.metric('Records', 'sum', field='Count')

    response = s.execute()
    return response

def gracc_hourly_query_jobs(es, index, starttime, endtime, offset, interval):
    s = Search(using=es, index=index)

    s = s.query('bool',
            filter=[
             Q('range', EndTime={'gte': starttime, 'lt': endtime })
          &  Q('term',  ResourceType='Batch')
          & ~Q('terms', SiteName=['NONE', 'Generic', 'Obsolete'])
          & ~Q('terms', VOName=['Unknown', 'unknown', 'other'])
        ]
    )

    curBucket = s.aggs.bucket('EndTime', 'date_histogram',
                              field='EndTime', interval=interval,
                              offset="-%ds" % offset)

    curBucket = curBucket.metric('CoreHours', 'sum', field='CoreHours')
    curBucket = curBucket.metric('Records', 'sum', field='Count')

    response = s.execute()
    return response

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

class DataSource(object):

    def __init__(self, cp):
        self.cp = cp

    def run(self):
        self.connect()

    def disconnect(self):
        self.conn.close()

    def connect(self):
        user = self.cp.get("Gratia", "User")
        password = self.cp.get("Gratia", "Password")
        host = self.cp.get("Gratia", "Host")
        database = self.cp.get("Gratia", "Database")
        port = int(self.cp.get("Gratia", "Port"))
        try:
            self.conn = MySQLdb.connect(user=user, passwd=password, host=host,
                port=port, db=database)
            log.info("Successfully connected to Gratia database")
        except Exception, e:
            log.exception(e)
            log.error("Unable to connect to Gratia database")
            raise
        curs = self.conn.cursor()
        curs.execute("set time_zone='+0:00'")

        gracc_url = self.cp.get("Gracc", "Url")
        #gracc_url = 'https://gracc.opensciencegrid.org/q'
        try:
            self.es = elasticsearch.Elasticsearch(
                [gracc_url], timeout=300, use_ssl=True, verify_certs=True,
                ca_certs='/etc/ssl/certs/ca-bundle.crt')
        except Exception, e:
            log.exception(e)
            log.error("Unable to connect to Gracc database")
            raise

    def connect_transfer(self):
        user = self.cp.get("Gratia Transfer", "User")
        password = self.cp.get("Gratia Transfer", "Password")
        host = self.cp.get("Gratia Transfer", "Host")
        database = self.cp.get("Gratia Transfer", "Database")
        port = int(self.cp.get("Gratia Transfer", "Port"))
        try:
            self.conn = MySQLdb.connect(user=user, passwd=password, host=host,
                port=port, db=database)
        except Exception, e:
            log.exception(e)
            log.error("Unable to connect to Gratia Transfer DB")
            raise
        curs=self.conn.cursor()
        curs.execute("set time_zone='+0:00'")

        gracc_url = self.cp.get("Gracc Transfer", "Url")
        #gracc_url = 'https://gracc.opensciencegrid.org/q'
        try:
            self.es = elasticsearch.Elasticsearch(
                [gracc_url], timeout=300, use_ssl=True, verify_certs=True,
                ca_certs='/etc/ssl/certs/ca-bundle.crt')
        except Exception, e:
            log.exception(e)
            log.error("Unable to connect to Gracc database")
            raise

    def getcache(self):
	cachedresultslist=[]
	num_time_cach_read=0
	#check if full refresh needed
        try:
		pickle_f_handle = open(self.cache_count_file_name)
		num_time_cach_read = cPickle.load(pickle_f_handle)
		pickle_f_handle.close()
		if(num_time_cach_read >= self.deprecate_cache_after):
			log.debug("Signaling read complete data from db, reads reached: <%s>" %(num_time_cach_read))
			num_time_cach_read=0
		else:
			num_time_cach_read=num_time_cach_read+1
			log.debug("Incrementing number of cached reads to: <%s>" %(num_time_cach_read))
        except Exception, e:
            log.info("Unable to find cache file: <%s>"%(self.cache_count_file_name))
	#increment the current read
	pickle_f_handle = open(self.cache_count_file_name, "w")
	cPickle.dump(num_time_cach_read, pickle_f_handle)
	pickle_f_handle.close()

	#get cacheifneeded i.e. when num_time_cach_read > 0
        try:
		if(num_time_cach_read>0):
			pickle_f_handle = open(self.cache_data_file_name)
			cachedresultslist = cPickle.load(pickle_f_handle)
			pickle_f_handle.close()
			if(len(cachedresultslist) < self.refreshwindowperiod):
				log.info("Existing cache size:  <%s> is less than refresh window size: <%s>" %(len(cachedresultslist),self.refreshwindowperiod ))
				cachedresultslist=[]
        except Exception, e:
            log.exception(e)
            log.info("Unable to find cache file: <%s>"%(self.cache_data_file_name))

	#modify the params to be sent to DB query
	param = self.get_params()
	end = param['endtime']
	log.debug("Default dates received in getcache are start: <%s> and end: <%s> "%(param['starttime'],param['endtime']))
	start = self.apply_delta(end)

	#remove the cache elements that will be refreshed
	if(len(cachedresultslist) > 0):
		cachedresultslist=cachedresultslist[:(len(cachedresultslist)-self.refreshwindowperiod)]	
		param['starttime'] = start
	else:
		log.debug("Setting date back to  start: <%s> "%(param['starttime']))
	return cachedresultslist, param


class HourlyJobsDataSource(DataSource):
    """
    Hourly view of the Gratia job data
    """

    def __init__(self, cp):
        super(HourlyJobsDataSource, self).__init__(cp)
        self.count_results = None
        self.hour_results = None

    def get_params(self):
        hours = int(int(self.cp.get("Gratia", "hours"))*1.5)
        now = int(time.time()-60)
        prev = now - 3600*hours
        offset = prev % 3600
        starttime = datetime.datetime(*time.gmtime(prev)[:6])
        endtime = datetime.datetime(*time.gmtime(now)[:6])
        return {'offset': offset, 'starttime': starttime, 'endtime': endtime,
                'interval': 'hour'}

    def get_json(self):
        assert self.count_results != None
        assert self.hour_results != None
        num_jobs = sum(self.count_results)
        total_hours = sum(self.hour_results)
        return {'jobs_hourly': int(num_jobs), 'cpu_hours_hourly': float(total_hours)}

    def query_jobs(self):
        params = self.get_params()

        response = gracc_hourly_query_jobs(self.es, jobs_raw_index, **params)

        results = response.aggregations.EndTime.buckets

        all_results = [ (x.Records.value or x.doc_count,
                         x.CoreHours.value,
                         x.key / 1000) for x in results ]

        log.info("GRACC returned %i results for jobs" % len(all_results))
        log.debug("Job result dump:")
        for count, hrs, epochtime in all_results:
            time_tuple = time.gmtime(epochtime)
            time_str = time.strftime("%Y-%m-%d %H:%M", time_tuple)
            log.debug("Time %s: Count %i, Hours %.2f" % (time_str, count, hrs))
        count_results = [i[0] for i in all_results]
        hour_results = [i[1] for i in all_results]
        num_results = int(self.cp.get("Gratia", "hours"))
        count_results = count_results[-num_results-1:-1]
        hour_results = hour_results[-num_results-1:-1]
        self.count_results, self.hour_results = count_results, hour_results
        return count_results, hour_results


class MonthlyDataSource(DataSource):
    refreshwindowperiod=2
    deprecate_cache_after=10 #deprecate cache after these number of reads
    tmpdir=tempfile.gettempdir()
    cache_data_file_name = os.path.join(tmpdir, "monthlydatasource.b")
    cache_count_file_name = os.path.join(tmpdir, "monthlydatasourcecount.b") 

    def apply_delta(self, dateobj):
	returnval = dateobj - monthdelta(self.refreshwindowperiod)
        returnval -= datetime.timedelta(returnval.day-1, 0)
        return returnval
	
    def get_params(self):
        months = int(int(self.cp.get("Gratia", "months"))+2)
        end = datetime.datetime(*(list(time.gmtime()[:2]) + [1,0,0,0]))
        start = end - datetime.timedelta(14*31, 0)
        start -= datetime.timedelta(start.day-1, 0)
        return {'starttime': start, 'endtime': end, 'interval': 'month'}

    def get_json(self):
        assert self.count_results != None
        assert self.hour_results != None
        assert self.transfer_results != None
        assert self.transfer_volume_results != None
        num_jobs = sum(self.count_results)
        total_hours = sum(self.hour_results)
        total_transfers = sum(self.transfer_results)
        total_transfer_volume = sum(self.transfer_volume_results)
        return {'jobs_monthly': int(num_jobs), 'cpu_hours_monthly': \
            float(total_hours), 'transfers_monthly': float(total_transfers),
            'transfer_volume_mb_monthly': total_transfer_volume}

    def query_jobs(self):
        params = self.get_params()

        response = gracc_query_jobs(self.es, jobs_summary_index, **params)

        results = response.aggregations.EndTime.buckets

        all_results = [ (x.Records.value or x.doc_count,
                         x.CoreHours.value,
                         x.key / 1000) for x in results ]

        log.info("GRACC returned %i results for jobs" % len(all_results))
        log.debug("Job result dump:")
        for count, hrs, epochtime in all_results:
            time_tuple = time.gmtime(epochtime)
            time_str = time.strftime("%Y-%m-%d %H:%M", time_tuple)
            log.debug("Month starting on %s: Jobs %i, Job Hours %.2f" %
                (time_str, count, hrs))
        count_results = [i[0] for i in all_results]
        hour_results = [i[1] for i in all_results]
        num_results = int(self.cp.get("Gratia", "months"))
        count_results = count_results[-num_results:]
        hour_results = hour_results[-num_results:]
        self.count_results, self.hour_results = count_results, hour_results
        return count_results, hour_results

    def query_transfers(self):
        self.connect_transfer()
        cachedresultslist, params=self.getcache()
        log.debug("Received  <%s> cached results"%(len(cachedresultslist)))
        log.debug("Received in query_transfers for DB Query start date: <%s> and end date <%s> "%(params['starttime'],params['endtime']))

        response = gracc_query_transfers(self.es, transfers_summary_index,
                                         **params)

        results = response.aggregations.StartTime.buckets

        all_results = [ (x.key / 1000,
                         x.Records.value,
                         x.Network.value / 1024**2) for x in results ]

        cachedresultslist.extend(all_results)
        all_results=cachedresultslist
        log.info( "-------- GRACC returned %i results for transfers----------------" % len(all_results))
        log.debug("-------- Transfer result dump: DB Fetched results----------------" )
        for i in all_results:
            count, mbs = i[1:]
            log.debug("Month starting on %s: Transfers %i, Transfer PB %.2f" % \
                (i[0], count, mbs/1024**2))
        log.debug("-------- Printing cached and DB Merged results----------------" )
        for i in cachedresultslist:
            count, mbs = i[1:]
            log.debug("Month starting on %s: Transfers %i, Transfer PB %.2f" % \
                (i[0], count, mbs/1024**2))
        month_results = [i[0] for i in all_results]
        count_results = [i[1] for i in all_results]
        hour_results = [i[2] for i in all_results]
        num_results = int(self.cp.get("Gratia", "months"))
        month_results = month_results[-num_results:]
        count_results = count_results[-num_results:]
        hour_results = hour_results[-num_results:]

        #write the data to cache file
        pickle_f_handle = open(self.cache_data_file_name, "w")
        cPickle.dump(all_results, pickle_f_handle)
        pickle_f_handle.close()

        self.disconnect()
        self.transfer_results = count_results
        self.transfer_volume_results = hour_results
        return count_results, hour_results


class DailyDataSource(DataSource):
    """
    Data source to provide transfer and job information over the past 30
    days.  Queries the Gratia summary tables for jobs and transfers.
    """
    refreshwindowperiod=5
    deprecate_cache_after=10  #deprecate cache after these number of reads
    tmpdir=tempfile.gettempdir()
    cache_data_file_name = os.path.join(tmpdir, "dailydatasource.b")
    cache_count_file_name = os.path.join(tmpdir, "dailydatasourcecount.b") 

    def apply_delta(self, dateobj):
	returnval = dateobj - datetime.timedelta(self.refreshwindowperiod)
        return returnval

    def get_params(self):
        days = int(int(self.cp.get("Gratia", "days"))+2)
        end = datetime.datetime(*(list(time.gmtime()[:3]) + [0,0,0]))
        start = end - datetime.timedelta(days, 0)
        start -= datetime.timedelta(start.day-1, 0)
        return {'starttime': start, 'endtime': end, 'interval': 'day'}

    def get_json(self):
        assert self.count_results != None
        assert self.hour_results != None
        assert self.transfer_results != None
        assert self.transfer_volume_results != None
        num_jobs = sum(self.count_results)
        total_hours = sum(self.hour_results)
        num_transfers = int(sum(self.transfer_results))
        transfer_volume = float(sum(self.transfer_volume_results))
        return {'jobs_daily': int(num_jobs), 'cpu_hours_daily': \
            float(total_hours), 'transfers_daily': num_transfers,
            'transfer_volume_mb_daily': transfer_volume}

    def query_jobs(self):
        params = self.get_params()

        response = gracc_query_jobs(self.es, jobs_summary_index, **params)

        results = response.aggregations.EndTime.buckets

        all_results = [ (x.Records.value or x.doc_count,
                         x.CoreHours.value,
                         x.key / 1000) for x in results ]

        log.info("GRACC returned %i results for daily jobs" % len(all_results))
        log.debug("Job result dump:")
        for count, hrs, epochtime in all_results:
            time_tuple = time.gmtime(epochtime)
            time_str = time.strftime("%Y-%m-%d %H:%M", time_tuple)
            log.debug("Day %s: Jobs %i, Job Hours %.2f" %
                (time_str, count, hrs))
        count_results = [i[0] for i in all_results]
        hour_results = [i[1] for i in all_results]
        num_results = int(self.cp.get("Gratia", "days"))
        count_results = count_results[-num_results-1:-1]
        hour_results = hour_results[-num_results-1:-1]
        self.count_results, self.hour_results = count_results, hour_results
        return count_results, hour_results

    def query_transfers(self):
        self.connect_transfer()
        cachedresultslist, params=self.getcache()

        log.debug("Received  <%s> cached results"%(len(cachedresultslist)))
        log.debug("Received in query_transfers for DB Query start date: <%s> and end date <%s> "%(params['starttime'],params['endtime']))

        response = gracc_query_transfers(self.es, transfers_summary_index,
                                         **params)

        results = response.aggregations.StartTime.buckets

        all_results = [ (x.key / 1000,
                         x.Records.value,
                         x.Network.value / 1024**2) for x in results ]

        cachedresultslist.extend(all_results)
        all_results=cachedresultslist

        log.info( "-------- Gratia returned %i results for transfers----------------" % len(all_results))
        log.debug("-------- Transfer result dump: DB Fetched results----------------" )
        for i in all_results:
            count, mbs = i[1:]
            log.debug("Day %s: Transfers %i, Transfer PB %.2f" % \
                (i[0], count, mbs/1024**2))
        log.debug("-------- Printing cached and DB Merged results----------------" )
        for i in cachedresultslist:
            count, mbs = i[1:]
            log.debug("Day %s: Transfers %i, Transfer PB %.2f" % \
                (i[0], count, mbs/1024**2))
        count_results = [i[1] for i in all_results]
        hour_results = [i[2] for i in all_results]
        num_results = int(self.cp.get("Gratia", "days"))
        count_results = count_results[-num_results-1:-1]
        hour_results = hour_results[-num_results-1:-1]

        #write the data to cache file
        pickle_f_handle = open(self.cache_data_file_name, "w")
        cPickle.dump(all_results, pickle_f_handle)
        pickle_f_handle.close()

        self.disconnect()
        self.transfer_results, self.transfer_volume_results = count_results, \
            hour_results
        return count_results, hour_results

