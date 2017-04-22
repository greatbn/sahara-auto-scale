#!/usr/bin/env python
from common import config
from common import log
from check import influxdb_client
from check import sahara_client
import threading
import time
CONF = config.get_config()
LOG = log.setup_log('sahara-time')


class Timer(threading.Thread):
    def __init__(self, _id, start_time, is_cluster):
        threading.Thread.__init__(self)
        self.id = _id
        self.start_time = start_time
        self.is_cluster = is_cluster
        self.sahara = sahara_client.SaharaClient()

    def run(self):
        while True:
            if self.is_cluster:
                status = self.sahara.get_cluster_status(cluster_id=self.id)
                LOG.debug('Cluster {0} with {1} status'.format(self.id,
                                                               status))
                if status == 'Active':
                    total_time = time.time() - self.start_time
                    cluster_name = self.sahara.get_cluster_name(self.id)
                    metrics = '{0}.{1}'.format(self.id, cluster_name)
                    self.influx = influxdb_client.InfluxDBHandler()
                    self.influx.send_metric(metrics=metrics,
                                            value=total_time,
                                            path='clusterTime')
                    LOG.info('Cluster: {0} Total Time: {1}'.format(self.id,
                                                                   total_time))
                    break
            else:
                job = self.sahara.get_job(job_id=self.id)
                # import ipdb;ipdb.set_trace()
                status = job.info['status']
                LOG.debug('Job {0} with {1} status'.format(self.id,
                                                           status))
                if status == 'SUCCEEDED':
                    import datetime
                    start_time = datetime.datetime.strptime(job.start_time,
                                                            "%Y-%m-%dT%H:%M:%S")
                    end_time = datetime.datetime.strptime(job.end_time,
                                                          "%Y-%m-%dT%H:%M:%S")
                    total_time = end_time - start_time
                    total_time = total_time.seconds
                    metrics = '{0}'.format(self.id)
                    self.influx = influxdb_client.InfluxDBHandler()
                    self.influx.send_metric(metrics=metrics,
                                            value=total_time,
                                            path='jobTime')
                    LOG.info('Job: {0} Total time: {1}'.format(self.id,
                                                               total_time))
                    break
