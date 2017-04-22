#!/usr/bin/python
"""Query in influxdb and generate gdnsd config file"""

import os
import sys
topdir = os.path.join(os.path.dirname(__file__), "..")
sys.path.append(topdir)
from influxdb import InfluxDBClient
from common import config
from common import log

LOG = log.setup_log('scale-sahara')
CONF = config.get_config()


class InfluxDBHandler(object):
    """
    Connect to influxdb and query to get metrics
    """

    def __init__(self):
        """
        Init variables
        """
        self.host = CONF.get('influxdb', 'host')
        self.port = CONF.get('influxdb', 'port')
        self.username = CONF.get('influxdb', 'username')
        self.password = CONF.get('influxdb', 'password')
        self.database = CONF.get('influxdb', 'database')
        self.time_range = CONF.get('DEFAULT', 'time_range')
        self.measurement = CONF.get('DEFAULT', 'measurement')
        self.get_influxdb_client()

    def get_influxdb_client(self):
        """
        Return influxdb client
        """
        try:
            self.influx = InfluxDBClient(host=self.host, port=self.port,
                                         username=self.username,
                                         password=self.password,
                                         database=self.database)
            print 'Ok'
        except Exception as e:
            LOG.error("Cannot connect to Influxdb: "+str(e))
            return

    def query(self, instance, query_propery):
        """
        Build query string
        """
        query = "{0} {1} {2} {3} {4}"
        query_prefix = "SELECT mean(value) FROM %s WHERE " % self.measurement
        query_instance = "tags1 = '{0}' AND".format(instance)
        query_resource = "tags3 = '{0}' AND".format(query_propery['query_resource'])
        query_usage = "tags4 = '{0}' AND ".format(query_propery['query_usage'])
        query_time_range = "time > now() - {0}".format(self.time_range)
        return query.format(query_prefix, query_instance,
                            query_resource, query_usage,
                            query_time_range)

    def perform_query(self, instance, query_propery):
        """
        Return metrics
        """
        query = self.query(instance=instance,
                           query_propery=query_propery)
        LOG.debug('Query: '+query)
        result = self.influx.query(query=query)
        LOG.debug('Result: '+str(result.raw))
        if 'series' in result.raw:
            return result.raw['series']
        else:
            return 0

    def check_instance(self, instance):
        """
        check cpu, ram of the instance
        """
        cpu_propery = {
            'query_resource': 'cpu',
            'query_usage': 'total'
        }
        ram_property = {
            'query_resource': 'memory',
            'query_usage': 'used_percent'
        }
        result_cpu = self.perform_query(instance=instance,
                                        query_propery=cpu_propery)
        result_ram = self.perform_query(instance=instance,
                                        query_propery=ram_property)
        result = {
            "cpu": result_cpu,
            "ram": result_ram
        }
        return result

    def send_metric(self, metrics, path, value):
        """
        Send metric to database
        """
        metrics = metrics.split(".")

        _metrics = {}
        # build metric
        for i in range(len(metrics)):
            _metrics['tags'+str(i)] = metrics[i]
        metrics = []
        metrics.append({
            "measurement": path,
            "tags": _metrics,
            "fields": {
                "value": float(value)
            }
        })
        self.influx.write_points(metrics)


if __name__ == '__main__':
    ins = InfluxDBHandler()
    instance = '0e23fc8e-dc44-4e2c-a0fd-aed2a1e8e09f'
    cpu_threshold = 80
    ram_threshold = 80
    result = ins.check_instance(instance=instance,
                                cpu_threshold=cpu_threshold,
                                ram_threshold=ram_threshold)
    LOG.info('Scalable: '+str(result))
