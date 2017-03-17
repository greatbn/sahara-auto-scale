#!/usr/bin/python
"""Query in influxdb and generate gdnsd config file"""

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
        self.cpu_threshold_up = CONF.get('DEFAULT', 'max_cpu_percent')
        self.ram_threshold_up = CONF.get('DEFAULT', 'max_ram_percent')
        self.cpu_threshold_down = CONF.get('DEFAULT', 'min_cpu_percent')
        self.ram_threshold_down = CONF.get('DEFAULT', 'min_ram_percent')
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
        except Exception as e:
            LOG.error("Cannot connect to Influxdb: "+str(e))
            return

    def query(self, operation, threshold, query_propery):
        """
        Build query string
        """
        query = "{0} {1} {2} {3} {4} {5}"
        query_prefix = "SELECT mean(value) FROM %s WHERE" % self.measurement
        query_resource = "tags3 = '{0}' AND".format(query_propery['query_resource'])
        query_usage = "tags4 = '{0}' AND".format(query_propery['query_usage'])
        query_threshold = "AND value {0} {1}".format(operation, threshold)
        query_time_range = "time > now() - {0}".format(self.time_range)
        query_subfix = "GROUP BY time({0}), tags1".format(self.time_range)
        return query.format(query_prefix, query_resource,
                            query_usage, query_threshold,
                            query_time_range, query_subfix)

    def perform_query(self, operation, threshold, query_propery):
        """
        Return metrics
        """
        query = self.query(operation=operation,
                           threshold=threshold,
                           query_propery=query_propery)
        result = self.influx.query(query=query)
        return result.raw['series']

    def get_instance(self):
        """
        Get instance exceed threshold RAM, CPU
        example:
        cpu_threshold = 90 (percent)
        ram_threshold = 80 (percent)
        """
        cpu_propery = {
            'query_resource': 'cpu',
            'query_usage': 'total'
        }
        ram_property = {
            'query_resource': 'memory',
            'query_usage': 'usage'
        }
        instance_exceed_cpu = self.perform_query(operation='>',
                                                 threshold=self.cpu_threshold_up,
                                                 query_propery=cpu_propery
                                                 )
        instance_below_cpu = self.perform_query(operation='<',
                                                threshold=self.cpu_threshold_down,
                                                query_propery=cpu_propery
                                                )
        instance_exceed_ram = self.perform_query(operation='>',
                                                 threshold=self.ram_threshold_up,
                                                 query_propery=ram_property
                                                 )
        instance_below_ram = self.perform_query(operation='<',
                                                threshold=self.ram_threshold_down,
                                                query_propery=ram_property
                                                )
        return instance_exceed_cpu, instance_exceed_ram,
        instance_below_cpu, instance_below_ram


if __name__ == '__main__':
    pass
