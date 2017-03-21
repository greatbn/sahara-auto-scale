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
            print 'Ok'
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
        query_usage = "tags4 = '{0}' AND ".format(query_propery['query_usage'])
        query_threshold = "value {0} {1} AND ".format(operation, threshold)
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
        LOG.debug('Query: '+query)
        result = self.influx.query(query=query)
        LOG.debug('Result: '+str(result.raw))
        if 'series' in result.raw:
            return result.raw['series']
        else:
            return 0

    def get_instances(self):
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
        instances = {
            "instance_exceed_cpu": instance_exceed_cpu,
            "instance_exceed_ram": instance_exceed_ram,
            "instance_below_cpu": instance_below_cpu,
            "instance_below_ram": instance_below_ram

        }
        return instances


if __name__ == '__main__':
    ins = InfluxDBHandler()
    instances = ins.get_instances()
    """
    {
   "instance_exceed_cpu":[
      {
         "values":[
            [
               "2017-03-21T00:30:00Z",
               null
            ],
            [
               "2017-03-21T00:45:00Z",
               4.330415565
            ]
         ],
         "name":"cloud",
         "columns":[
            "time",
            "mean"
         ],
         "tags":{
            "tags1":"2b5c2b1a-e2fc-4545-bfcf-add9b34b77ad"
         }
      },
      {
         "values":[
            [
               "2017-03-21T00:30:00Z",
               5.402141050000002
            ],
            [
               "2017-03-21T00:45:00Z",
               5.946581101666666
            ]
         ],
         "name":"cloud",
         "columns":[
            "time",
            "mean"
         ],
         "tags":{
            "tags1":"465a41b5-8896-4da5-bf7e-3735b26daea4"
         }
      },
      {
         "values":[
            [
               "2017-03-21T00:30:00Z",
               5.792309052180258
            ],
            [
               "2017-03-21T00:45:00Z",
               5.7888406041683345
            ]
         ],
         "name":"cloud",
         "columns":[
            "time",
            "mean"
         ],
         "tags":{
            "tags1":"7a9a1861-23a5-4bb2-8c69-f25bef89aba7"
         }
      },
      {
         "values":[
            [
               "2017-03-21T00:30:00Z",
               null
            ],
            [
               "2017-03-21T00:45:00Z",
               4.168479625
            ]
         ],
         "name":"cloud",
         "columns":[
            "time",
            "mean"
         ],
         "tags":{
            "tags1":"bfba2ffb-2309-4460-83f8-bb4879fee61f"
         }
      }
   ],
   "instance_exceed_ram":[
      {
         "values":[
            [
               "2017-03-21T00:30:00Z",
               487224451.28205127
            ],
            [
               "2017-03-21T00:45:00Z",
               487284736.0
            ]
         ],
         "name":"cloud",
         "columns":[
            "time",
            "mean"
         ],
         "tags":{
            "tags1":"2b5c2b1a-e2fc-4545-bfcf-add9b34b77ad"
         }
      },
      {
         "values":[
            [
               "2017-03-21T00:30:00Z",
               794003823.5897436
            ],
            [
               "2017-03-21T00:45:00Z",
               794144768.0
            ]
         ],
         "name":"cloud",
         "columns":[
            "time",
            "mean"
         ],
         "tags":{
            "tags1":"465a41b5-8896-4da5-bf7e-3735b26daea4"
         }
      },
      {
         "values":[
            [
               "2017-03-21T00:30:00Z",
               388375893.3333333
            ],
            [
               "2017-03-21T00:45:00Z",
               388381354.6666667
            ]
         ],
         "name":"cloud",
         "columns":[
            "time",
            "mean"
         ],
         "tags":{
            "tags1":"7a9a1861-23a5-4bb2-8c69-f25bef89aba7"
         }
      },
      {
         "values":[
            [
               "2017-03-21T00:30:00Z",
               489919409.2307692
            ],
            [
               "2017-03-21T00:45:00Z",
               490009941.3333333
            ]
         ],
         "name":"cloud",
         "columns":[
            "time",
            "mean"
         ],
         "tags":{
            "tags1":"bfba2ffb-2309-4460-83f8-bb4879fee61f"
         }
      }
   ],
   "instance_below_ram":0,
   "instance_below_cpu":[
      {
         "values":[
            [
               "2017-03-21T00:30:00Z",
               1.8897654411538463
            ],
            [
               "2017-03-21T00:45:00Z",
               1.8229943299999998
            ]
         ],
         "name":"cloud",
         "columns":[
            "time",
            "mean"
         ],
         "tags":{
            "tags1":"2b5c2b1a-e2fc-4545-bfcf-add9b34b77ad"
         }
      },
      {
         "values":[
            [
               "2017-03-21T00:30:00Z",
               1.6443865161538462
            ],
            [
               "2017-03-21T00:45:00Z",
               1.601261939
            ]
         ],
         "name":"cloud",
         "columns":[
            "time",
            "mean"
         ],
         "tags":{
            "tags1":"bfba2ffb-2309-4460-83f8-bb4879fee61f"
         }
      }
   ]
}
    """
    import json
    LOG.info('List instances: '+ json.dumps(instances))
