#!/usr/bin/env python
import pika
import json
import time
from common import config
from common import log
from timer import Timer

CONF = config.get_config()
LOG = log.setup_log('sahara-time')


class SaharaNotifyException(Exception):
    pass


class SaharaNotify(object):
    def __init__(self):
        """
        Khởi tạo biến
        """
        self.rabbit_host = CONF.get('rabbitmq', 'host')
        self.rabbit_port = int(CONF.get('rabbitmq', 'port'))
        self.rabbit_userid = CONF.get('rabbitmq', 'userid')
        self.rabbit_password = CONF.get('rabbitmq', 'password')
        self.cluster = []

    def connect(self):
        """
        Kết nối đến rabbitmq
        Tạo 1 channel và trả vè channel
        """
        try:
            credentials = pika.credentials.PlainCredentials(
                username=self.rabbit_userid,
                password=self.rabbit_password)
            connection = pika.BlockingConnection(pika.ConnectionParameters(
                host=self.rabbit_host,
                port=self.rabbit_port,
                credentials=credentials))
            channel = connection.channel()
            return channel
        except Exception as e:
            LOG.error('Cannot connect to RabbitMQ: ' + str(e))

    def init_queue(self, channel, queue_name, exchange):
        """
        Tạo 1 queue và bind vào exchange của sahara
        """
        try:
            channel.queue_declare(queue=queue_name,
                                  exclusive=False)
            LOG.debug('Declare new queues {0} in exchange {1}'.format(
                queue_name,
                exchange))
            channel.queue_bind(exchange=exchange,
                               queue=queue_name,
                               routing_key="#")
            LOG.debug('Bind to queue %s successful' % queue_name)
        except Exception as e:
            LOG.error('Cannot initial your queue: ' + str(e))

    def callback(self, ch, method, parameters, body):
        """
        Khi có một sự kiện gửi tới queue mà đang bind vào thì thực hiện chạy hàm này
        Kiểm tra method của event
            - provision_cluster: event tạo cluster
            - run_edp_job: event khởi chạy 1 job
        Khởi tạo 1 thread Timer() với các tham số: ID = cluster_id | job_id

        """
        start_provision_time = time.time()
        body = eval(body)
        if "method" in body['oslo.message']:
            message = json.loads(body['oslo.message'])
            if message['method'] == 'provision_cluster':
                cluster_id = message['args']['cluster_id']
                t = Timer(_id=cluster_id,
                          start_time=start_provision_time,
                          is_cluster=True)
                t.start()
            elif message['method'] == 'run_edp_job':
                """
                check job with status =	Succeeded
                """
                job_id = message['args']['job_execution_id']
                t = Timer(_id=job_id,
                          start_time=start_provision_time,
                          is_cluster=False)
                t.start()

    def run(self):
        """
        Tạo 1 kênh kết nối tới rabbitmq
        định nghĩa 1 queue: 'sahara.notify' và bind vào exchange 'sahara'
        Và sau đó thực hiện consume
        """
        try:
            channel = self.connect()
            LOG.info('Connected to {0} port {1}'.format(self.rabbit_host,
                                                        self.rabbit_port))
            queue_name = 'sahara.notify'
            exchange = 'sahara'
            self.init_queue(channel=channel,
                            queue_name=queue_name,
                            exchange=exchange)
            LOG.info('')
            channel.basic_consume(self.callback,
                                  queue=queue_name,
                                  no_ack=True)
            channel.start_consuming()
        except Exception as e:
            LOG.error('Something went wrong ' + str(e))


if __name__ == "__main__":
    notify = SaharaNotify()
    notify.run()
