#!/usr/bin/python
"""
Implement Sahara client
"""

from keystoneauth1.identity import v3
from keystoneauth1 import session
from keystoneclient.v3 import client as keystone_client
from saharaclient import client as sahara_client
from common import config
from common import log
import sys

CONF = config.get_config()
LOG = log.setup_log('scale-sahara')


class SaharaClient(object):

    def __init__(self):
        """
        Init Variables
        """
        self.auth_url = CONF.get('keystoneauth_token', 'auth_url')
        self.username = CONF.get('keystoneauth_token', 'username')
        self.password = CONF.get('keystoneauth_token', 'password')
        self.project_name = CONF.get('keystoneauth_token', 'project_name')
        self.user_domain_name = CONF.get('keystoneauth_token', 'user_domain_name')
        self.project_domain_name = CONF.get('keystoneauth_token', 'project_domain_name')
        self.session = self.get_session()
        self.sahara = self.get_sahara_client()

    def get_session(self):
        """
        Return openstack session
        """
        auth = v3.Password(auth_url=self.auth_url,
                           username=self.username,
                           password=self.password,
                           project_name=self.project_name,
                           user_domain_name=self.user_domain_name,
                           project_domain_name=self.project_domain_name)
        sess = session.Session(auth=auth)
        return sess

    def get_sahara_client(self):
        """
        Return Sahara client
        """
        return sahara_client.Client('1.1', session=self.sess)

    def get_all_clusters(self):
        """
        Return all sahara cluster in Openstack
        """
        return self.sahara.clusters.list()

    def get_cluster_status(self, cluster_id):
        """
        Return status of cluster
        """
        return self.sahara.clusters.get(cluster_id=cluster_id).status

    def get_node_groups(self, cluster_id):
        """
        Return all node in cluster
        """
        return self.sahara.clusters.get(cluster_id=cluster_id).node_groups

    def find_cluster_by_instance(self, instance_id):
        """
        Return cluster id which instance in
        """
        all_clusters = self.get_all_clusters()
        for cluster in all_clusters:
            node_groups = self.get_node_groups(cluster_id=cluster.id)
            # if len(node_groups) == 1:
            #     node_processes = node_groups[0]['node_processes']
            #     if 'namenode' in node_processes and 'datanode' in node_processes:
            #         for node in node_groups[0]['instances']:
            #             if instance_id == node['instance_id']:
            #                 LOG.error('Cannot scale cluster')
            #                 sys.exit(1)
            # else:
            for node_group in node_groups:
                for node in node_group['instances']:
                    if instance_id == node['instance_id']:
                        if 'datanode' in node_group['node_processes']:
                            return cluster.id
                            LOG.info('Found Cluster: '+cluster.id)
                        else:
                            LOG.error('Cannot scale cluster without datanode process')
                            sys.exit(1)


    def scale_cluster(self, cluster_id, operation):
        """
        Scale cluster
        operation = 'up' or 'down'
        """
        return True


if __name__ == '__main__':
    pass
