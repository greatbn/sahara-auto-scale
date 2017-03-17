# Copyright  2017
"""
Implement Sahara client
"""
from keystoneauth1.identity import v3
from keystoneauth1 import session
from keystoneclient.v3 import client as keystone_client
from saharaclient import client as sahara_client
import os
import sys
topdir = os.path.join(os.path.dirname(__file__), "..")
sys.path.append(topdir)
from common import config
from common import log


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
        self.sess = self.get_session()
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
            for node_group in node_groups:
                for node in node_group['instances']:
                    if instance_id == node['instance_id']:
                        if 'datanode' in node_group['node_processes']:
                            if 'namenode' not in node_group['node_processes']:
                                LOG.info('Found Cluster: '+cluster.id)
                                scale_info = {
                                    "cluster_id": cluster.id,
                                    "name": node_group['name'],
                                    "count": len(node_group['instances'])
                                }
                                return scale_info

                            else:
                                LOG.error("Cannot scale cluster with datanode "
                                          "and namenode process in samehost")
                                sys.exit(1)
                        else:
                            LOG.error("Cannot scale cluster without datanode "
                                      "process")
                            sys.exit(1)
            LOG.error('Cannot find instance: %s in cluster: %s' % (instance_id,
                                                                   cluster.name
                                                                   )
                      )
        LOG.error('Cannot find cluster with instance: %s' % instance_id)


    def scale_cluster(self, operation, scale_info):
        """
        Scale cluster

        operation = 'up' or 'down'
        scale_info = {
            "cluster_id": <cluster_id>,
            "name": <name_node_group>,
            "count": <count instance in group>
        }
        template: scale
        {
            "resize_node_groups": [
                {
                    "count": 2,
                    "name": "old_ng"
                }
            ]

        }
        """
        cluster_id = scale_info['cluster_id']
        cluster_status = self.get_cluster_status(cluster_id=cluster_id)
        LOG.info('Cluster %s with status: %s' % (cluster_id, cluster_status))
        if cluster_status == 'Active':
            if operation == 'up':
                count = int(scale_info['count']) + 1
            else:
                count = int(scale_info['count']) - 1
            scale_template = {
                "resize_node_groups": [
                    {
                        "count": count,
                        "name": scale_info['name']
                    }
                ],
            }
            self.sahara.scale(cluster_id, scale_template)
            LOG.info("Scaling Cluster: %s" % cluster_id)
            return True
        else:
            LOG.error("Cannot ccale cluster %s with status %s" % (cluster_id,
                                                                  cluster_status
                                                                  ))


if __name__ == '__main__':
    instance_id = '34ed1992-ccaa-4354-a82c-3588bb7ba43c'
    scale = SaharaClient()
    cluster_id, node_group_template_id = scale.find_cluster_by_instance(instance_id=instance_id)
    print "Cluster ID: " + cluster_id
    scale.scale_cluster(cluster_id=cluster_id, operation='up')
