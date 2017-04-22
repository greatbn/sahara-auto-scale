from keystoneauth1.identity import v3
from keystoneauth1 import session
from keystoneclient.v3 import client as keystone_client
from saharaclient import client as sahara_client
auth = v3.Password(auth_url='http://192.168.51.5000/v3', username='admin', password='vccloud123', project_name='admin', user_domain_name='default', project_domain_name='default')
sess = session.Session(auth=auth)
sahara_client.Client('1.1', session=sess)
auth = v3.Password(auth_url='http://192.168.51.71:5000/v3', username='admin', password='vccloud123', project_name='admin', user_domain_name='default', project_domain_name='default')
sess = session.Session(auth=auth)
sahara_client.Client('1.1', session=sess)
sc = sahara_client.Client('1.1', session=sess)
