"""
Query to influxdb, get all instance with cpu, ram exceed or below threshold
After that, check the instance in which cluster. Next scale it
"""
from check import sahara_client
from check import influxdb_client


class AutoScale(object):

    def __init__(self):
        self.influx = influxdb_client.InfluxDBHandler()
        self.sahara = sahara_client.SaharaClient()

    def main(self):
        """
        Get all instances in all clusters which has is_autoscale = True
        """
        clusters = self.sahara.get_all_clusters()

        for cluster in clusters:
            cluster = cluster.to_dict()
            if cluster['is_autoscale']:
                for node_group in cluster['node_groups']:
                    if 'datanode' in node_group['node_processes']:
                        if 'namenode' not in node_group['node_processes']:
                            max_cpu = cluster['max_cpu']
                            max_ram = cluster['max_ram']
                            min_cpu = cluster['min_cpu']
                            min_ram = cluster['min_ram']
                            total_cpu_used = 0
                            total_ram_used = 0
                            for inst in node_group['instances']:
                                instance_id = inst['instance_id']
                                result = self.influx.check_instance(instance_id)
                                total_cpu_used += float(
                                    result['cpu'][0]['values'][0][1])
                                total_ram_used += float(
                                    result['ram'][0]['values'][0][1])
                            num_inst = len(node_group['instances'])
                            cpu_avg = float(total_cpu_used/num_inst)
                            ram_avg = float(total_ram_used/num_inst)
                            scale_info = {
                                "cluster_id": cluster['id'],
                                "name": node_group['name'],
                                "count": num_inst
                            }
                            if cpu_avg > max_cpu or ram_avg > max_ram:
                                self.sahara.scale_cluster(operation='up',
                                                          scale_info=scale_info)
                            if cpu_avg < min_cpu and ram_avg < min_ram:
                                self.sahara.scale_cluster(operation='down',
                                                          scale_info=scale_info)


if __name__ == "__main__":
    auto = AutoScale()
    auto.main()
