"""
Query to influxdb, get all instance with cpu, ram exceed or below threshold
After that, check the instance in which cluster. Next scale it
"""
from check import sahara_client
from check import influxdb_client


main():
    influx = influxdb_client.InfluxDBHandler()
    instances = influx.get_instances()

        # instances = {
        #     "instance_exceed_cpu": instance_exceed_cpu,
        #     "instance_exceed_ram": instance_exceed_ram,
        #     "instance_below_cpu": instance_below_cpu,
        #     "instance_below_ram": instance_below_ram
        #
        # }
    instance_exceed_cpu = instances['instance_exceed_cpu']
    instance_exceed_ram = instances['instance_exceed_ram']
    instance_below_cpu = instances['instance_below_cpu']
    instance_below_ram = instances['instance_below_ram']
    sahara = sahara_client.SaharaClient()
    if len(instance_exceed_cpu) =! 0:
        for inst in instance_exceed_cpu:
            inst = inst['tags']['tags1']
            scale_info = sahara.find_cluster_by_instance(instance_id=inst)
            if not scale_info:
                sahara.scale_cluster(operation='up', scale_info=scale_info)
    elif len(instance_exceed_ram) =! 0:
        for inst in instance_exceed_ram:
            inst = inst['tags']['tags1']
            scale_info = sahara.find_cluster_by_instance(instance_id=inst)
            if not scale_info:
                sahara.scale_cluster(operation='up', scale_info=scale_info)
    elif len(instance_below_cpu) =! 0:
        for inst in instance_exceed_ram:
            inst = inst['tags']['tags1']
            scale_info = sahara.find_cluster_by_instance(instance_id=inst)
            if not scale_info:
                sahara.scale_cluster(operation='down', scale_info=scale_info)
    elif len(instance_below_ram) =! 0:
        for inst in instance_exceed_ram:
            inst = inst['tags']['tags1']
            scale_info = sahara.find_cluster_by_instance(instance_id=inst)
            if not scale_info:
                sahara.scale_cluster(operation='down', scale_info=scale_info)
