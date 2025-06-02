# Tiltfile for deploying the Fire Incidents project infrastructure
load('infra.Tiltfile', 'install_infra')

INFRA   = '0-infra'
BRONZE  = '1-bronze'
SILVER  = '2-silver'
GOLD    = '3-gold'

def build_base_image():
        docker_build(
        'base',
        context='.',
        ignore=['mount'],
        dockerfile='./docker/base.dockerfile',
        live_update=[
            # sync('./src', '/app/src'),
            # run('pip install --no-cache-dir -r /app/requirements.txt'),
        ],
    )

def deploy_fireeventsource():

    k8s_yaml('./k8s/bronze-fireeventsource.yaml')
    
    k8s_resource('fire-event-source',
        labels=[BRONZE],
        trigger_mode=TRIGGER_MODE_MANUAL

    )

    k8s_resource(new_name='fire-event-source-storage',
        objects=[ 'fireeventsource-storage', 'fireeventsource-storage-pvc'],
        labels=[BRONZE],
        trigger_mode=TRIGGER_MODE_MANUAL

    )
def deploy_data_quality():

    k8s_yaml('./k8s/silver-dataquality.yaml')
    
    k8s_resource('fire-event-data-quality',
        labels=[SILVER],
        trigger_mode=TRIGGER_MODE_MANUAL

    )

def deploy_data_serving():

    k8s_yaml('./k8s/gold-serving-layer.yaml')
    
    k8s_resource('fire-event-data-serving',
        labels=[GOLD],
        trigger_mode=TRIGGER_MODE_MANUAL
    )



def main():
    # needed to increase the upsert timeout for superset deployment 
    install_infra([INFRA])
    build_base_image()
    deploy_fireeventsource()
    deploy_data_quality()
    deploy_data_serving()
    

    # 4 - deploy the ingestion service (Bronze Layer)
    # 4.1 - create a chronjob to ingest data from file into events topic (bronze layer) 
    # 4.2 - create a chronjob to ingest data from rest api into events topic (bronze layer)
    # 5.1 - from the events topic, create a stream processing service that will evaluate all fire incidents.
    #       for each incident, it will create a new record in the incidents topic (silver layer).
    #       in case of duplicate incidents the service will react acording the environment variable ON_DUPLICATE. 
    #       The valid ON_DUPLICATE values are: "ignore", "update", "fail".


    # n   - The business intelligence team needs to run queries that aggregate these incidents
    #       along the following dimensions: time period, district, and battalion
    # n+1 - create the report using superset.




main()