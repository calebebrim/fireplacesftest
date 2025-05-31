# Tiltfile for deploying the Fire Incidents project infrastructure
load('Tiltfile.infra', 'install_infra')

def deploy_ingress():

    docker_build(
        'fire-incidents-ingress',
        context='./docker',
        dockerfile='./docker/ingress.dockerfile',
        live_update=[
            sync('./ingress', '/app/ingress'),
            run('pip install -e .'),
        ],
    )


def main():
    # needed to increase the upsert timeout for superset deployment 
    install_infra()

    

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