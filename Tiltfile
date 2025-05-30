# 1 - install database
def install_postgres():
    k8s_yaml('k8s/postgres.yaml')
    k8s_resource('postgres', port_forwards=5432)

def main():
    


    install_postgres()
    # 1 - install kafka
    # 2 - install s3 repo
    # 3 - create a bucket once s3 repo is installed
    # 4 - deploy the ingestion service
