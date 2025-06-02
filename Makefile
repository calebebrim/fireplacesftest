# Makefile for managing a KinD cluster

# Cluster name (can be overridden: `make install CLUSTER_NAME=mycluster`)
CLUSTER_NAME ?= kind-cluster
# KinD config file (optional)
KIND_CONFIG ?= kind-config.yaml

# Install KinD cluster
.PHONY: install
install:
	@echo "Creating KinD cluster named '$(CLUSTER_NAME)'..."
ifdef KIND_CONFIG
	kind create cluster --name $(CLUSTER_NAME) --config $(KIND_CONFIG)
else
	kind create cluster --name $(CLUSTER_NAME)
endif
	@echo "KinD cluster '$(CLUSTER_NAME)' created successfully."

# Uninstall KinD cluster
.PHONY: uninstall
uninstall:
	@echo "Deleting KinD cluster named '$(CLUSTER_NAME)'..."
	kind delete cluster --name $(CLUSTER_NAME)
	@echo "KinD cluster '$(CLUSTER_NAME)' deleted successfully."

# Show usage
.PHONY: help
help:
	@echo "Usage:"
	@echo "  make install [CLUSTER_NAME=<name>] [KIND_CONFIG=<path>]  - Create a KinD cluster"
	@echo "  make uninstall [CLUSTER_NAME=<name>]                     - Delete the KinD cluster"
	@echo "  make help                                                - Show this help"

imagerepository:
	@echo "KinD cluster management Makefile"
	@echo "Available targets:"
	@echo "  install    - Create a KinD cluster with the specified name and config"
	@echo "  uninstall  - Delete the KinD cluster with the specified name"
	@echo "  help       - Show this help message"
	docker run -d -p 5000:5000 --restart=always --name registry registry:2
stoprepo: 
	docker stop registry && docker rm registry

quickstart: imagerepository
	make install CLUSTER_NAME=fireplace KIND_CONFIG=kind-config.yaml

shutdown: 
	-make uninstall CLUSTER_NAME=fireplace KIND_CONFIG=kind-config.yaml
	-make stoprepo

build_base:
	docker build --pull --rm -f 'docker/base.dockerfile' -t 'base:latest' '.' 