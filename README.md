# KinD Cluster Setup Instructions

This document provides step-by-step instructions to **install** and **uninstall** a KinD (Kubernetes in Docker) cluster using the included `Makefile`.

---

## 🛠️ Prerequisites

✅ Docker installed and running
✅ `kind` CLI installed ([https://kind.sigs.k8s.io/docs/user/quick-start/#installation](https://kind.sigs.k8s.io/docs/user/quick-start/#installation))
✅ `make` utility installed

---

## 📁 Files

* **Makefile**
  Contains commands to install and uninstall the KinD cluster.

* **Optional KinD Config file (`kind-config.yaml`)**
  If you want to customize the cluster (e.g., node count, port mappings), create this file.

---

## 🚀 Usage

### 1️⃣ Create a KinD cluster

By default, the cluster will be created with the name `kind-cluster`.

```bash
make install
```

#### ⚙️ Customizing the cluster name

Specify the `CLUSTER_NAME` variable to choose a custom name:

```bash
make install CLUSTER_NAME=my-cluster
```

#### ⚙️ Using a custom KinD configuration file

Provide a KinD configuration file with the `KIND_CONFIG` variable:

```bash
make install KIND_CONFIG=custom-kind.yaml
```

This allows you to define:

* Number of nodes
* Node roles (control-plane, worker)
* Port mappings
* etc.

---

### 2️⃣ Delete (uninstall) a KinD cluster

Delete the default cluster:

```bash
make uninstall
```

Delete a cluster with a specific name:

```bash
make uninstall CLUSTER_NAME=my-cluster
```

---

### 3️⃣ Helpful commands

To list all clusters:

```bash
kind get clusters
```

To check the status of your cluster:

```bash
kubectl cluster-info --context kind-<CLUSTER_NAME>
```

---

## 📄 Example KinD Config File

Here’s a basic example for `kind-config.yaml`:

```yaml
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
nodes:
  - role: control-plane
  - role: worker
  - role: worker
```

---

## ℹ️ Additional Notes

* The `Makefile` has a `help` target to show usage:

```bash
make help
```

* Ensure Docker has enough resources (CPU, memory) allocated for your cluster.

* If you plan to use Ingress controllers or LoadBalancer features, consider additional configurations (e.g., using `kind` port mappings).

---

🎉 **That’s it!** You’re ready to spin up and tear down KinD clusters using `make`!

If you have any questions or want to extend this setup (e.g., auto-load images, install tools), let me know! 🚀

---

## ⚡ Quickstart

1. **Clone the repository** (if you haven't already):

    ```bash
    git clone <repo-url>
    cd <repo-directory>
    ```

2. **Run**:

    ```bash
    make quickstart
    ```

3. **Verify the cluster is running**:

    ```bash
    kind get clusters
    kubectl cluster-info --context fireplace
    ```

4. **Stop everything**:

    ```bash
    make shutdown
    ```

