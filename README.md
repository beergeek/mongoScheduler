# mongoScheduler - Work in Progress

## Introduction

Many customers have two data centres but often struggle to get a third data centre that they classify "worthy" enough to run a primary of a MongoDB replcia set or shard. In this case we (Consulting Engineering) recommend the customer uses a secondary with `priority: 0` so that secondary never becomes a primary. I hear you ask "why not use an arbiter".....well that is easy to answer: they are evil. When using arbiters you either have high availability or data durability, but not both. You cannot use a write concern of "majority" with an arbiter in play. So having a secondary with `priority: 0` is the way to go. We want to set a particular pod to always be the secondary that can never be voed as primary, let's say the last pod. In this case we want all other pods to be spread evenly over the two data centres where primaries can reside and have the non-electable secondary in the third data centre.

The idea would be to have the following situation:

![Desired Toplogy](https://github.com/beergeek/mongoScheduler/blob/main/images/mongoSchedulerFixed.png "Desired Toplogy Spread")

With the normal Kubernetes Scheduler the pods would placed in any data centre in any order to satsify the toplogy spread (if set) and affinity and antiaffinity, but not in a preditable fashion where you know pod 0 would be in data centre 0 and pod 1 would be in data centre 1 etc. The following sitation could be realised:

![Undesired Topology](https://github.com/beergeek/mongoScheduler/blob/main/images/k8sScheduler.png "Undesired Topology Spread")

Currently there is not way for a Kubernetes statefulSet to set one pod to be in a particular place or on a particlar node because all members of the statefulSet have the same attributes. Therefore we would need to change the configuration of the replica set or shard whenever the deployment was created or pods restarted to ensure only the pod in the third data centre would be non-electable: e.g. a pain.

This custom Kubernetes Scheduler predictively schedules pods onto worker nodes in certains data centres by splitting the number of members of the replica set/shard minus on pod and then using the modulus of the available data centres where electable-members can be placed. The scheduler always places the last pod (non-electable) onto designated worker nodes in a selected data centre where non-electable pods should reside. Antiaffinity and affinity are respected (within limitations) for both pod and node.

This scheduler only works for statefulSets.

## Setup

### Scheduler

This repo includes Helm charts to install the custom scheduler. I recommend using Helfmile to install the required resources all at once. By modifying the `value.yaml` file under your selected environment, e.g. `charts/values/production/values.yaml` for the `production` envioronment, you can manage different schedulers for different environments by just changing the name of the environment directory (`charts/values/<ENV>`). The value for `<ENV>` can be any name that Kubernetes can use.

[Helm](https://helm.sh/docs/intro/install/) is required to be installed and [Helmfile](https://github.com/roboll/helmfile) is also highly recommended. If Helmfile is used you will also need [Helm-Diff](https://github.com/databus23/helm-diff).

Available attributes in the `values.yaml` file:
|Key|Description|
|----------|------------------------------------|
|imageDetail.name|Address of the Docker image to use for the scheduler. The provided image is `ghcr.io/beergeek/mongoscheduler`.|
|imageDetail.version|Version of the Docker image to use.|
|imageDetail.pullPolicy|The pull policy for the Docker image. Can be `IfNotPresent`, `Always`, or `Never`.|
|config.namespace|The Kubernetes namespace where the scheduler will be deployed and operate.|
|config.logLevel|The log level for the schduler logs. Can eb `DEBUG` or `INFO`.|
|config.dataCentresLabel|The Kubernetes worker node label used to identify which data centre a worker node belongs to|
|config.primaryDataCentres|An array of data centres where electable members can reside. These will be the values of the select label to identify the worker names (`config.dataCentresLabel`).|
|config.noPrimaryDataCentres|An array of data centres where non-electable memebrs will reside. These will be the values of the select label to identify the worker names (`config.dataCentresLabel`).|

The name of the schduler deployed by default is `mongo-scheduler-<ENV>`, the actual pod will have a random string at the end of the name. The `<ENV>` is the value specified above for the environment and will be used as an environment variable when deploying via Helmfile.

To deploy the schduler with Helmfile use the following:

```shell
ENV=<ENV> NS=<NAMESPACE> KUBECONFIG=$PWD/<KUBECONFIG_FILE> helmfile apply
```

Where `<ENV>` is specified as above, `<NAMESPACE>` is the Kuberntes environment to deploy into and manage scheduling (if configured in statefulSets), and the `<KUBECONFIG_FILE>` is the config file to gain access to the Kubernetes cluster.

To remove the scheduler for a namespace and environment via Helmfile use:

```shell
ENV=<ENV> NS=<NAMESPACE> KUBECONFIG=$PWD/<KUBECONFIG_FILE> helmfile destroy
```

### statefulSets

To use this with the MongoDB Kubernetes Operator the `schedulerName` attribute must be set for the deployment resource:

```shell
---
apiVersion: mongodb.com/v1
kind: MongoDB
metadata:
  name: my-replica-set
spec:
  members: 3
  version: 4.4.8-ent
  service: my-service

  opsManager:
    configMapRef:
      name: my-project
  credentials: my-credentials
  type: ReplicaSet

  persistent: true
  podSpec:
    podTemplate:
      spec:
        schedulerName: mongo-scheduler-prod
        containers:
          - name: mongodb-enterprise-database
            resources:
              limits:
                memory: 512M
        affinity:
          podAntiAffinity:
            requiredDuringSchedulingIgnoredDuringExecution:
              - labelSelector:
                  matchExpressions:
                    - key: "app"
                      operator: In
                      values:
                      - my-replica-set-svc
```

The key of interest is `spec.podSpec.podTemplate.spec.schedulerName` and is the name of the scheduler deployed as described above.

## Limitations

* No `preferred` affinity or antiaffinity as yet
* No `Gt` or `Lt` for affinity or antoaffinity as yet
* No dynamic provisioning of PVs as yet
* 