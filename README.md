# mongoScheduler - Work in Progress

## Introduction

Many customers have two data centres but often struggle to get a third data centre that they classify "worthy" enough to run a primary of a MongoDB replcia set or shard. In this case we (Consulting Engineering) recommend the customer uses a secondary with `priority: 0` so that secondary never becomes a primary. I hear you ask "why not use an arbiter".....well that is easy to answer: they are evil. When using arbiters you either have high availability or data durability, but not both. You cannot use a write concern of "majority" with an arbiter in play. So having a secondary with `priority: 0` is the way to go. We want to set a particular pod to always be the secondary that can never be voed as primary, let's say the last pod. In this case we want all other pods to be spread evenly over the two data centres where primaries can reside and have the non-electable secondary in the third data centre.

The idea would be to have the following situation:

![Desired Toplogy](https://github.com/beergeek/mongoScheduler/blob/main/images/mongoSchedulerFixed.png "Desired Toplogy Spread")

With the normal Kubernetes Scheduler the pods would placed in any data centre in any order to satsify the toplogy spread (if set) and affinity and antiaffinity, but not in a preditable fashion where you know pod 0 would be in data centre 0 and pod 1 would be in data centre 1 etc. The following sitation could be realised:

![Undesired Topology](https://github.com/beergeek/mongoScheduler/blob/main/images/k8sScheduler.png "Undesired Topology Spread")

Currently there is not way for a Kubernetes statefulSet to set one pod to be in a particular place or on a particlar node because all members of the statefulSet have the same attributes. Therefore we would need to change the configuration of the replica set or shard whenever the deployment was created or pods restarted to ensure only the pod in the third data centre would be non-electable: e.g. a pain.

This custom Kubernetes Scheduler predictively schedules pods onto worker nodes in certains data centres by splitting the number of members of the replica set/shard minus on pod and then using the modulus of the available data centres where electable-members can be placed. The scheduler always places the last pod (non-electable) onto designated worker nodes in a selected data centre where non-electable pods should reside. Antiaffinity and affinity are respected (within limitations) for both pod and node.

## Setup


## Limitations

* No `preferred` affinity or antiaffinity as yet
* No `Gt` or `Lt` for affinity or antoaffinity as yet
* No dynamic provisioning of PVs as yet