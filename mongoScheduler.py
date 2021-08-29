try:
  import os
  import json
  import logging
  import random
  import helpers
  from kubernetes import client, config, watch
  from kubernetes.utils import parse_quantity
  from kubernetes.client.rest import ApiException
  from yaml import safe_load
except ImportError as e:
  print(e)
  exit(1)

# Constants
AFFINITY = 0
ANTIAFFINITY = 1

# /
  # Description: function to determine the number of replicas and the PVCs in the statefulSet.
  #
  # Inputs:
  #   stateful_set: The name of the satefulSet
  #   namespace: The name of the Kubernetes namespace
# /
def statefulSetCheck(stateful_set, namespace = "default"):
  replicas = None
  pvcs = None
  ssClient = client.AppsV1Api()
  statefulSets = ssClient.list_namespaced_stateful_set(namespace = namespace).items
  for n in statefulSets:
    if n.metadata.name == stateful_set:
      replicas = n.spec.replicas
      pvcs = n.spec.volume_claim_templates
      break
  return replicas, pvcs

# /
  # Description: function to determine the available nodes in a data centre.
  #
  # Inputs:
  #   apiClient: The object for the Kubernetes API
  #   dataCentre: The name of the data centre of interest, this is a value of a selected label
# /
def nodes_available(dataCentre, apiClient, dataCentresLabel):
  ready_nodes = []
  for n in apiClient.list_node().items:
    #logging.debug("Node: %s" % n)
    for status in n.status.conditions:
      logging.debug("Node Status:\nstatus: %s\ntype: %sdatacentre: %s" % (status.status, status.type, n.metadata.labels[dataCentresLabel]))
      if status.status == "True" and status.type == "Ready" and n.metadata.labels[dataCentresLabel] == dataCentre:
        ready_nodes.append({"hostname": n.metadata.name, "allocatable": n.status.allocatable, "capacity": n.status.capacity, "metadata": n.metadata})
  return ready_nodes

# /
  # Description: function to determine the data centres to use for the pod.
  #
  # Inputs:
  #   podName: The name of the pod
  #   replicas: The number of replicas in the statefulSet
  #   dataCentres: An array of data centre names, this is a value of a selected label
# /
def findDC(podName, replicas, primaryDataCentres, noPrimaryDataCentres):
  increment = podName.split('-')[-1]
  logging.debug("Increment: %s" % increment)
  if int(increment) != (int(replicas) - 1):
    logging.debug("Primary pod")
    dataCentre = primaryDataCentres[int(increment) % (len(primaryDataCentres))]
  else:
    logging.debug("Non-primary  pod")
    dataCentre = random.choice(noPrimaryDataCentres)
  return dataCentre

# /
  # Description: function to get the current deployed pods for the statefulSet and record which nodes they are running on
  #   for affinty/antiaffinity purposes.
  #
  # Inputs:
  #   bindingName: The name of the pod
  #   nodeName: The name of the node to assign to
  #   apiClient: The object for the Kubernetes API
  #   namespace: The name of the Kubernetes namespace
# /
def getCurrentPods(apiClient, searchKey, searchValue, namespace):
  currentPods = []
  for pod in apiClient.list_namespaced_pod(namespace = namespace).items:
    if pod.status.phase == "Running" and searchKey in pod.metadata.labels and pod.metadata.labels[searchKey] == searchValue:
      currentPods.append({"hostname": pod.spec.node_name, "podMetadata": pod.metadata})
  logging.debug("Current Pods: %s" % currentPods)

  return currentPods

def sortPodAffinity(affinityObject, targetPod, pods, nodes, affinityType):
  if affinityType == ANTIAFFINITY:
    selectedNodes = nodes
  else:
    selectedNodes = []
  if affinityObject is not None and affinityObject.required_during_scheduling_ignored_during_execution is not None:
    for requiredRule in affinityObject.required_during_scheduling_ignored_during_execution:
      if requiredRule.label_selector.match_expressions is not None:
        if affinityObject.required_during_scheduling_ignored_during_execution[0].topology_key == 'kubernetes.io/hostname':
          topologyKey = targetPod.spec.node_name
          print("POD HOSTS: %s" % topologyKey)
        else:
          logging.warn("Unknown `toplogyKey`")
          continue
        print(affinityObject.required_during_scheduling_ignored_during_execution[0].label_selector.match_expressions)
        for expressions in affinityObject.required_during_scheduling_ignored_during_execution[0].label_selector.match_expressions:
          print("EXPRESSIONS.key: %s EXPRESSIONS.value: %s" % (expressions.key, expressions.values))
          for pod in pods:
            if expressions.operator == 'In':
              if expressions.key in pod.metadata.labels and pod.metadata.labels[expressions.key] == expressions.values[0]:
                print("FOUND %s %s" % (pod.metadata.name, pod.spec.node_name))
                if affinityType == ANTIAFFINITY:
                  selectedNodes = [node for node in selectedNodes if node.metadata.name != pod.spec.node_name]
                else:
                  selectedNodes.extend(node for node in nodes if node.metadata.name == topologyKey)
              else:
                print("NOT FOUND")
                if affinityType == AFFINITY:
                  selectedNodes = []
                  return selectedNodes
            elif expressions.operator == 'NotIn':
              if expressions.key in pod.metadata.labels and pod.metadata.labels[expressions.key] not in expressions.value:
                if affinityType == ANTIAFFINITY:
                  selectedNodes = [node for node in selectedNodes if node.metadata.name != topologyKey]
                else:
                  selectedNodes.extend(node for node in nodes if node.metadata.name == topologyKey)
              else:
                if affinityType == AFFINITY:
                  selectedNodes = []
                  return selectedNodes
            elif expressions.operator == 'Exists':
              if expressions.key in pod.metadata.labels:
                if affinityType == ANTIAFFINITY:
                  selectedNodes = [node for node in selectedNodes if node.metadata.name != topologyKey]
                else:
                  selectedNodes.extend(node for node in nodes if node.metadata.name == topologyKey)
              else:
                if affinityType == AFFINITY:
                  selectedNodes = []
                  return selectedNodes
            elif expressions.operator == 'DoesNotExist':
              if expressions.key not in pod.metadata.labels:
                if affinityType == ANTIAFFINITY:
                  selectedNodes = [node for node in selectedNodes if node.metadata.name != topologyKey]
                else:
                  selectedNodes.extend(node for node in nodes if node.metadata.name == topologyKey)
              else:
                if affinityType == AFFINITY:
                  selectedNodes = []
                  return selectedNodes
#             elif expressions.operator == 'Gt':
#               for labels in pod.metadata.labels:
#                 if node.metadata[expressions.key] > expressions.value:
#                   if affinityType == 'antiaffinity':
#                     selectedNodes.pop(selectedNodes.index(selectedNodes.index(next(node for node in nodes if node['hostname'] == topologyKey))))
#                   else:
#                     selectedNodes.append(selectedNodes.index(next(node for node in nodes if node['hostname'] == topologyKey)))
#             elif expressions.operator == 'Lt':
#               for labels in pod.metadata.labels:
#                 if node.metadata[expressions.key] < expressions.value:
#                   if affinityType == 'antiaffinity':
#                     selectedNodes.pop(selectedNodes.index(selectedNodes.index(next(node for node in nodes if node['hostname'] == topologyKey))))
#                   else:
#                     selectedNodes.append(selectedNodes.index(next(node for node in nodes if node['hostname'] == topologyKey)))
            else:
              logging.warn("No valid operator for affinifty/anti-affinity")
              continue
          print(selectedNodes)
  return unique(selectedNodes)

def sortNodeAffinity(affinityObject, nodes):
  selectedNodes = []
  if affinityObject.required_during_scheduling_ignored_during_execution is not None:
    for requiredRule in affinityObject.required_during_scheduling_ignored_during_execution:
      if requiredRule.node_selector_terms.match_expressions is not None:
        for expressions in affinityObject.required_during_scheduling_ignored_during_execution[0].node_selector_terms.match_expressions:
          if expressions.operator == 'In':
            print("EXPRESSIONS.key: %s EXPRESSIONS.value: %s" % (expressions.key, expressions.values[0]))
            nodeFound = [node for node in nodes if expressions.key in node.metadata.labels and node.metadata.labels[expressions.key] == expressions.values[0]]
            if nodeFound:
              print("FOUND")
              selectedNodes.extend(nodeFound)
            else:
              print("NOT FOUND")
              selectedNodes = []
              return selectedNodes
          elif expressions.operator == 'NotIn':
            nodeFound = [node for node in nodes if expressions.key in node.metadata.labels and node.metadata.labels[expressions.key] != expressions.values[0]]
            if nodeFound:
              print("FOUND")
              selectedNodes.extend(nodeFound)
            else:
              print("NOT FOUND")
              selectedNodes = []
              return selectedNodes
          elif expressions.operator == 'Exists':
            nodeFound = [node for node in nodes if expressions.key in node.metadata.labels]
            if nodeFound:
              selectedNodes.extend(nodeFound)
            else:
              selectedNodes = []
              return selectedNodes
          elif expressions.operator == 'DoesNotExist':
            nodeFound = [node for node in nodes if expressions.key not in node.metadata.labels]
            if nodeFound:
              selectedNodes.extend(nodeFound)
            else:
              selectedNodes = []
              return selectedNodes
#           elif expressions.operator == 'Gt':
#             for labels in target.metadata.labels:
#               if node.metadata[expressions.key] > expressions.value:
#                 if affinityType == 'antiaffinity':
#                   selectedNodes.pop(selectedNodes.index(selectedNodes.index(next(node for node in nodes if node['hostname'] == topologyKey))))
#                 else:
#                   selectedNodes.append(selectedNodes.index(next(node for node in nodes if node['hostname'] == topologyKey)))
#           elif expressions.operator == 'Lt':
#             for labels in target.metadata.labels:
#               if node.metadata[expressions.key] < expressions.value:
#                 if affinityType == 'antiaffinity':
#                   selectedNodes.pop(selectedNodes.index(selectedNodes.index(next(node for node in nodes if node['hostname'] == topologyKey))))
#                 else:
#                   selectedNodes.append(selectedNodes.index(next(node for node in nodes if node['hostname'] == topologyKey)))
          else:
            logging.warn("No valid operator for affinifty/anti-affinity")
            continue

  return unique(selectedNodes)

def unique(un_unique_list):
  # initialize a null list
  unique_list = []
   
  # traverse for all elements
  for x in un_unique_list:
      # check if exists in unique_list or not
      if x not in unique_list:
          unique_list.append(x)

  return unique_list

def getAffinityNodes(affinityObject, availableNodes, podNodes):
  affinity = {}
  antiaffinity = {}
  if affinityObject.pod_anti_affinity is not None:
    if affinityObject.pod_anti_affinity.required_during_scheduling_ignored_during_execution is not None:
      logging.debug(affinityObject.pod_anti_affinity.required_during_scheduling_ignored_during_execution[0])
    if affinityObject.pod_anti_affinity.preferred_during_scheduling_ignored_during_execution is not None:
      logging.debug(affinityObject.pod_anti_affinity.preferred_during_scheduling_ignored_during_execution[0])
  if affinityObject.pod_affinity is not None:
    if affinityObject.pod_affinity.required_during_scheduling_ignored_during_execution is not None:
      logging.debug(affinityObject.pod_affinity.required_during_scheduling_ignored_during_execution[0])
    if affinityObject.pod_affinity.preferred_during_scheduling_ignored_during_execution is not None:
      logging.debug(affinityObject.pod_affinity.preferred_during_scheduling_ignored_during_execution[0])

  return affinity, antiaffinity

def scoreNodes(availableNodes, requestedCPU, requestedMem):
  # (cpu((capacity-sum(requested))*MaxNodeScore/capacity) + memory((capacity-sum(requested))*MaxNodeScore/capacity))/weightSum

  if requestedCPU is None:
    requestedCPU = 0
  if requestedMem is None:
    requestedMem = 0

  for node in availableNodes:
    cpu = helpers.checkCpuString(node['capacity']['cpu'])
    mem = parse_quantity(node['capacity']['memory'])
    node['score'] = ((cpu - requestedCPU) / cpu) + ((mem - requestedMem) / mem)
    logging.debug("Score for %s: %s" % (node['node'], node['score']))
  
  if node['score'] <= 0:
    availableNodes.pop(availableNodes.index(node))
  
  return sorted(availableNodes, key = lambda k: k['score'], reverse=True)

def getTotalResourcesRequested(containerArray):
  totalCpu = 0
  totalMemories = 0
  for container in containerArray:
    logging.debug("Resources: %s" % container.resources)
    if container.resources.requests is not None:
      if 'cpu' in container.resources.requests:
        totalCpu += helpers.checkCpuString(container.resources.requests['cpu'])
      if 'memory' in container.resources.requests:
        totalMemories += parse_quantity(container.resources.requests['memory'])
  return totalCpu, totalMemories

def getPVs(apiClient, storageClassNames):
  tempPvs = apiClient.list_persistent_volume()
  pvs = []
  logging.debug("All PVs: %s" % tempPvs)
  for pv in tempPvs.items:
    if pv.spec.storage_class_name in storageClassNames and pv.status.phase == 'Available': # or (pv.status.phase == 'Bound' and pv.spec.claim_ref.name == podName)):
      pvs.append(pv)
  return pvs

def getPVCs(apiClient, storageClassNames):
  tempPvcs = apiClient.list_persistent_volume_claim_for_all_namespaces()
  pvcs = []
  logging.debug("All PVs: %s" % tempPvcs)
  for pvc in tempPvcs.items:
    if pvc.spec.storage_class_name in storageClassNames and pvc.status.phase == 'Available': # or (pv.status.phase == 'Bound' and pv.spec.claim_ref.name == podName)):
      pvcs.append(pvc)
  return pvcs

def bindPV(apiClient, bindings):
  for requiredBinding in bindings:
    try:
      logging.debug("Binding %s to %s" ^ (requiredBinding.pvc.name, requiredBinding.pv.name))
      result = apiClient.patch_persistent_volume()
    except ApiException as e:
      logging.error("Failed to bind %s to %s" ^ (requiredBinding.pvc.name, requiredBinding.pv.name)) 
      raise Exception("Failed to bind %s to %s" ^ (requiredBinding.pvc.name, requiredBinding.pv.name))
  return True

def bindPVC(apiClient, bindings):
  for requiredBinding in bindings:
    try:
      logging.debug("Binding %s and %s" ^ (requiredBinding.pv.name, requiredBinding.pvc.name))
      result = apiClient.patch_namespaced_persistent_volume_claim()
    except ApiException as e:
      logging.error("Failed to bind %s and %s" ^ (requiredBinding.pv.name, requiredBinding.pvc.name))
      raise Exception("Failed to bind %s and %s" ^ (requiredBinding.pv.name, requiredBinding.pvc.name))
  return True

# /
  # Description: function to determine if PVC is already bound or if there is a PVC available if unbound
  #
# /
def checkPVAllocatability(pvs, podPVCs, podName):
  pvcArray = []
  for pvc in podPVCs:
    pvPVC = {
      "allocatable": [],
      "unallocatable": [],
      "allocated": []
    }
    pvcBool = False
    if pvc.status.phase == 'Bound':
      logging.debug("PVC %s for pod %s is already bound" % (pvc.spec.storage_class_name + "-" + podName, podName))
      pvPVC['allocated'].append({ 'pod': podName, 'pvc': pvc.spec.storage_class_name + "-" + podName})
      continue
    for pv in pvs:
      # check if this PV already has been not been claimed and the capacity is adquete
      if pv.spec.claim_ref is None and parse_quantity(pv.spec.capacity['storage']) >= parse_quantity(pvc.spec.resources.requests['storage']):
        pvPVC['allocatable'].append({'pv': pv.metadata.name, 'pvc': pvc.metadata.name, 'capacity': pv.spec.capacity['storage']})
        pvcBool = True
    if pvcBool is False:
      pvPVC['unallocatable'].append({'pvc': pvc.metadata.name})
    pvcArray.append({"pvc": pvc.metadata.name, "pv": pvPVC})

  sorted(pvPVC['allocatable'], key = lambda k: k['capacity'], reverse=True)
    
  return pvPVC

# /
  # Description: function to schedule the statefulSet.
  #
  # Inputs:
  #   bindingName: The name of the pod
  #   nodeName: The name of the node to assign to
  #   apiClient: The object for the Kubernetes API
  #   namespace: The name of the Kubernetes namespace
# /
def scheduler(apiClient, bindingName, targetName, namespace):
      
  target=client.V1ObjectReference()
  target.kind = "Node"
  target.apiVersion = "v1"
  target.name = targetName

  body=client.V1Binding(target = target)
  
  meta=client.V1ObjectMeta()
  meta.name = bindingName
  
  body.target = target
  body.metadata = meta
  
  return apiClient.create_namespaced_binding(namespace, body, _preload_content=False)

def main():

  # name of the scheduler
  scheduler_name = os.getenv('SNAME')

  with open('/init/mongoScheduler.yaml', 'r') as f:
    iCfg = safe_load(f)
    f.close()

  # Determine logging level
  if iCfg['logLevel'].upper() == 'DEBUG':
    logLevel = logging.DEBUG
  else:
    logLevel = logging.INFO
  logging.basicConfig(format='{"ts": "%(asctime)s, "f": "%(funcName)s", "l": %(lineno)d, "msg": "%(message)s"}', level=logLevel)

  # Record our configuration settings
  logging.debug("Config: %s" % iCfg)

  # configure the API client
  config.load_incluster_config()
  v1 = client.CoreV1Api()

  # Watch the stream for changes to pods for the namespace
  w = watch.Watch()
  for event in w.stream(v1.list_namespaced_pod, iCfg['namespace']):
    if event['object'].status.phase == "Pending" and event['object'].spec.scheduler_name == scheduler_name and event['object'].status.conditions is None:
      if event['object'].metadata.owner_references[0].kind == 'StatefulSet':
        # record pod name
        pod = event['object'].metadata.name
        logging.debug("Pod: %s" % pod)

        # records the statfulSet name
        ss = event['object'].metadata.owner_references[0].name
        logging.debug("StatefulSet: %s, Pod: %s" % (ss, pod))

        # pod affinity items for pod
        podAffinity = event['object'].spec.affinity
        logging.debug("Affinity: %s" % podAffinity)

        podVol = event['object'].spec.volumes
        logging.debug("Volumes: %s" % podVol)

        requestedCPU, requestedMem = getTotalResourcesRequested(event['object'].spec.containers)
        logging.debug("Requests: cpu: %s, mem: %s" % (requestedCPU, requestedMem))

        # determine how many replicas and PVCs in the statefulSet
        replicas, pvcs = statefulSetCheck(stateful_set = ss, namespace = iCfg['namespace'])
        logging.debug("Number of replicas in statefulSet: %s" % replicas)
        logging.debug("PVCs in statefulSet: %s" % pvcs)

        if pvcs:
          sc = []
          for pvc in pvcs:
            sc.append(pvc.spec.storage_class_name)
          logging.debug("Required storageClasses: %s" % sc)

          pvs = getPVs(apiClient = v1, storageClassNames = sc)
          logging.debug("Selected PVs: %s" % pvs)

          allPvcs = getPVCs(apiClient = v1, storageClassNames = sc)
          logging.debug("Selected PVs: %s" % allPvcs)

          pvMap = checkPVAllocatability(pvs, pvcs, pod)
          logging.debug("PV and PVC mapping: %s" % pvMap)

          for pvc in pvMap:
            logging.debug("PVC to PV map options: %s" % pvc)
          #vol = scheduler(apiClient = v1, bindingName = event['object'].metadata.name, targetName = selectedNode, namespace = iCfg['namespace'])
        else:
          logging.debug("No PVCs required")

        # Get the current pods deployed in the statefulSet and what nodes they are running on.
        currentPods = getCurrentPods(apiClient = v1, searchKey = "app", searchValue = ss, namespace = iCfg['namespace'])
        logging.debug("Current pods deployed for statefulSet: %s" % currentPods)

        # determine which data centre to assign to the pod to
        dataCentreSelected = findDC(podName = pod, replicas = replicas, primaryDataCentres = iCfg['primaryDataCentres'], noPrimaryDataCentres = iCfg['noPrimaryDataCentres'])
        logging.debug("Selected data centre: %s" % dataCentreSelected)

        # Determine the available nodes for the data centre selected.
        nodesAvailable = nodes_available(apiClient = v1, dataCentre = dataCentreSelected, dataCentresLabel = iCfg['dataCentresLabel'])
        logging.debug("Available nodes for data centre %s: %s" % (dataCentreSelected, nodesAvailable))

        # Apply affinity and antiaffinity for available nodes
        affinityNodes, anffinityNodes = getAffinityNodes(podAffinity, nodesAvailable)

        # Calculate score for each available node within the affinity/anti-affinity group
        sortedScoredNodes = scoreNodes(nodesAvailable, requestedCPU, requestedMem)
        logging.debug("Scored available nodes: %s" % sortedScoredNodes)

        if currentPods and len(currentPods) > 0 and nodesAvailable and len(nodesAvailable) > 0:
          for nodeInstance in nodesAvailable:
            if not any(d['hostname'] == nodeInstance['hostname'] for d in currentPods):
              selectedNode = nodeInstance['hostname']
              break
            selectedNode = None
        else:
          selectedNode = random.choice(nodesAvailable)['hostname']

        logging.debug("Selected node: %s" % selectedNode)

        try:
          if selectedNode:
            res = scheduler(apiClient = v1, bindingName = event['object'].metadata.name, targetName = selectedNode, namespace = iCfg['namespace'])
            logging.debug("Bind result: %s" % res)
            logging.info("Pod %s is bound to node %s" % (pod, selectedNode))
          else:
            logging.error("Cannot schedule")
        except client.rest.ApiException as e:
            logging.error(json.loads(e.body)['message'])
      else:
        logging.warn("This scheduler is only for statefulSets")
                    
if __name__ == '__main__':
    main()