try:
  import os
  import json
  import logging
  import random
  import helpers
  import copy
  from kubernetes import client, config, watch
  from kubernetes.utils import parse_quantity
  from kubernetes.client.rest import ApiException
  from kubernetes.client.models.v1_persistent_volume_spec import V1PersistentVolumeSpec
  from kubernetes.client.models.v1_persistent_volume_claim_spec import V1PersistentVolumeClaimSpec
  from yaml import safe_load
except ImportError as e:
  print(e)
  exit(1)

# Constants
AFFINITY = 0
ANTIAFFINITY = 1
AVAILABLE = "Available"
BOUND = "Bound"
DOESNOTEXIST = "DoesNotExist"
EXISTS = "Exists"
IN = "In"
NOTIN = "NotIn"
PENDING = "Pending"

# /
  # Description: function to determine the number of replicas and the PVCs in the statefulSet.
  #
  # Inputs:
  #   stateful_set: The name of the satefulSet
  #   namespace: The name of the Kubernetes namespace
# /
def statefulSetCheck(stateful_set, namespace):
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
  nodes = apiClient.list_node()
  for node in nodes.items:
    #logging.debug("Node: %s" % n)
    for status in node.status.conditions:
      logging.debug("Node Status:\nstatus: %s\ntype: %sdatacentre: %s" % (status.status, status.type, node.metadata.labels[dataCentresLabel]))
      if status.status != "True" and status.type != "Ready" and node.metadata.labels[dataCentresLabel] != dataCentre:
        nodes.items.pop(nodes.items.index(node))
        break
        #ready_nodes.items.append({"hostname": node.metadata.name, "allocatable": node.status.allocatable, "capacity": node.status.capacity, "metadata": node.metadata})
  return nodes

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
def getSSPods(pods, searchKey, searchValue):
  ssPods = []
  for pod in pods.items:
    if pod.status.phase == "Running" and searchKey in pod.metadata.labels and pod.metadata.labels[searchKey] == searchValue:
      ssPods.append({"hostname": pod.spec.node_name, "podMetadata": pod.metadata})
  logging.debug("Current Pods: %s" % ssPods)

  return ssPods


# /
  # Description: function to get the current deployed pods and record which nodes they are running on
  #   for affinty/antiaffinity purposes.
  #
  # Inputs:
  #   bindingName: The name of the pod
  #   nodeName: The name of the node to assign to
  #   pods: podList of pods
  #   namespace: The name of the Kubernetes namespace
# /
def getAllPods(apiClient, namespace):
  allPods = apiClient.list_namespaced_pod(namespace = namespace)
  for pod in allPods.items:
    if pod.status.phase != "Running":
      allPods.items.pop(allPods.items.index(pod))
  logging.debug("All Pods: %s" % allPods)

  return allPods

def sortPodAffinity(affinityObject, targetPod, pods, node, topologyKey, affinityType):
  logging.debug("Starting affinity/antiaffinity for %s" % targetPod)
  suitableNode = False
  logging.debug(affinityObject.label_selector.match_expressions)
  for expressions in affinityObject.label_selector.match_expressions:
    logging.debug("EXPRESSIONS.key: %s EXPRESSIONS.operator: %s EXPRESSIONS.value: %s" % (expressions.key, expressions.operator, expressions.values))
    if expressions.operator == IN:
      logging.debug("IN")
      for pod in pods.items:
        logging.debug("LABELS: %s" % pod.metadata.labels)
        if expressions.key in pod.metadata.labels and pod.metadata.labels[expressions.key] in expressions.values:
          logging.debug("Key found: %s on %s" % (expressions.key. pod.metadata.name))
          if node.metadata[topologyKey] == pod.spec.node_name:
            if affinityType == ANTIAFFINITY:
              return False
            else:
              suitableNode = True
          else:
            if affinityType == ANTIAFFINITY:
              suitableNode = True
            else:
              return False
        else:
          logging.debug("Key NOT found: %s on %s" % (expressions.key, pod.metadata.name))
          if affinityType == AFFINITY:
            return False
          else:
            suitableNode = True
    elif expressions.operator == NOTIN:
      for pod in pods.items:
        if expressions.key in pod.metadata.labels and pod.metadata.labels[expressions.key] in expressions.value:
          if node.metadata[topologyKey] != pod.spec.node_name:
            if affinityType == ANTIAFFINITY:
              return False
            else:
              suitableNode = True
          else:
            if affinityType == ANTIAFFINITY:
              suitableNode = True
            else:
              return False
        else:
          if affinityType == AFFINITY:
            return False
          else:
            suitableNode = True
    #elif expressions.operator == EXISTS:
    #  for pod in pods:
    #    if expressions.key in pod.metadata.labels:
    #      if node.metadata[topologyKey] == pod.spec.node_name:
    #        if affinityType == ANTIAFFINITY:
    #          return False
    #        else:
    #          selectedNodes = True
    #    else:
    #      if affinityType == AFFINITY:
    #        selectedNodes = []
    #        return selectedNodes
    #elif expressions.operator == DOESNOTEXIST:
    #  if expressions.key not in pod.metadata.labels:
    #    if affinityType == ANTIAFFINITY:
    #      selectedNodes = [node for node in selectedNodes if node.metadata.name != topologyKey]
    #    else:
    #      selectedNodes.extend(node for node in nodes if node.metadata.name == topologyKey)
    #  else:
    #    if affinityType == AFFINITY:
    #      selectedNodes = []
    #      return selectedNodes
#         elif expressions.operator == 'Gt':
#           for labels in pod.metadata.labels:
#             if node.metadata[expressions.key] > expressions.value:
#               if affinityType == 'antiaffinity':
#                 selectedNodes.pop(selectedNodes.index(selectedNodes.index(next(node for node in nodes if node['hostname'] == topologyKey))))
#               else:
#                 selectedNodes.append(selectedNodes.index(next(node for node in nodes if node['hostname'] == topologyKey)))
#         elif expressions.operator == 'Lt':
#           for labels in pod.metadata.labels:
#             if node.metadata[expressions.key] < expressions.value:
#               if affinityType == 'antiaffinity':
#                 selectedNodes.pop(selectedNodes.index(selectedNodes.index(next(node for node in nodes if node['hostname'] == topologyKey))))
#               else:
#                 selectedNodes.append(selectedNodes.index(next(node for node in nodes if node['hostname'] == topologyKey)))
    else:
      logging.warn("No valid operator for affinifty/anti-affinity")
      return False
  return suitableNode

def sortNodeAffinity(affinityObject, nodes):
  selectedNodes = []
  if affinityObject.required_during_scheduling_ignored_during_execution is not None:
    for requiredRule in affinityObject.required_during_scheduling_ignored_during_execution:
      if requiredRule.node_selector_terms.match_expressions is not None:
        for expressions in requiredRule.node_selector_terms.match_expressions:
          if expressions.operator == IN:
            logging.debug("EXPRESSIONS.key: %s EXPRESSIONS.value: %s" % (expressions.key, expressions.values[0]))
            nodeFound = [node for node in nodes if expressions.key in node.metadata.labels and node.metadata.labels[expressions.key] in expressions.values]
            if nodeFound:
              selectedNodes.extend(nodeFound)
            else:
              selectedNodes = []
              return selectedNodes
          elif expressions.operator == NOTIN:
            nodeFound = [node for node in nodes if expressions.key in node.metadata.labels and node.metadata.labels[expressions.key] not in expressions.values]
            if nodeFound:
              selectedNodes.extend(nodeFound)
            else:
              selectedNodes = []
              return selectedNodes
          elif expressions.operator == EXISTS:
            nodeFound = [node for node in nodes if expressions.key in node.metadata.labels]
            if nodeFound:
              selectedNodes.extend(nodeFound)
            else:
              selectedNodes = []
              return selectedNodes
          elif expressions.operator == DOESNOTEXIST:
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

def getAffinityNodes(affinityObject, availableNodes, pods, pod):
  if affinityObject.pod_anti_affinity is not None:
    if affinityObject.pod_anti_affinity.required_during_scheduling_ignored_during_execution is not None:
      logging.debug(affinityObject.pod_anti_affinity.required_during_scheduling_ignored_during_execution[0])
      for node in availableNodes.items:
        for requiredRule in affinityObject.pod_anti_affinity.required_during_scheduling_ignored_during_execution:
          logging.debug("REQUIRED RULE: %s" % requiredRule)
          if requiredRule.label_selector.match_expressions is not None:
            if requiredRule.topology_key == 'kubernetes.io/hostname':
              topologyKey = node.metadata.labels['kubernetes.io/hostname']
              logging.debug("POD HOSTS: %s" % topologyKey)
            else:
              logging.warn("Unknown `toplogyKey`")
              continue
            suitableNode = sortPodAffinity(requiredRule, pod, pods, node, topologyKey, ANTIAFFINITY)
            if suitableNode is True:
              logging.debug("Node %s is SUITABLE" % node.metadata.labels[requiredRule.topology_key])
            else:
              logging.debug("Node %s is NOT SUITABLE" % node.metadata.labels[requiredRule.topology_key])
              availableNodes.items.pop(availableNodes.items.index(node))
          
    if affinityObject.pod_anti_affinity.preferred_during_scheduling_ignored_during_execution is not None:
      logging.debug(affinityObject.pod_anti_affinity.preferred_during_scheduling_ignored_during_execution[0])
  if affinityObject.pod_affinity is not None:
    if affinityObject.pod_affinity.required_during_scheduling_ignored_during_execution is not None:
      logging.debug(affinityObject.pod_affinity.required_during_scheduling_ignored_during_execution[0])
    if affinityObject.pod_affinity.preferred_during_scheduling_ignored_during_execution is not None:
      logging.debug(affinityObject.pod_affinity.preferred_during_scheduling_ignored_during_execution[0])

  return availableNodes

def scoreNodes(availableNodes, requestedCPU, requestedMem):

  if requestedCPU is None:
    requestedCPU = 0
  if requestedMem is None:
    requestedMem = 0

  for node in availableNodes.items:
    cpu = helpers.checkCpuString(node.status.capacity['cpu'])
    mem = parse_quantity(node.status.capacity['memory'])
    node.metadata.labels['score'] = ((cpu - requestedCPU) / cpu) + ((mem - requestedMem) / mem)
    logging.debug("Score for %s: %s" % (node.metadata.labels['kubernetes.io/hostname'], node.metadata.labels['score']))
  
  if node.metadata.labels['score'] <= 0:
    availableNodes.items.pop(availableNodes.items.index(node))
  
  return sorted(availableNodes.items, key = lambda k: k.metadata.labels['score'], reverse=True)

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

def getPVs(apiClient, storageClassNames, podName):
  tempPvs = apiClient.list_persistent_volume()
  pvs = []
  for pv in tempPvs.items:
    if pv.spec.storage_class_name in storageClassNames and pv.status.phase == AVAILABLE or (pv.status.phase == BOUND and pv.spec.claim_ref.name == podName):
      pvs.append(pv)
  return pvs

def getPVCs(apiClient, namespace):
  tempPvcs = apiClient.list_namespaced_persistent_volume_claim(namespace)
  for pvc in tempPvcs.items:
    if not (pvc.status.phase == PENDING or pvc.status.phase == BOUND):
      tempPvcs.items.pop(tempPvcs.items.index(pvc))
  return tempPvcs

def bindPV(apiClient, bindings, namespace):
  boundPVs = []
  for requiredBinding in bindings:
    try:
      requiredBinding['pv'][0].claim_ref = {
        "api_version": 'v1',
        "field_path": None,
        "kind": 'PersistentVolumeClaim',
        "name": requiredBinding['pvc'].metadata.name,
        "namespace": namespace
      }
      logging.debug("PV: %s" % requiredBinding['pv'][0].metadata.name)
      logging.debug("Binding %s to %s" % (requiredBinding['pvc'].metadata.name, requiredBinding['pv'][0].metadata.name))
      apiClient.patch_persistent_volume(requiredBinding['pv'][0].metadata.name, requiredBinding['pv'][0])
      boundPVs.append({'pv': requiredBinding['pv'][0].metadata.name})
      #revertPVs(boundPVs)
    except ApiException as e:
      logging.error("Failed to bind %s to %s: %s" % (requiredBinding['pvc'].metadata.name, requiredBinding['pv'][0].metadata.name, e)) 
      #raise Exception("Failed to bind %s to %s" % (requiredBinding['pvc'].metadata.name, requiredBinding['pv'][0].metadata.name))
  return True

def bindPVC(apiClient, bindings, namespace):
  boundPVCs = []
  for requiredBinding in bindings:
    requiredBinding['pvc'].spec.volume_name = requiredBinding['pv'][0].metadata.name
    try:
      logging.debug("Binding %s and %s" % (requiredBinding['pv'][0].metadata.name, requiredBinding['pvc'].metadata.name))
      apiClient.patch_namespaced_persistent_volume_claim(requiredBinding['pvc'].metadata.name, namespace, requiredBinding['pvc'])
      boundPVCs.append({'pvc': requiredBinding['pvc'].metadata.name})
    except ApiException as e:
      logging.error("Failed to bind %s and %s: %s" % (requiredBinding['pv'][0].metadata.name, requiredBinding['pvc'].metadata.name, e))
      #revertPVCs(boundPVCs)
      #raise Exception("Failed to bind %s and %s" % (requiredBinding['pv'][0].metadata.name, requiredBinding['pvc'].metadata.name))
  return True

# /
  # Description: function to determine if PVC is already bound or if there is a PV available if unbound
  #
# /
def checkPVAllocatability(pvs, podPVCs, podName):
  pvPVC = {
    "allocatable": [],
    "unallocatable": [],
    "allocated": []
  }
  for pvc in podPVCs:
    pvc.metadata.name = pvc.spec.storage_class_name + "-" + podName
    if pvc.status.phase == BOUND:
      logging.debug("PVC %s for pod %s is already bound" % (pvc.metadata.name, podName))
      for pv in pvs:
        if pv.spec.claim_ref is not None and pv.spec.claim_ref.name == pvc.spec.storage_class_name + "-" + podName:
          pvPVC['allocated'].append({ 'pvc': pvc, 'pv': pv})
          break
      continue
    pvMap = {'pvc': pvc, 'pv': []}
    for pv in pvs:
      # check if this PV already has been not been claimed and the capacity is adquete
      if pv.spec.claim_ref is None and parse_quantity(pv.spec.capacity['storage']) >= parse_quantity(pvc.spec.resources.requests['storage']):
        pvMap['pv'].append(pv) #, pv.spec.capacity['storage']))
    if len(pvMap['pv']) > 0:
      pvPVC['allocatable'].append(pvMap)
    else:
      pvPVC['unallocatable'].append({'pvc': pvc.spec.storage_class_name + "-" + podName})
    #sorted(pvPVC, key = lambda k: k['pv'].spec.capacity['storage'], reverse=True)
    #pvcArray.append({"pvc": pvc, "pv": pvPVC})

  logging.debug("SORTED: %s" % pvPVC)
    
  return pvPVC

def checkNodeVolAffinity(pv, node):
  if pv.spec.node_affinity == None:
    return True
  
  passes_test = False
  if pv.spec.node_affinity.required is not None:
    for node_selector_term in pv.spec.node_affinity.required.node_selector_terms:
      if node_selector_term.match_expressions is not None:
        for expressions in node_selector_term.match_expressions:
          if expressions.operator == IN:
            if expressions.key in node.metadata.labels and node.metadata.labels[expressions.key] in expressions.values:
              passes_test = True
            else:
              return False
          elif expressions.operator == NOTIN:
            if expressions.key in node.metadata.labels and node.metadata.labels[expressions.key] not in expressions.values:
              passes_test = True
            else:
              return False
          elif expressions.operator == EXISTS:
            if expressions.key in node.metadata.labels:
              passes_test = True
            else:
              return False
          elif expressions.operator == DOESNOTEXIST:
            if expressions.key not in node.metadata.labels:
              passes_test = True
            else:
              return False
    return passes_test


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

        # Get the current pods deployed in the namespace.
        allPods = getAllPods(apiClient = v1, namespace = iCfg['namespace'])
        logging.debug("Current pods deployed for statefulSet: %s" % allPods)

        ## Get the current pods deployed in the statefulSet and what nodes they are running on.
        #ssPods = getSSPods(allPods, searchKey = "app", searchValue = ss)
        #logging.debug("Current pods deployed for statefulSet: %s" % ssPods)

        # determine which data centre to assign to the pod to
        dataCentreSelected = findDC(podName = pod, replicas = replicas, primaryDataCentres = iCfg['primaryDataCentres'], noPrimaryDataCentres = iCfg['noPrimaryDataCentres'])
        logging.debug("Selected data centre: %s" % dataCentreSelected)

        # Determine the available nodes for the data centre selected.
        nodesAvailable = nodes_available(apiClient = v1, dataCentre = dataCentreSelected, dataCentresLabel = iCfg['dataCentresLabel'])
        logging.debug("Available nodes for data centre %s: %s" % (dataCentreSelected, nodesAvailable))

        # Apply affinity and antiaffinity for available nodes
        suitableNodes = getAffinityNodes(podAffinity, nodesAvailable, allPods, pod)

        # Calculate score for each available node within the affinity/anti-affinity group
        sortedScoredNodes = scoreNodes(suitableNodes, requestedCPU, requestedMem)
        logging.debug("Scored available nodes: %s" % sortedScoredNodes)

        if pvcs:
          sc = []
          for pvc in pvcs:
            sc.append(pvc.spec.storage_class_name)
          logging.debug("Required storageClasses: %s" % sc)

          pvs = getPVs(apiClient = v1, storageClassNames = sc, podName = pod)
          logging.debug("All PVs: %s" % pvs)

          allPvcs = getPVCs(apiClient = v1, namespace = iCfg['namespace'])
          logging.debug("All PVCs: %s" % allPvcs)

          pvMap = checkPVAllocatability(pvs, pvcs, pod)
          logging.debug("PV and PVC mapping: %s" % pvMap)

          if len(pvMap['unallocatable']) > 0:
            for un in pvMap['unallocatable']:
              logging.error("Cannot allocate PVC %s" % un['pvc'])
            raise("It broke bad!")

          # Check allocated possible for selected node
          for node in sortedScoredNodes:
            passAllocated = False
            passUnallocated = False
            for pvPvcCombo in pvMap['allocated']:
              logging.debug("Checking allocated PVCs/PV node affinity for node %s" % node.metadata.name)
              passAllocated = checkNodeVolAffinity(pvPvcCombo['pv'], node)
              if passAllocated is False:
                sortedScoredNodes.pop(sortedScoredNodes.index(node))
                break
              
          finalResourceMap = []
          for node in sortedScoredNodes:
            # need a deepcopy here to reset
            tempPvMap = copy.deepcopy(pvMap['allocatable'])
            pvAllocSuccess = False
            for pvPvcCombo in tempPvMap:
              for pvAvail in pvPvcCombo['pv']:
                logging.debug("Checking unallocated PVCs/PV node affinity for node %s with %s" % (node.metadata.name, pvAvail.metadata.name))
                passUnallocated = checkNodeVolAffinity(pvAvail, node)
                if passUnallocated is True:
                  logging.debug("OK: PV %s for PVC %s and Node %s" %(pvAvail.metadata.name, pvPvcCombo['pvc'].metadata.name ,node.metadata.name))
                  pvAllocSuccess = True
                  break
                else:
                  logging.debug("Removing PV %s for PVC %s and Node %s" %(pvAvail.metadata.name, pvPvcCombo['pvc'].metadata.name ,node.metadata.name))
                  pvAvail.pop(pvAvail.index(pvAvail))
              if pvAllocSuccess is True:
                #finalResourceMap.append({node: node, })
                logging.debug("Success for PV/PVC on node")
                break
              else:
                sortedScoredNodes.pop(sortedScoredNodes.index(node))
            if pvAllocSuccess is True:
              pvMap['allocatable'] 
          
          if len(sortedScoredNodes) < 0:
            raise("No node available")

          # Bind PVC and PV
          boundSuccess = bindPV(v1, pvMap['allocatable'], namespace = iCfg['namespace'])
          boundPVCSuccess = bindPVC(v1, pvMap['allocatable'], namespace = iCfg['namespace'])
        else:
          logging.debug("No PVCs required")


        try:
          if len(sortedScoredNodes) > 0:
            logging.debug("Selected node: %s" % sortedScoredNodes[0].metadata.name)
            res = scheduler(apiClient = v1, bindingName = event['object'].metadata.name, targetName = sortedScoredNodes[0].metadata.name, namespace = iCfg['namespace'])
            logging.debug("Bind result: %s" % res)
            logging.info("Pod %s is bound to node %s" % (pod, sortedScoredNodes[0].metadata.name))
          else:
            logging.error("Cannot schedule")
        except client.rest.ApiException as e:
            logging.error(json.loads(e.body)['message'])
      else:
        logging.warn("This scheduler is only for statefulSets")
                    
if __name__ == '__main__':
    main()