try:
  import os
  import json
  import logging
  import random
  import helpers
  import copy
  import re
  from kubernetes import client, config, watch
  from kubernetes.utils import parse_quantity
  from kubernetes.client.rest import ApiException
  from kubernetes.client.models.v1_persistent_volume_list import V1PersistentVolumeList
  from kubernetes.client.models.v1_node_list import V1NodeList
  from kubernetes.client.models.v1_pod_list import V1PodList
  from time import sleep
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
MAXCOUNT = 5
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
  #   dataCentresLabel: Node label to compare for `dataCentre`
# /
def nodes_available(dataCentre, apiClient, dataCentresLabel):
  goodNodes = V1NodeList(items = [])
  nodes = apiClient.list_node()
  for node in nodes.items:
    logging.debug("Node: %s" % node.metadata.name)
    if dataCentresLabel in node.metadata.labels and node.metadata.labels[dataCentresLabel] == dataCentre:
      for status in node.status.conditions:
        if status.status == "True" and status.type == "Ready":
          goodNodes.items.append(node)
          logging.debug("Adding node: %s" % node.metadata.name)
  return goodNodes

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
  runningPods = V1PodList(items = [])
  allPods = apiClient.list_namespaced_pod(namespace = namespace)
  for pod in allPods.items:
    if pod.status.phase == "Running":
      runningPods.items.append(pod)
  logging.debug("All Pods: %s" % runningPods)

  return runningPods

# /
  # Desccription: function determines what nodes satisfies pod affinity and antoaffinty
  #
  # Inputs:
  #   affinityObject: Pod's affinity objects
  #   targetPod: pod of interest
  #   pods: List of pods to compare against for affinifty and antaffinity
  #   node: Node to check
  #   topologyKey: Topology key to check against the pod for the node
  #   affinityType: AFFINITY or ANTIAFFINITY
# /
def sortPodAffinity(affinityObject, targetPod, pods, node, topologyKey, affinityType):
  logging.info("Starting affinity/antiaffinity for pod %s and node %s" % (targetPod, node.metadata.name))
  logging.debug("Toplogoy key: %s" % node.metadata.labels[topologyKey])
  suitableNode = False
  logging.debug(affinityObject.label_selector.match_expressions)
  for expressions in affinityObject.label_selector.match_expressions:
    logging.debug("EXPRESSIONS.key: %s EXPRESSIONS.operator: %s EXPRESSIONS.value: %s" % (expressions.key, expressions.operator, expressions.values))
    if expressions.operator == IN:
      logging.debug("IN")
      for pod in pods.items:
        logging.debug("POD: %s. LABELS: %s" % (pod.metadata.name, pod.metadata.labels))
        if expressions.key in pod.metadata.labels and pod.metadata.labels[expressions.key] in expressions.values:
          logging.debug("Key found: %s on %s" % (expressions.key, pod.metadata.name))
          if node.metadata.labels[topologyKey] == pod.spec.node_name:
            logging.debug("Node matches pod key, was %s" % pod.spec.node_name)
            if affinityType == ANTIAFFINITY:
              return False
            else:
              suitableNode = True
              break
          else:
            logging.debug("Node does not match pod key, was %s" % pod.spec.node_name)
            if affinityType == ANTIAFFINITY:
              suitableNode = True
              break
        else:
          logging.debug("Key NOT found: %s on %s" % (expressions.key, pod.metadata.name))
          if affinityType == ANTIAFFINITY:
            suitableNode = True
            break
          continue
    elif expressions.operator == NOTIN:
      for pod in pods.items:
        if expressions.key in pod.metadata.labels and pod.metadata.labels[expressions.key] not in expressions.value:
          if node.metadata.labels[topologyKey] == pod.spec.node_name:
            if affinityType == ANTIAFFINITY:
              return False
            else:
              suitableNode = True
              break
          else:
            if affinityType == ANTIAFFINITY:
              suitableNode = True
              break
        else:
          logging.info("Key NOT found: %s on %s" % (expressions.key, pod.metadata.name))
          if affinityType == ANTIAFFINITY:
            suitableNode = True
            break
          continue
    #elif expressions.operator == EXISTS:
    #  for pod in pods:
    #    if expressions.key in pod.metadata.labels:
    #      if node.metadata[.topologyKey] == pod.spec.node_name:
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
    logging.debug("Returning: %s for node %s" % (str(suitableNode), node.metadata.name))
  return suitableNode

# /
  # Desccription: function determines what nodes satisfies node affinity and antoaffinty
  #
  # Inputs:
  #   affinityObject: Pod's affinity objects
  #   nodes: List of worker nodes
# /
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

  return helpers.unique(selectedNodes)

# /
  # Description: Check the PV and node affinity is validate
  #
  # Inputs:
  #   pv: PV to check
  #   node: Node to check against
# /
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
  # Description: Get the affinity and antiaffinity
  #
  # Inputs:
  #   affinityObject: Pod's affinity objects
  #   availableNodes: list of worker nodes that can be used
  #   pods: List of pods in the namespace
  #   pod: Name of pod
# /
def getAffinityNodes(affinityObject, availableNodes, pods, pod):
  if affinityObject.pod_anti_affinity is not None:
    if affinityObject.pod_anti_affinity.required_during_scheduling_ignored_during_execution is not None:
      logging.debug(affinityObject.pod_anti_affinity.required_during_scheduling_ignored_during_execution)
      for node in reversed(availableNodes.items):
        for requiredRule in affinityObject.pod_anti_affinity.required_during_scheduling_ignored_during_execution:
          logging.debug("REQUIRED ANTIAFFINITY RULE: %s" % requiredRule)
          if requiredRule.label_selector.match_expressions is not None:
            if requiredRule.topology_key != 'kubernetes.io/hostname':
              logging.warn("Unknown `toplogyKey`")
              continue
            suitableNode = sortPodAffinity(requiredRule, pod, pods, node, requiredRule.topology_key, ANTIAFFINITY)
            if suitableNode is True:
              logging.info("Node %s is SUITABLE for pod Antiaffinity for pod %s" % (node.metadata.name, pod))
            else:
              logging.info("Node %s is NOT SUITABLE for pod Antiaffinity for pod %s" % (node.metadata.name, pod))
              if node in availableNodes.items:
                availableNodes.items.pop(availableNodes.items.index(node))
        
      if affinityObject.pod_anti_affinity.preferred_during_scheduling_ignored_during_execution is not None:
        logging.warn("Preferred Pod Anti Affinity is ignored")
        logging.debug(affinityObject.pod_anti_affinity.preferred_during_scheduling_ignored_during_execution)
  for node in availableNodes.items:
    logging.info("Remaining node: %s" % node.metadata.name)
  if affinityObject.pod_affinity is not None:
    if affinityObject.pod_affinity.required_during_scheduling_ignored_during_execution is not None:
      logging.debug(affinityObject.pod_affinity.required_during_scheduling_ignored_during_execution)
      for node in reversed(availableNodes.items):
        for requiredRule in affinityObject.pod_affinity.required_during_scheduling_ignored_during_execution:
          logging.debug("REQUIRED AFFINITY RULE: %s" % requiredRule)
          if requiredRule.label_selector.match_expressions is not None:
            if requiredRule.topology_key != 'kubernetes.io/hostname':
              logging.warn("Unknown `toplogyKey`")
              continue
            suitableNode = sortPodAffinity(requiredRule, pod, pods, node, requiredRule.topology_key, AFFINITY)
            if suitableNode is True:
              logging.info("Node %s is SUITABLE for pod Affinity for pod %s" % (node.metadata.name, pod))
            else:
              logging.info("Node %s is NOT SUITABLE for pod Affinity for pod %s" % (node.metadata.name, pod))
              if node in availableNodes.items:
                availableNodes.items.pop(availableNodes.items.index(node))

    if affinityObject.pod_affinity.preferred_during_scheduling_ignored_during_execution is not None:
      logging.debug(affinityObject.pod_affinity.preferred_during_scheduling_ignored_during_execution)
      logging.warn("Preferred Pod Anti Affinity is ignored")

  return availableNodes

# /
  # Description: Score the worker nodes for available resources
  #
  # Inputs:
  #   availableNodes: list of worker nodes that can be used
  #   requestedCPU: Request CPU for the pod
  #   requestedMem: request memoery for the pod
# /
def scoreNodes(availableNodes, requestedCPU, requestedMem):

  if requestedCPU is None:
    requestedCPU = 0
  if requestedMem is None:
    requestedMem = 0

  for node in reversed(availableNodes.items):
    cpu = helpers.checkCpuString(node.status.capacity['cpu'])
    mem = parse_quantity(node.status.capacity['memory'])
    node.metadata.labels['score'] = ((cpu - requestedCPU) / cpu) + ((mem - requestedMem) / mem)
    logging.debug("Score for %s: %s" % (node.metadata.labels['kubernetes.io/hostname'], node.metadata.labels['score']))
  
    if node.metadata.labels['score'] <= 0:
      availableNodes.items.pop(availableNodes.items.index(node))
  
  return sorted(availableNodes.items, key = lambda k: k.metadata.labels['score'], reverse=True)

# /
  # Description: Calculate the total resources needed for the pod
  #
  # Inputs:
  #   containerArray: List of container objects
# /
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

# /
  # Description: Retrieves all PVs available for purpose.
  #
  # Inputs:
  #   apiClient: Kubernetes API client
  #   storageClassNames: An array of storageClass naems that come from the template of PVCs from the statefuleSet
  #   podName: Name of the pod (which will be used for form the PVC name)
# /
def getPVs(apiClient, storageClassNames, podName):
  tempPvs = apiClient.list_persistent_volume()
  pvs = []
  for pv in tempPvs.items:
    for storageClass in storageClassNames:
      if pv.spec.storage_class_name == storageClass and pv.status.phase == AVAILABLE or (pv.status.phase == BOUND and pv.spec.claim_ref.name ==  storageClass + '-' + podName):
        pvs.append(pv)
  return pvs

# /
  # Description: Retrieves all PVCs for the pod.
  #
  # Inputs:
  #   apiClient: Kubernetes API client
  #   namespace: Namespace to retrive the PVCs from (e.g. where the pod will reside)
  #   pvcTemplateName: An array of VPC Template Names come from the template of PVCs from the statefuleSet
  #   podName: Name of the pod (which will be used for form the PVC name)
# /
def getPVCs(apiClient, namespace, pvcTemplateName, podName):
  tempPvcs = apiClient.list_namespaced_persistent_volume_claim(namespace)
  podPVCs = V1PersistentVolumeList(items = [])
  for pvc in tempPvcs.items:
    if pvc.status.phase == PENDING or pvc.status.phase == BOUND:
      for pvcName in pvcTemplateName:
        query = re.compile(r"^%s-%s.*$" % (pvcName, podName))
        if re.match(query, pvc.metadata.name):
          logging.debug("Adding PVC: %s" % pvc.metadata.name)
          podPVCs.items.append(pvc)
  return podPVCs

# /
  # Description: Bind the PV to the PVC
  #
  # Inputs:
  #   apiClient: Kubernetes client
  #   bindings: List of bindings
  #   namespace: Kubernetes namespace
# /
def bindPV(apiClient, bindings, namespace):
  boundPVs = []
  for requiredBinding in bindings:
    count = 0
    while count < MAXCOUNT:
      try:
        requiredBinding['pv'].spec.claim_ref = {
          "api_version": 'v1',
          "field_path": None,
          "kind": 'PersistentVolumeClaim',
          "name": requiredBinding['pvc'].metadata.name,
          "namespace": namespace
        }
        logging.debug("PV: %s" % requiredBinding['pv'].metadata.name)
        logging.debug("PV claim_ref: %s" % requiredBinding['pv'].metadata.name)
        logging.info("Binding the PV %s to PVC %s" % (requiredBinding['pv'].metadata.name, requiredBinding['pvc'].metadata.name))
        apiClient.patch_persistent_volume(requiredBinding['pv'].metadata.name, requiredBinding['pv'])
        boundPVs.append({'pv': requiredBinding['pv'].metadata.name})
        #revertPVs(boundPVs)
      except ApiException as e:
        if e.status == 409 and count < MAXCOUNT:
          count += 1
          logging.info("Conflict error, trying again")
          sleep(5)
        else:
          logging.error("Failed to bind %s to %s: %s" % (requiredBinding['pvc'].metadata.name, requiredBinding['pv'].metadata.name, e)) 
          return False
  return True

# /
  # Description: Bind the PVC to the PV
  #
  # Inputs:
  #   apiClient: Kubernetes client object
  #   bindings: List of bindings
  #   namespace: Kubernetes namespace
# /
def bindPVC(apiClient, bindings, namespace):
  boundPVCs = []
  for requiredBinding in bindings:
    count = 0
    requiredBinding['pvc'].spec.volume_name = requiredBinding['pv'].metadata.name
    while count < MAXCOUNT:
      try:
        logging.info("Binding the PVC %s to PV %s" % (requiredBinding['pvc'].metadata.name, requiredBinding['pv'].metadata.name))
        apiClient.patch_namespaced_persistent_volume_claim(requiredBinding['pvc'].metadata.name, namespace, requiredBinding['pvc'])
        boundPVCs.append({'pvc': requiredBinding['pvc'].metadata.name})
      except ApiException as e:
        if e.status == 409 and count < MAXCOUNT:
          count += 1
          logging.info("Conflict error, trying again")
          sleep(5)
        else:
          logging.error("Failed to bind %s and %s: %s" % (requiredBinding['pv'].metadata.name, requiredBinding['pvc'].metadata.name, e))
          return False
        #revertPVCs(boundPVCs)
  return True

# /
  # Description: function to determine if PVC is already bound or if there is a PV available if unbound
  #
  # Inputs:
  #   pvs: List of Persistent Volumes
  #   podPVCs: List of Persistent Volume Claims
  #   podName: Name of pod
# /
def checkPVAllocatability(pvs, podPVCs, podName):
  pvPVC = {
    "allocatable": [],
    "unallocatable": [],
    "allocated": []
  }
  for pvc in podPVCs.items:
    logging.info("PVC checking: %s" % pvc.metadata.name)
    logging.info("PVC State: %s" % pvc.status.phase)
    if pvc.status.phase == BOUND:
      logging.debug("PVC %s for pod %s is already bound" % (pvc.metadata.name, podName))
      for pv in pvs:
        if pv.spec.claim_ref is not None and pv.spec.claim_ref.name == pvc.metadata.name:
          pvPVC['allocated'].append({ 'pvc': pvc, 'pv': pv})
          break
      continue
    pvMap = {'pvc': pvc, 'pv': []}
    for pv in pvs:
      # check if this PV already has been not been claimed and the capacity is adquete
      if pv.spec.claim_ref is None and parse_quantity(pv.spec.capacity['storage']) >= parse_quantity(pvc.spec.resources.requests['storage']):
        pvMap['pv'].append(pv) #, pv.spec.capacity['storage']))
    sorted(pvMap['pv'], key = lambda k: k.spec.capacity['storage'], reverse=True)
    if len(pvMap['pv']) > 0:
      pvPVC['allocatable'].append(pvMap)
    else:
      pvPVC['unallocatable'].append({'pvc': pvc.metadata.name})
    
  return pvPVC

# /
  # Description: manages the storage for PVs and PVCs
  #
  # Inputs:
  #   apiClient: Kubernetes client object
  #   statefulSetPVCs: list of PVCS for the statefulSet
  #   nodes: list of available nodes to check for storage compliance
  #   pod: name of the pod of interest
  #   namespace: Kubernetes namespace
  #   
# /
def manageStorage(apiClient, statefulSetPVCs, nodes, pod, namespace):
  storageOK = False
  sc = []
  pvcTemplateNames = []
  for pvc in statefulSetPVCs:
    sc.append(pvc.spec.storage_class_name)
    pvcTemplateNames.append(pvc.metadata.name)
    sc = helpers.unique(sc)
  pvcTemplateNames = helpers.unique(pvcTemplateNames)
  #logging.debug("Required storageClasses: %s" % sc)

  pvs = getPVs(apiClient = apiClient, storageClassNames = sc, podName = pod)
  #logging.debug("All PVs: %s" % pvs)

  pvcs = getPVCs(apiClient = apiClient, namespace = namespace, pvcTemplateName = pvcTemplateNames, podName = pod)
  #logging.debug("All PVCs: %s" % pvcs)

  pvMap = checkPVAllocatability(pvs, pvcs, pod)
  for data in pvMap['allocatable']:
    logging.info("PVC: %s" % data['pvc'].metadata.name)
    for x in data['pv']:
      logging.info("Associated PVs: %s" % x.metadata.name)
  #logging.debug("PV and PVC mapping: %s" % pvMap)

  logging.debug("Allocated count: %s, unallocated count: %s, broken count: %s" % (len(pvMap['allocated']), len(pvMap['allocatable']), len(pvMap['unallocatable']) ))

  if len(pvMap['unallocatable']) > 0:
    for un in pvMap['unallocatable']:
      logging.error("Cannot allocate PVC %s" % un['pvc'])
  else:
    # Check allocated possible for selected node
    phaseOneNodes = copy.deepcopy(nodes)
    for node in reversed(nodes):
      passAllocated = False
      passUnallocated = False
      for pvPvcCombo in pvMap['allocated']:
        logging.debug("Checking allocated PVCs/PV node affinity for node %s" % node.metadata.name)
        passAllocated = checkNodeVolAffinity(pv = pvPvcCombo['pv'], node = node)
        if passAllocated is False:
          if node in nodes:
            nodes.pop(nodes.index(node))
          break
      if len(phaseOneNodes) < 1:
        logging.warn("No nodes available for the bound PVC/PVs")
        break

    nodes = copy.deepcopy(phaseOneNodes)
    for node in reversed(nodes):
      # need a deepcopy here to reset
      tempPvMap = copy.deepcopy(pvMap['allocatable'])
      # set the allocation to be false by default
      pvAllocSuccess = False
      for pvPvcCombo in tempPvMap:
        for pvAvail in reversed(pvPvcCombo['pv']):
          logging.debug("Checking unallocated PVCs/PV node affinity for node %s with PV %s" % (node.metadata.name, pvAvail.metadata.name))
          passUnallocated = checkNodeVolAffinity(pv = pvAvail, node = node)
          if passUnallocated is True:
            logging.debug("PV %s ok for PVC %s and Node %s" %(pvAvail.metadata.name, pvPvcCombo['pvc'].metadata.name ,node.metadata.name))
            pvAllocSuccess = True
            break
          else:
            logging.debug("Removing PV %s from list for PVC %s and Node %s" %(pvAvail.metadata.name, pvPvcCombo['pvc'].metadata.name ,node.metadata.name))
            pvAvail.pop(pvAvail.index(pvAvail))
        if pvAllocSuccess is True:
          logging.debug("Success for PV/PVC on node")
          break
        else:
          nodes.pop(nodes.index(node))
      # check if the PVC allocation test was correct
      #if pvAllocSuccess is True:
      #  pvMap['allocatable'] 
          
    if len(nodes) < 0:
      logging.warn("No node available")
      return False
    else:
      # clean up and make PV allocation unique
      pvToPVC = helpers.cleanMultiples(pvpvcList = pvMap['allocatable'])
      
      # Bind PVC and PV
      for storage in pvToPVC:
        logging.info("Pod: %s, PVC allocatable: %s, PVs: %s" % (pod, storage['pvc'].metadata.name, storage['pv'].metadata.name))
      for storage in pvMap['allocated']:
        logging.info("Pod: %s, PVC bound: %s" % (pod, storage['pvc'].metadata.name))
      boundPVSuccess = bindPV(apiClient = apiClient, bindings = pvToPVC, namespace = namespace)
      if boundPVSuccess is True:
        boundPVCSuccess = bindPVC(apiClient = apiClient, bindings = pvToPVC, namespace = namespace)
        if boundPVCSuccess is True:
          storageOK = True
  return storageOK

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

        requestedCPU, requestedMem = getTotalResourcesRequested(event['object'].spec.containers)
        logging.debug("Requests: cpu: %s, mem: %s" % (requestedCPU, requestedMem))

        # determine how many replicas and PVCs in the statefulSet
        replicas, ssPvcs = statefulSetCheck(stateful_set = ss, namespace = iCfg['namespace'])
        logging.debug("Number of replicas in statefulSet: %s" % replicas)
        logging.debug("PVCs in statefulSet: %s" % ssPvcs)

        # Get the current pods deployed in the namespace.
        allPods = getAllPods(apiClient = v1, namespace = iCfg['namespace'])
        for livePod in allPods.items:
          logging.debug("Current pod: %s" % livePod.metadata.name)

        ## Get the current pods deployed in the statefulSet and what nodes they are running on.
        #ssPods = getSSPods(allPods, searchKey = "app", searchValue = ss)
        #logging.debug("Current pods deployed for statefulSet: %s" % ssPods)

        # determine which data centre to assign to the pod to
        dataCentreSelected = findDC(podName = pod, replicas = replicas, primaryDataCentres = iCfg['primaryDataCentres'], noPrimaryDataCentres = iCfg['noPrimaryDataCentres'])
        logging.debug("Selected data centre: %s" % dataCentreSelected)

        # Determine the available nodes for the data centre selected.
        nodesAvailable = nodes_available(apiClient = v1, dataCentre = dataCentreSelected, dataCentresLabel = iCfg['dataCentresLabel'])
        for node in nodesAvailable.items:
          logging.debug("Available nodes for data centre %s: %s" % (dataCentreSelected, node.metadata.name))

        # Apply affinity and antiaffinity for available nodes
        suitableNodes = getAffinityNodes(podAffinity, nodesAvailable, allPods, pod)

        # Calculate score for each available node within the affinity/anti-affinity group
        sortedScoredNodes = scoreNodes(suitableNodes, requestedCPU, requestedMem)
        logging.debug("Scored available nodes: %s" % sortedScoredNodes)

        storageOK = False
        if ssPvcs:
          storageOK = manageStorage(apiClient = v1, statefulSetPVCs = ssPvcs, nodes = sortedScoredNodes, pod = pod, namespace = iCfg['namespace'])
        else:
          logging.info("No PVCs required")
          storageOK = True

        try:
          if len(sortedScoredNodes) > 0 and storageOK is True:
            logging.info("Selected node: %s" % sortedScoredNodes[0].metadata.name)
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