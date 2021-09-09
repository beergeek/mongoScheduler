try:
  import decimal
  import re
  import logging
except ImportError as e:
  print(e)
  exit(1)


# helpers
TB = 1000 * 1000 * 1000 * 1000

TIB = 1024 * 1024 * 1024 * 1024

# GB - GigaByte size
GB = 1000 * 1000 * 1000
# GiB - GibiByte size
GIB = 1024 * 1024 * 1024

# MB - MegaByte size
MB = 1000 * 1000
# MiB - MebiByte size
MIB = 1024 * 1024

# KB - KiloByte size
KB = 1000
# KiB - KibiByte size
KIB = 1024

qtyCase = {
  'T': TB,
  'Ti': TIB,
  'G': GB,
  'Gi': GIB,
  'M': MB,
  'Mi': MIB,
  'k': KB,
  'K': KB,
  'ki': KIB,
  'Ki': KIB
}

def splitQuantityString(quantity):
  splitPattern = "^([0-9.]+)([eEinumkKMGTP][i]?)$"
  qtyArray = re.findall(splitPattern, quantity)
  logging.debug("Quantity: %s %s" % (qtyArray[0][0], qtyArray[0][1]))

  multiplier = qtyCase.get(qtyArray[0][1], 1)

  return decimal.Decimal(qtyArray[0][0]) * multiplier

def checkCpuString(quantity):
  logging.debug("\"%s\"" % quantity)
  splitPattern = "^([0-9.]+)([m]?)$"
  qtyArray = re.findall(splitPattern, quantity)
  logging.debug("CPU Quantity: %s" % qtyArray)

  if len(qtyArray[0]) and qtyArray[0][1] == 'm':
    return decimal.Decimal(qtyArray[0][0]) / 1000
  else:
    return decimal.Decimal(qtyArray[0][0])

# /
  # Desccription: Makes a list only have unique members
  #
  # Inputs:
  #   un_unique_list: List to make unique
# /
def unique(un_unique_list):
  # initialize a null list
  unique_list = []
   
  # traverse for all elements
  for x in un_unique_list:
      # check if exists in unique_list or not
      if x not in unique_list:
          unique_list.append(x)

  return unique_list

# /
  # Desccription: Removes all multiple instances
  #
  # Inputs:
  #   pvpvcList: List of pvc and pv objects
# /
def cleanMultiples(pvpvcList):
  pvFound = []
  finalMap = []
  for data in pvpvcList:
    for pv in data['pv']:
      if pv in pvFound:
        continue
      else:
        pvFound.append(pv)
        finalMap.append({"pvc": data['pvc'], "pv": pv})
        break
  for i in finalMap:
    logging.info("PVC %s, PV: %s" % (i['pvc'].metadata.name, i['pv'].metadata.name))
  return finalMap