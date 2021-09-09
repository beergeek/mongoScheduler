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