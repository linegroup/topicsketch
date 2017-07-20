__author__ = 'Wei Xie'
__email__ = 'linegroup3@gmail.com'
__affiliation__ = 'Living Analytics Research Centre, Singapore Management University'
__website__ = 'http://mysmu.edu/phdis2012/wei.xie.2012'

import os
from ctypes import cdll

if os.name == 'posix':
    hashBase = cdll.LoadLibrary('./c/mlh.so')
if os.name == 'nt':
    hashBase = cdll.LoadLibrary('./c/mlh.dll')


HASH_NUMBER = 5

hashBase.initialize(HASH_NUMBER)


def hash_code(txt):
    txt = txt.encode('ascii', 'xmlcharrefreplace')
    l = len(txt)
    l = min(32, l)

    ret = [hashBase.hash(txt, l, h) for h in xrange(HASH_NUMBER)]

    return ret

