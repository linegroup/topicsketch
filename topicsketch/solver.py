__author__ = 'Wei Xie'
__email__ = 'linegroup3@gmail.com'
__affiliation__ = 'Living Analytics Research Centre, Singapore Management University'
__website__ = 'http://mysmu.edu/phdis2012/wei.xie.2012'


import scipy.sparse.linalg as la
import numpy as np


# solve m2 = sum a_k * v_k * v_k', m3 = smu a_k * (r' * v_k) * v_k * v_k'
# return result (a, (r'*v), v)

def solve(_m2, _m3, _n,  _k):

    vals, vecs = la.eigsh(_m2, _k)

    vals = vals + 0j
    W = np.array(vecs, dtype=np.complex128)
    for i in range(_k):
        for j in range(_n):
            W[j, i] /= np.sqrt(vals[i])

    T = np.dot(W.T, _m3.dot(W))

    vals, vecs = np.linalg.eig(T)

    v = np.dot(W, np.linalg.solve(np.dot(W.T, W), vecs))

    s = []
    for i in range(_k):
        s.append(sum(v[:, i]))

    for i in range(_k):
        for j in range(_n):
            v[j, i] /= s[i]

    a = []
    for i in range(_k):
        v_ = np.dot(W.T, v[:, i])
        a.append(1. / np.dot(v_.T, v_))

    a = np.array(a)

    r = vals

    return a, r, v

