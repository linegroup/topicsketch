from distutils.core import setup
from Cython.Build import cythonize

setup(
  name = 'Fast Smoother',
  ext_modules = cythonize("fast_smoother.pyx"),
)
