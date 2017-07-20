from distutils.core import setup
from Cython.Build import cythonize

setup(
  name = 'Fast Significance Scorer',
  ext_modules = cythonize("fast_signi.pyx"),
)
