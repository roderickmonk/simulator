from distutils.core import setup
from Cython.Build import cythonize

setup(name='Speed Test',
      ext_modules=cythonize("speedtest2.pyx"))

      