import sys

from setuptools import setup, Extension, find_packages
import setuptools

# because of relative paths, this must be run from inside ray/lib/python/

setup(name="ray",
      version="0.1",
      packages=["ray"],
      zip_safe=False
)
