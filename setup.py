"""
We need to include this file to enable the editable mode:
'pip install -e .'
"""
from setuptools import setup

setup(package_data = {
        'application': ['py.typed'],
    })

