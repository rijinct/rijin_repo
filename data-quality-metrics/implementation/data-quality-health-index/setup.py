from setuptools import setup
import os

requirements = [l.strip() for l in open('requirements.txt').readlines()]

setup(
    name='data-quality-health-index',
    version='0.1',
    description='Data Quality Health Index',
    license="Proprietary",
    classifiers=['License :: Other/Proprietary License'],
    packages=['com'],
    install_requires=requirements,
    include_package_data=True,
    test_suite="test",
)