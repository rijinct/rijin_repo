from setuptools import setup

requirements = [l.strip() for l in open('requirements.txt').readlines()]

setup(
    name='pyspark_aggregation_framework',
    version='0.1',
    description='Data loading using spark Framework',
    license="Proprietary",
    classifiers=['License :: Other/Proprietary License'],
    packages=['framework'],
    install_requires=requirements,
    include_package_data=True,
    test_suite="tests",
)
