from setuptools import setup

requirements = [l.strip() for l in open('requirements.txt').readlines()]

setup(
    name='health-monitoring-collectors',
    version='0.1',
    description='Health Monitoring Scripts',
    license="Proprietary",
    classifiers=['License :: Other/Proprietary License'],
    packages=['healthmonitoring'],
    install_requires=requirements,
    include_package_data=True,
    test_suite="tests",
)
