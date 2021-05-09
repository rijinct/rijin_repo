'''
Created on 09-Apr-2020

@author: nageb
'''
import os

from healthmonitoring.framework.config import properties
from healthmonitoring.framework.util import file_util

from healthmonitoring.framework.specification.yaml import \
    YamlSpecificationLoader


class TestUtil:

    @staticmethod
    def get_priority_directories():
        file_util.under_test = True
        properties.read(
            file_util.get_resource_path('monitoring.properties'))
        priority_directory = properties.get(
            'DirectorySection', 'PriorityDirectories')
        priority_directories_absolute_path = \
            TestUtil._get_absolute_path_for_priority_directory(
                priority_directory)
        return priority_directories_absolute_path

    @staticmethod
    def load_specification():
        file_util.under_test = True
        priority_directories = TestUtil.get_priority_directories()
        custom_directories = TestUtil.get_sub_directories(
            properties.get('DirectorySection', 'Custom'))
        standard_directories = TestUtil.get_sub_directories(
            properties.get('DirectorySection', 'Standard'))
        loader = YamlSpecificationLoader()
        loader.load_monitoring_specification(
            file_util.get_resource_path("."))
        loader.load_hosts_specification(
            file_util.get_resource_path("."))
        loader.load(priority_directories, custom_directories,
                    standard_directories)

    @staticmethod
    def get_sub_directories(directory):
        file_util.under_test = True
        directory = file_util.get_resource_path(directory)
        sub_directories_path = []
        for sub_directory in os.listdir(directory):
            if os.path.isdir(os.path.join(directory, sub_directory)):
                sub_directories_path.append(
                    os.path.join(directory, sub_directory))
        return sub_directories_path

    @staticmethod
    def _get_absolute_path_for_priority_directory(directory):
        priority_directories_absolute_path = []
        if directory:
            priority_directories = directory.split(',')
            priority_directories_absolute_path = \
                TestUtil._get_priority_directories_path(
                    priority_directories)
        return priority_directories_absolute_path

    @staticmethod
    def _get_priority_directories_path(priority_directories):
        priority_directories_absolute_path = []
        if not priority_directories:
            for priority_directory in properties.get(
                    'DirectorySection', 'PriorityDirectories').split(','):
                priority_directories_absolute_path.append(
                    file_util.get_resource_path(priority_directory))
        return priority_directories_absolute_path
