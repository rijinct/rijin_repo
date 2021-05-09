from healthmonitoring.framework import config
from healthmonitoring.framework.util.loader import SpecificationLoader


class SpecificationUtil:

    @staticmethod
    def get_host(name):
        SpecificationLoader.load_if_required()
        return config.hosts_spec.get_host(name)

    @staticmethod
    def get_hosts(name):
        SpecificationLoader.load_if_required()
        return config.hosts_spec.get_hosts(name)

    @staticmethod
    def get_field(host_name, field):
        SpecificationLoader.load_if_required()
        return config.hosts_spec.get_host(host_name).fields[field]

    @staticmethod
    def get_property(*args):
        SpecificationLoader.load_if_required()
        property_value = config.monitoring_spec
        for key in args:
            property_value = property_value.get(key)
        return property_value
