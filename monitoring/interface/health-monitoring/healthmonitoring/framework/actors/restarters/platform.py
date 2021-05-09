from healthmonitoring.framework.actors.restart import _ServiceRestarter


class _PlatformServiceRestarter(_ServiceRestarter):

    def __init__(self, service_name):
        _ServiceRestarter.__init__(self, service_name)
        self.script_path = "/opt/nsn/ngdb/ifw/bin/platform/platform_services_controller.pl"  # noqa: E501
