from healthmonitoring.framework.actors.restart import _ServiceRestarter


class _PortalServiceRestarter(_ServiceRestarter):

    def __init__(self, service_name):
        _ServiceRestarter.__init__(self, service_name)
        self.script_path = "/home/portal/ifw/install/scripts/portal_services_controller.pl"  # noqa: E501
