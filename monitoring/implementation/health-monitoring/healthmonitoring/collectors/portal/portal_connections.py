'''
Created on 21-Apr-2020

@author: a4yadav
'''
from subprocess import getoutput
from datetime import datetime
from healthmonitoring.collectors.utils.app_variables import AppVariables

from healthmonitoring.collectors import _LocalLogger

logger = _LocalLogger.get_logger(__name__)

portal_hosts = AppVariables.load_app_variables()['cemod_portal_hosts'].split(
    ' ')
date = datetime.now().strftime('%Y-%m-%d %H:00:00')


def get_portal_connections():
    dict_list = []
    for portal_host in portal_hosts:
        logger.info('Getting connections for %s' % portal_host)
        dict_list.append(_get_connections_for_portal_host(portal_host))
    return dict_list


def _get_connections_for_portal_host(portal_host):
    return dict(Date=date,
                Host=portal_host,
                LoadBalancer=_get_connection_count(portal_host, 443),
                PortalService=_get_connection_count(portal_host, 8443),
                PortalDb=_get_connection_count(portal_host, 5432))


def _get_connection_count(host, port):
    command = '''ssh %s "ss -nap | grep ':%d' | wc -l"''' % (host, port)
    logger.debug("Executing command: '%s'" % command)
    return int(getoutput(command))


def main():
    if portal_hosts:
        print(get_portal_connections())
    else:
        logger.info('Portal Nodes are empty, nothing to parse')


if __name__ == '__main__':
    main()
