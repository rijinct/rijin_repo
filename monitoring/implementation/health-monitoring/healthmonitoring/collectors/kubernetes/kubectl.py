'''
Created on 24-Jul-2020

@author: nageb
'''

from pathlib import Path
import re
import sys

from healthmonitoring.framework.util.specification import SpecificationUtil


def main():
    print(_execute(sys.argv))


def _execute(args):
    if len(args) != 2:
        raise ValueError("Expected single argument")
    raw_data = Collector().collect(args[1])
    return Presenter(raw_data).present()


class Collector:

    _NAMESPACE = "c3mod"

    def collect(self, command):

        try:
            return eval("self._{}()".format(command))
        except AttributeError as e:
            raise NameError(command, e)

    def _get_glusterfs_status(self):
        return _EndpointsCollector(Collector._NAMESPACE).\
            contains("dis-nci-glusterfs")

    def _get_paas_status(self):
        return _PodsCollector("paas").all_pods_ok()

    def _get_ncms_status(self):
        return _PodsCollector("ncms").all_pods_ok()

    def _get_rook_status(self):
        return _PodsCollector("rook-ceph").all_pods_ok()

    def _get_csfp_af_status(self):
        return _PodsCollector("dis-apcore").all_pods_ok()

    def _get_universes_status(self):
        return _JobsCollector(Collector._NAMESPACE).has_succeeded('universe')

    def _get_usecases_status(self):
        return _JobsCollector(Collector._NAMESPACE).has_succeeded('usecase')


class Presenter:

    def __init__(self, data):
        self._data = data

    def present(self):
        return "1" if self._data else "0"


class _EndpointsCollector:
    _ENDPOINTS_FILE = "endpoints.txt"

    def __init__(self, namespace):
        self._namespace = namespace

    def contains(self, endpoint):
        with open(self._get_pathname()) as file:
            return bool(_Util.fgrep("dis-nci-glusterfs", file.readlines()))

    def _get_pathname(self):
        return _Util.get_directory() / "endpoints-{}.txt".format(
            self._namespace)


class _JobsCollector:

    def __init__(self, namespace):
        self._namespace = namespace

    def has_succeeded(self, job_name):
        with open(self._get_pathname()) as file:
            matched_lines = _Util.fgrep(job_name, file.readlines())
            failed_lines = _Util.fgrep("0/1", matched_lines)
            return True if matched_lines and not failed_lines else False

    def _get_pathname(self):
        return _Util.get_directory() / "jobs-{}.txt".format(self._namespace)


class _PodsCollector:

    def __init__(self, namespace):
        self._namespace = namespace

    def all_pods_ok(self):
        with open(self._get_pathname()) as file:
            lines = file.readlines()
            status_lines = _Util.cut(lines[1:], field=3, delim=' ')
            total_lines = len(status_lines)
            running_lines = len(_Util.fgrep("Running", status_lines))
            completed_lines = len(_Util.fgrep("Completed", status_lines))
            successful_lines = running_lines + completed_lines
            return successful_lines == total_lines and total_lines > 0

    def _get_pathname(self):
        return _Util.get_directory() / "pods-{}.txt".format(self._namespace)


class _Util:

    _DIRECTORY = Path(SpecificationUtil.get_property('Kubectl', 'mount'))
    _TEST_DIRECTORY = Path("../../../resources")

    under_test = False

    @staticmethod
    def get_directory():
        return _Util._TEST_DIRECTORY if _Util.under_test else _Util._DIRECTORY

    @staticmethod
    def fgrep(pattern, lines):
        matched_lines = []
        for line in lines:
            if pattern in line:
                matched_lines.append(line)

        return matched_lines

    @staticmethod
    def cut(lines, field, delim=' '):
        for i in range(len(lines)):
            line = lines[i]
            parts = re.split('{}+'.format(delim), line)
            lines[i] = parts[field - 1]
        return lines


if __name__ == "__main__":
    main()
