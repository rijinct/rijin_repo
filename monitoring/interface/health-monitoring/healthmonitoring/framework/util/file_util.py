'''
Created on 09-Apr-2020

@author: nageb
'''
from pathlib import Path

under_test = False


def get_resource_path(relative_path_to_resources):
    file_util_pathname = Path(__file__)
    util_pathname = file_util_pathname.parent
    framework_pathname = util_pathname.parent
    healthmonitoring_pathname = framework_pathname.parent
    project_pathname = healthmonitoring_pathname.parent
    if under_test:
        resources_pathname = project_pathname / "tests" / "resources"
    else:
        resources_pathname = project_pathname / "resources"
    pathname = resources_pathname / relative_path_to_resources
    return pathname.resolve()
