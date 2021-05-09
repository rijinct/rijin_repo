'''
Created on 07-May-2020

@author: nageb
'''
from pathlib import Path


def get_resource_path(relative_path_to_resources):
    under_test = True
    self_pathname = Path(__file__)
    util_pathname = self_pathname.parent
    collectors_pathname = util_pathname.parent
    healthmonitoring_pathname = collectors_pathname.parent
    tests_pathname = healthmonitoring_pathname.parent
    project_pathname = tests_pathname.parent
    if under_test:
        resources_pathname = project_pathname / "tests" / "resources"
    else:
        resources_pathname = project_pathname / "resources"
    pathname = resources_pathname / relative_path_to_resources
    return pathname.resolve()
