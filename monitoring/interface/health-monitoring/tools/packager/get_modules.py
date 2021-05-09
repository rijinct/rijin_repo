#!/usr/bin/python3
import os
import sys
import re


def _get_module_name(name):
    return re.search(r"healthmonitoring[/\\](.*)", name).group().replace(
        "/", ".").replace("\\", ".").replace(".py", "")


def _get_module_names(path, files):
    modules = []
    for module in files:
        if not module.endswith("__init__.py") and module.endswith(".py"):
            modules.append(_get_module_name(
                os.path.join(path, module)))
    return modules


def get_list_of_modules(path):
    modules = ["pkg_resources.py2_warn"]
    for root, directories, files in os.walk(os.path.normpath(path)):
        modules.extend(_get_module_names(root, files))
    return modules


def main():
    print(get_list_of_modules(sys.argv[1]))


if __name__ == "__main__":
    main()
