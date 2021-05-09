'''
Created on 24-Apr-2020

@author: nageb
'''

from pathlib import Path
import sys

from logger import Logger

from healthmonitoring.framework import config, runner
from healthmonitoring.framework.specification.yaml import \
    YamlEventsSpecificationWriter
from healthmonitoring.framework.audit.util import ReportToEventsSpecConverter

logger = Logger.getLogger(__name__)
logger.setLevel(config.log_level)


def main():
    _check_usage()
    directory = Path(sys.argv[1]).resolve()
    runner.load_config(Path(directory) / 'monitoring.properties')
    runner.load_all_specs(directory)
    runner.create_audit_reports(directory)
    for report_name in config.report_store.get_names():
        _convert_report(config.report_store.get(report_name), directory)


def _check_usage():
    if len(sys.argv) != 2:
        print("Usage: python {} <pathname to monitoring.properties>".
              format(sys.argv[0]))
        sys.exit(1)


def _convert_report(report, directory):
    events_spec = ReportToEventsSpecConverter.convert(report)
    report_directory = _get_report_directory(report, directory)

    print("Writing report '{}' to '{}'...".format(
        report.name, report_directory))

    _remove_files_if_present(report_directory)
    Path(report_directory).mkdir(parents=True, exist_ok=True)
    _create_files(events_spec, report_directory)


def _get_report_directory(report, directory):
    return Path(directory) / "standard" / "audit-{}".format(report.name)


def _remove_files_if_present(report_directory):
    for filename in "items.yaml", "triggers.yaml", "events.yaml":
        _remove_file_if_present(report_directory / filename)


def _remove_file_if_present(pathname):
    try:
        Path(pathname).unlink()
    except FileNotFoundError:
        pass


def _create_files(events_spec, report_directory):
    Path(report_directory / "items.yaml").write_text("items: {}")
    Path(report_directory / "triggers.yaml").write_text("triggers: []")
    YamlEventsSpecificationWriter.write(
        events_spec, report_directory / "events.yaml")


if __name__ == '__main__':
    main()
