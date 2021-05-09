'''
Created on 18-Dec-2020

@author: deerakum
'''
from lxml import html

from dbUtils import DBUtil

from healthmonitoring.collectors import _LocalLogger
logger = _LocalLogger.get_logger(__name__)


def main():
    _execute()


def _execute():
    raw_data = Collector().collect()
    return Presenter(raw_data).present()


class Collector:

    def __init__(self):
        self._adap_info = []
        self._version_info = []
        self._udfs_info = []
        self._plat_app_info = []

    def collect(self):
        logger.info("Started Parsing version.txt file")
        html_obj = self.get_html_object()
        all_pre_tags = html_obj.xpath("//pre")
        self._populate_cemod_os_info(all_pre_tags)
        self._populate_kerb_content_app_info(html_obj)
        self._populate_platform_app_info(all_pre_tags[-2])
        self._populate_udf_info(all_pre_tags[-3])
        self._populate_adaptations_info(all_pre_tags[-1])
        logger.info("Parsing Completed")
        return self._adap_info, self._udfs_info, \
            self._plat_app_info, self._version_info

    def get_html_object(self):
        with open("/mnt/staging/version.txt") as versions_reader:
            version_info = versions_reader.read()
        return html.fromstring(version_info)

    def _populate_cemod_os_info(self, all_pre_tags):
        cemod_version = all_pre_tags[0].text_content()
        os_version = all_pre_tags[2].text_content()
        self._version_info.append({
            "Type": "Cemod_version",
            "Value": cemod_version.split(":")[1].strip()
        })
        self._version_info.append({
            "Type": "OS_version",
            "Value": os_version.split(":")[1].strip()
        })

    def _populate_kerb_content_app_info(self, html_obj):
        content_app = html_obj.xpath('//table//td/text()[normalize-space()]')
        self._version_info.append({
            "Type": "Kerberos",
            "Value": content_app[0].strip("\n ")
        })
        self._version_info.append({
            "Type": "ContentApps",
            "Value": content_app[1].strip("\n ")
        })

    def _populate_platform_app_info(self, plat_app_info):
        plats = plat_app_info.text_content().strip().split("\n")
        for plats_app in plats:
            self._plat_app_info.append({
                "Type": "PlatformApps",
                "Value": plats_app
            })

    def _populate_udf_info(self, udf_info):
        udfs = udf_info.text_content().strip().split("\n")[2:]
        for udf in udfs:
            self._udfs_info.append({"Type": "Udf", "Value": udf})

    def _populate_adaptations_info(self, adap_info):
        adaptations = adap_info.text_content().strip().split("\n")
        for adaptation in adaptations:
            self._adap_info.append({"Type": "Adaptation", "Value": adaptation})


class Presenter:

    def __init__(self, data):
        self._adaps_info, self._udfs_info, \
            self._plat_app_info, self._version_app_info = data

    def present(self):
        self._push_data_to_db()
        return self._adaps_info, self._udfs_info, \
            self._plat_app_info, self._version_app_info

    def _push_data_to_db(self):
        meta_data_info = {
            'adaptationsInfo': str(self._adaps_info),
            'udfsInfo': str(self._udfs_info),
            'platformAppsInfo': str(self._plat_app_info),
            'versionInfo': str(self._version_app_info)
        }
        for info_type, info_value in meta_data_info.items():
            DBUtil().jsonPushToMariaDB(info_value.replace("'", '"'), info_type)
        logger.info("Data has been pushed to db")


if __name__ == '__main__':
    main()
