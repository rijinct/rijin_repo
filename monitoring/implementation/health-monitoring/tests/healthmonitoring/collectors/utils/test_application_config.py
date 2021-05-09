'''
Created on 24-Jun-2020

@author: mohamsha
'''
import unittest
from healthmonitoring.collectors.utils.application_config import \
    ApplicationConfig
from mock.mock import patch
from pathlib import Path


class TestApplicationConfig(unittest.TestCase):

    @patch('healthmonitoring.collectors.utils.application_config.config')
    @patch('healthmonitoring.framework.util.loader.YamlSpecificationLoader')
    def test_get_kafka_adap_port(
            self, MockedYamlSpecificationLoader, MockConfig):
        MockConfig.properties.get.return_value = \
            str(Path(__file__).joinpath(
                "..", "..", "..", "..",
                "resources", "application_config.xml").resolve())
        self.assertEqual(ApplicationConfig().get_kafka_adap_port(
            "FLBB_APPLICATIONS_1"), '8131,8132')


if __name__ == "__main__":
    unittest.main()
