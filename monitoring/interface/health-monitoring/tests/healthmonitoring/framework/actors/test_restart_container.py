'''
Created on 07-Feb-2020

@author: nageb
'''

import unittest

from healthmonitoring.framework.actors.restart_container import \
    RestartContainerActor


class TestRestartActor(unittest.TestCase):

    def test_container_restart_action(self):
        restart_container_actor = RestartContainerActor()
        with self.assertRaises(SystemExit) as command:
            restart_container_actor.act({})
        self.assertEqual(command.exception.code, 1)


if __name__ == "__main__":
    # import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
