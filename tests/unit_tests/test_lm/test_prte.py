
# pylint: disable=protected-access, unused-argument, no-value-for-parameter

from unittest import mock, TestCase

import radical.utils as ru

from .test_common                           import setUp
from radical.pilot.agent.launch_method.prte import PRTE


class TestPRTE(TestCase):

    # ------------------------------------------------------------------------------
    #
    @mock.patch.object(PRTE, '__init__', return_value=None)
    @mock.patch.object(PRTE, '_configure', return_value='prun')
    def test_construct_command(self, mocked_init, mocked_configure):

        test_cases = setUp('lm', 'prte')

        component = PRTE(name=None, cfg=None, session=None)

        component.name           = 'prte'
        component._verbose       = None
        component._log           = ru.Logger('dummy')
        component.launch_command = 'prun'

        for task, result in test_cases:

            if result == "RuntimeError":
                with self.assertRaises(RuntimeError):
                    _, _ = component.construct_command(task, None)

            else:
                command, hop = component.construct_command(task, None)
                self.assertEqual([command, hop], result, msg=task['uid'])


if __name__ == '__main__':

    tc = TestPRTE()
    tc.test_construct_command()


# ------------------------------------------------------------------------------
# pylint: enable=protected-access, unused-argument, no-value-for-parameter
