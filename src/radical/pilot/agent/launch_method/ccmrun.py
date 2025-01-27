
__copyright__ = "Copyright 2016, http://radical.rutgers.edu"
__license__   = "MIT"


import radical.utils as ru

from .base import LaunchMethod


# ------------------------------------------------------------------------------
#
class CCMRun(LaunchMethod):

    # --------------------------------------------------------------------------
    #
    def __init__(self, name, cfg, session):

        LaunchMethod.__init__(self, name, cfg, session)


    # --------------------------------------------------------------------------
    #
    def _configure(self):
        # ccmrun: Cluster Compatibility Mode (CCM) job launcher for Cray systems
        self.launch_command = ru.which('ccmrun')


    # --------------------------------------------------------------------------
    #
    def construct_command(self, t, launch_script_hop):

        # NOTE: we actually ignore the slots defined by the scheduler

        td          = t['description']
        task_exec    = td['executable']
        task_cores   = td['cpu_processes']  # FIXME: cpu_threads
        task_args    = td.get('arguments') or []
        task_argstr  = self._create_arg_string(task_args)

        ccmrun_command = "%s -n %d %s %s" % (self.launch_command, task_cores,
                                             task_exec, task_argstr)

        return ccmrun_command, None


# ------------------------------------------------------------------------------

