
__copyright__ = "Copyright 2016, http://radical.rutgers.edu"
__license__   = "MIT"


from .base import LaunchMethod


# ------------------------------------------------------------------------------
#
class Fork(LaunchMethod):

    # --------------------------------------------------------------------------
    #
    def __init__(self, name, cfg, session):

        LaunchMethod.__init__(self, name, cfg, session)


    # --------------------------------------------------------------------------
    #
    def _configure(self):
        # "Regular" tasks
        self.launch_command = ''

    # --------------------------------------------------------------------------
    #
    @classmethod
    def rm_config_hook(cls, name, cfg, rm, log, profiler):
        return {'version_info': {
            name: {'version': '0.42', 'version_detail': 'There is no spoon'}}}

    # --------------------------------------------------------------------------
    #
    def construct_command(self, t, launch_script_hop):

        # NOTE: ignore thread and process counts, and expect application to do
        #       the needful

        td          = t['description']
        task_exec    = td['executable']
        task_args    = td.get('arguments') or []
        task_argstr  = self._create_arg_string(task_args)

        if task_argstr:
            command = "%s %s" % (task_exec, task_argstr)
        else:
            command = task_exec

        return command, None


# ------------------------------------------------------------------------------

