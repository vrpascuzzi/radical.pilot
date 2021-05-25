
__copyright__ = 'Copyright 2016-2021, The RADICAL-Cybertools Team'
__license__   = 'MIT'

import os

import radical.utils as ru

from .base import LaunchMethod


# ------------------------------------------------------------------------------
#
class APRun(LaunchMethod):

    # --------------------------------------------------------------------------
    #
    def _configure(self):

        self.launch_command = ru.which('aprun')
        # TODO: ensure that only one concurrent aprun per node is executed!

    # --------------------------------------------------------------------------
    #
    def construct_command(self, t, launch_script_hop):

        td        = t['description']

        task_exec = td['executable']
        task_args = td['arguments']
        task_env  = td['environment']

        cmd_options = '-n %(cpu_processes)s ' \
                      '-N %(cpu_processes)s ' \
                      '-d %(cpu_threads)s' % td

        saga_smt = os.environ.get('RADICAL_SAGA_SMT')
        if saga_smt:
            cmd_options += ' -j %s' % saga_smt

        cmd_options += ' --cc depth '
        # CPU affinity binding
        # - use –d and --cc depth to let ALPS control affinity
        # - use --cc none if you want to use OpenMP (or KMP) env. variables
        #   to specify affinity: --cc none -e KMP_AFFINITY=<affinity>
        #   (*) turn off thread affinity: export KMP_AFFINITY=none

        cmd_options += ' '.join(['-e %s=%s' % x for x in task_env.items()])
        if td['cpu_threads'] and 'OMP_NUM_THREADS' not in task_env:
            cmd_options += ' -e OMP_NUM_THREADS=%(cpu_threads)s' % td

        task_args_str = self._create_arg_string(task_args)
        if task_args_str:
            task_exec += ' %s' % task_args_str

        cmd = '%s %s %s' % (self.launch_command, cmd_options, task_exec)

        self._log.debug('aprun cmd: %s', cmd)

        return cmd, None

# ------------------------------------------------------------------------------

# OBSOLETE (kept for records)

# The relevant aprun documentation is at (search for `-cc` and `-L`):
# http://docs.cray.com/books/S-2496-4101/html-S-2496-4101/cnl_apps.html
#
#   -L node_list    : candidate nodes to constrain application placement
#   -n pes          : number of PEs to place
#   -N pes_per_node : number of PEs to place per node
#   -d depth        : number of CPUs for each PE and its threads
#   -cc cpu_list    : bind processing elements (PEs) to CPUs.
#
# (CPUs here mostly means cores)
#
# Example:
#     aprun -L node_1 -n 1 -N 1 -d 3 -cc 0,1,2       cmd : \
#           -L node_2 -n 1 -N 1 -d 3 -cc 0,1,2       cmd : \
#           -L node_3 -n 2 -N 2 -d 3 -cc 0,1,2:3,4,5 cmd :
#
# Each node can only be used *once* in that way for any individual
# aprun command.  This means that the depth must be uniform for that
# node, over *both* cpu and gpu processes.  This limits the mixability
# of cpu and gpu processes for tasks started via aprun.
#
# For command construction we sift through the task slots and create a slot
# list which basically defines sets of cores (-cc) for each node (-L).
# Those sets need to have the same size per node (the depth -d).
# The number of sets defines the number of procs to start (-n/-N).
#
# If the list of arguments for aprun becomes too long, then a temporary
# hostfile is used, and is referenced from the aprun command line.
#
# Because of limitations in file support the following is applied:
#
#   original:
#     aprun -L node_1 -n 1  -N 1 -d 3 -cc 0,1,2       cmd : \
#           -L node_2 -n 1  -N 1 -d 3 -cc 0,1,2       cmd : \
#           -L node_3 -n 2  -N 2 -d 3 -cc 0,1,2:3,4,5 cmd :
#
#   collapsed:
#     aprun -L node_1,node_2 -n 2 -N 1 -d 3 -cc 0,1,2       cmd : \
#           -L node_3        -n 2 -N 2 -d 3 -cc 0,1,2:3,4,5 cmd :
#
# Note that the `-n` argument needs to be adjusted accordingly.

# ------------------------------------------------------------------------------
