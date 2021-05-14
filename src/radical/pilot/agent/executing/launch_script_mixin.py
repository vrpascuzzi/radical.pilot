
__copyright__ = 'Copyright 2013-2021, The RADICAL-Cybertools Team'
__license__   = 'MIT'

import os


# ------------------------------------------------------------------------------
#
class LaunchScriptMixin:

    # --------------------------------------------------------------------------
    #
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    # --------------------------------------------------------------------------
    #
    def script_env(self, task):
        tid  = task['uid']
        td   = task['description']
        sbox = task['task_sandbox_path']

        output = '\n# Environment variables\n'
        # output += '. %s/env.orig\n'                % self._pwd
        output += 'export RADICAL_BASE="%s"\n'     % self._pwd
        output += 'export RP_SESSION_ID="%s"\n'    % self._cfg['sid']
        output += 'export RP_PILOT_ID="%s"\n'      % self._cfg['pid']
        output += 'export RP_AGENT_ID="%s"\n'      % self._cfg['aid']
        output += 'export RP_SPAWNER_ID="%s"\n'    % self.uid
        output += 'export RP_TASK_ID="%s"\n'       % tid
        output += 'export RP_TASK_NAME="%s"\n'     % td['name']
        output += 'export RP_GTOD="%s"\n'          % self.gtod
        output += 'export RP_TMP="%s"\n'           % self._task_tmp
        output += 'export RP_PILOT_SANDBOX="%s"\n' % self._pwd
        output += 'export RP_PILOT_STAGING="%s"\n' % self._pwd

        if self._prof.enabled:
            output += 'export RP_PROF="%s/%s.prof"\n' % (sbox, tid)
        else:
            output += 'unset  RP_PROF\n'

        if 'RP_APP_TUNNEL' in os.environ:
            output += 'export RP_APP_TUNNEL="%s"\n' % os.environ['RP_APP_TUNNEL']

        output += """
prof(){
    if test -z "$RP_PROF"
    then
        return
    fi
    event=$1
    msg=$2
    now=$($RP_GTOD)
    echo "$now,$event,task_script,MainThread,$RP_TASK_ID,AGENT_EXECUTING,$msg" >> $RP_PROF
}
"""
        # FIXME: this should be set by an LaunchMethod filter or something
        output += 'export OMP_NUM_THREADS="%s"\n' % td['cpu_threads']

        # also add any env vars requested in the task description
        if td['environment']:
            for key, val in td['environment'].items():
                output += 'export %s="%s"\n' % (key, val)

        return output

    # --------------------------------------------------------------------------
    #
    @staticmethod
    def script_task_exec(launch_command, *args):
        exec_cmd = launch_command
        if len(args) >= 2:
            exec_cmd += ' 1> %s 2> %s' % tuple(args[:2])

        output  = '\n# The command to run\n'
        output += 'prof task_exec_start\n'
        output += '%s\nRETVAL=$?\n' % exec_cmd
        output += 'prof task_exec_stop\n'

        return output

    # --------------------------------------------------------------------------
    #
    @staticmethod
    def script_pre_exec(task):
        output = ''

        if task['description']['pre_exec']:
            fail_option = ' (echo "pre_exec failed"; false) || exit'

            output += '\n# Pre-exec commands\n'
            output += 'prof task_pre_start\n'
            for elem in task['description']['pre_exec']:
                output += '%s || %s\n' % (elem, fail_option)
            output += 'prof task_pre_stop\n'

        return output

    # --------------------------------------------------------------------------
    #
    @staticmethod
    def script_post_exec(task):
        output = ''

        if task['description']['post_exec']:
            fail_option = ' (echo "post_exec failed"; false) || exit'

            output += '\n# Post-exec commands\n'
            output += 'prof task_post_start\n'
            for elem in task['description']['post_exec']:
                output += '%s || %s\n' % (elem, fail_option)
            output += 'prof task_post_stop "$ret=RETVAL"\n'

        return output

# ------------------------------------------------------------------------------
