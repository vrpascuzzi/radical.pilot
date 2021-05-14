
__copyright__ = 'Copyright 2013-2021, The RADICAL-Cybertools Team'
__license__   = 'MIT'

import os
import stat
import pprint

import radical.utils as ru

from ...  import utils     as rpu
from ...  import states    as rps
from ...  import constants as rpc

from .popen import Popen


# ------------------------------------------------------------------------------
#
class PopenSH(Popen):

    # --------------------------------------------------------------------------
    #
    def initialize(self):
        super().initialize()

        self._tasks_to_terminate = []

    # --------------------------------------------------------------------------
    #
    def spawn(self, launcher, task):

        tid  = task['uid']
        td   = task['description']
        sbox = task['task_sandbox_path']

        # make sure the sandbox exists
        self._prof.prof('exec_mkdir', uid=tid)
        rpu.rec_makedir(sbox)
        self._prof.prof('exec_mkdir_done', uid=tid)

        launch_script_name = '%s/%s.sh' % (sbox, tid)
        slots_fname        = '%s/%s.sl' % (sbox, tid)

        try:
            launch_command, _ = launcher.construct_command(task,
                                                           launch_script_name)
        except Exception as e:
            msg = 'Error in spawner (%s)' % e
            self._log.exception(msg)
            raise RuntimeError(msg) from e

        launch_script_str  = '#!/bin/sh\n'
        launch_script_str += self.script_env(task)

        launch_script_str += '\nprof task_start\n'
        launch_script_str += '\n# Change to task sandbox\ncd %s\n' % sbox
        # FIXME: task_pre_exec should be LM specific
        if self._cfg.get('task_pre_exec'):
            launch_script_str += '\n'.join(self._cfg['task_pre_exec']) + '\n'

        launch_script_str += self.script_pre_exec(task)

        # prepare stdout/stderr
        stdout_file = td.get('stdout') or '%s.out' % tid
        stderr_file = td.get('stderr') or '%s.err' % tid
        task['stdout_file'] = os.path.join(sbox, stdout_file)
        task['stderr_file'] = os.path.join(sbox, stderr_file)
        launch_script_str += self.script_task_exec(
            launch_command, task['stdout_file'], task['stderr_file'])

        launch_script_str += self.script_post_exec(task)

        launch_script_str += '\n# Exit script with command return code\n'
        launch_script_str += 'prof task_stop\n'
        launch_script_str += 'echo $RETVAL > ../run_done/$RP_TASK_ID\n'

        with open(slots_fname, 'w') as fd:
            fd.write('\n%s\n' % pprint.pformat(task['slots']))

        with open(launch_script_name, 'w') as fd:
            fd.write(launch_script_str)
        # done writing to launch script, get it ready for execution
        st = os.stat(launch_script_name)
        os.chmod(launch_script_name, st.st_mode | stat.S_IEXEC)
        self._log.info('Launching task %s via %s', tid, launch_script_name)

        self._prof.prof('exec_start', uid=tid)
        os.system('cp %s run_queue/' % launch_script_name)
        self._prof.prof('exec_ok', uid=tid)

        task['proc'] = 0
        self._watch_queue.put(task)

    # --------------------------------------------------------------------------
    # Iterate over all running tasks, check their status, and decide on the
    # next step.  Also check for a requested cancellation for the tasks.
    def _check_running(self):

        action = 0
        for task in self._tasks_to_watch:

            tid = task['uid']

            try:
                if not os.path.exists('run_done/%s' % tid):
                    if tid in self._tasks_to_terminate:
                        exit_code = -1
                    else:
                        ret = ru.sh_callout('tail -n5 %s' % task['stderr_file'])
                        if 'Caught signal Terminated' not in ret[0]:
                            continue
                        exit_code = -1
                        self._tasks_to_terminate.extend(
                            [x['uid'] for x in self._tasks_to_watch])
                else:
                    os.system('mv run_done/%s run_final/%s' % (tid, tid))
                    with open('run_final/%s' % tid, 'r') as fd:
                        exit_code = int(fd.read().strip())
            except Exception as e:
                self._log.exception('ERROR during state check: %s' % e)
                continue

            if exit_code is None:
                # Process is still running

                if tid in self._tasks_to_cancel:

                    self._prof.prof('exec_cancel_start', uid=tid)
                    action += 1
                    with self._cancel_lock:
                        self._tasks_to_cancel.remove(tid)
                    self._prof.prof('exec_cancel_stop', uid=tid)

                    self._prof.prof('unschedule_start', uid=tid)
                    self.publish(rpc.AGENT_UNSCHEDULE_PUBSUB, task)
                    self.advance(task, rps.CANCELED, publish=True, push=False)

                    # we don't need to watch canceled tasks
                    self._tasks_to_watch.remove(task)
            else:

                self._prof.prof('exec_stop', uid=tid)

                # we have a valid return code -- task is final
                action += 1
                self._log.info('Task %s has return code %s.', tid, exit_code)

                task['exit_code'] = exit_code

                # Free the Slots, Flee the Flots, Ree the Frots!
                self._tasks_to_watch.remove(task)
                self._prof.prof('unschedule_start', uid=tid)
                self.publish(rpc.AGENT_UNSCHEDULE_PUBSUB, task)

                if exit_code != 0:
                    # The task failed - fail after staging output
                    task['target_state'] = rps.FAILED

                else:
                    # The task finished cleanly, see if we need to deal with
                    # output data.  We always move to stage out, even if there
                    # are no directives -- at the very least, we'll upload
                    # stdout/stderr
                    task['target_state'] = rps.DONE

                self.advance(task, rps.AGENT_STAGING_OUTPUT_PENDING,
                                   publish=True, push=True)

        return action

# ------------------------------------------------------------------------------
