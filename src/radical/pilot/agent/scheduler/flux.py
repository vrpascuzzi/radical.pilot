
__copyright__ = "Copyright 2017, http://radical.rutgers.edu"
__license__   = "MIT"


import os
import json

import radical.utils        as ru

from ...   import states    as rps
from ...   import constants as rpc

from ..launch_method import LaunchMethod
from .base           import AgentSchedulingComponent


# ------------------------------------------------------------------------------
#
class Flux(AgentSchedulingComponent):
    '''
    Pass all scheduling and execution control to Flux, and leave it to the
    execution component to collect state updates.
    '''

    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg, session):

        self.nodes = None
        self._pwd  = os.getcwd()

        AgentSchedulingComponent.__init__(self, cfg, session)


    # --------------------------------------------------------------------------
    #
    def _configure(self):

        self.gtod   = "%s/gtod" % self._pwd
        self.prof   = "%s/prof" % self._pwd

        flux_env = self._cfg['rm_info']['lm_info']['flux_env']
        for k,v in flux_env.items():
            os.environ[k] = v

        import flux
        from   flux import job

        flux_url   = flux_env['FLUX_URI']
        self._flux = flux.Flux(url=flux_url)

        # don't advance tasks via the component's `advance()`, but push them
        # toward the executor *without state change* - state changes are
        # performed in retrospect by the executor, based on the scheduling and
        # execution events collected from Flux.
        qname   = rpc.AGENT_EXECUTING_QUEUE
        fname   = '%s/%s.cfg' % (self._cfg.path, qname)
        cfg     = ru.read_json(fname)
        self._q = ru.zmq.Putter(qname, cfg['put'])

        # create job spec via the flux LM
        self._lm = LaunchMethod.create(name    = 'FLUX',
                                       cfg     = self._cfg,
                                       session = self._session)


    # --------------------------------------------------------------------------
    #
    def work(self, tasks):

        # overload the base class work method

        from flux import job as flux_job

        self.advance(tasks, rps.AGENT_SCHEDULING, publish=True, push=False)

        for task in ru.as_list(tasks):

            try:

                ru.write_json('/tmp/task.json', task)

              # # FIXME: transfer from executor
              # self._cu_environment = self._populate_cu_environment()

                uid  = task['uid']
                td   = task['description']
                sbox = task['task_sandbox_path']

                ru.rec_makedir(sbox)

                spec = self._task_to_flux(uid, td, sbox)

                jid = flux_job.submit(self._flux, spec, debug=True)
                task['flux_id']     = jid
                task['flux_states'] = list()

                # publish without state changes - those are retroactively applied
                # based on flux event timestamps.
                # TODO: apply some bulking, submission is not really fast.
                #       But at the end performance is determined by flux now, so
                #       communication only affects timelyness of state updates.
                self._q.put(task)

            except Exception as e:
                self._log.exception('flux submission failed')
                task['target_state'] = rps.FAILED
                self.advance(task, rps.AGENT_STAGING_OUTPUT_PENDING,
                                   publish=True, push=True)


    # --------------------------------------------------------------------------
    #
    def _task_to_flux(self, uid, td, sbox):
        '''

        The translator between RP and FLUX job description supports the
        following keys:

            - NAME                  :
            - EXECUTABLE            : tasks.command
            - ARGUMENTS             : tasks.command
            - ENVIRONMENT           : attributes.system.environment
            - SANDBOX               : attributes.system.cwd

            - CPU_PROCESSES         : resources.slot.count ?
            - CPU_PROCESS_TYPE      : ?
            - CPU_THREADS           : resources.slot.core.count
            - CPU_THREAD_TYPE       : ?

            - GPU_PROCESSES         : resource.slot.gpu.count
            - GPU_PROCESS_TYPE      : ?
            - GPU_THREADS           : -/-
            - GPU_THREAD_TYPE       : -/-

            - LFS_PER_PROCESS       : ?
            - MEM_PER_PROCESS       : ?

            - INPUT_STAGING         : n/a
            - OUTPUT_STAGING        : n/a
            - PRE_EXEC              : n/a
            - POST_EXEC             : n/a
            - KERNEL                : RP specific
            - CLEANUP               : n/a
            - PILOT                 : RP specific
            - STDOUT                : n/a
            - STDERR                : n/a
            - RESTARTABLE           : RP specific (workflow layer)
            - TAGS                  : RP specific (co-scheduling, priority, ...)
            - METADATA              : RP specific
        '''

        from flux import job as flux_job

        env = dict()
        env['RADICAL_BASE']      = self._pwd
        env['RP_SESSION_ID']     = self._cfg['sid']
        env['RP_PILOT_ID']       = self._cfg['pid']
        env['RP_AGENT_ID']       = self._cfg['aid']
        env['RP_SPAWNER_ID']     = self.uid
        env['RP_TASK_ID']        = uid
        env['RP_TASK_NAME']      = td.get('name')
        env['RP_GTOD']           = self.gtod
        env['RP_PROF']           = self.prof
      # env['RP_TMP']            = self._cu_tmp
        env['RP_TASK_SANDBOX']   = sbox
        env['RP_PILOT_SANDBOX']  = self._pwd
        env['RP_PILOT_STAGING']  = self._pwd

        if self._prof.enabled:
            env['RP_PROF_TGT']   = '%s/%s.prof' % (sbox, uid)

        else:
            env['RP_PROF_TGT']   = ''

        if 'RP_APP_TUNNEL' in os.environ:
            env['RP_APP_TUNNEL'] = os.environ['RP_APP_TUNNEL']


        # FLUX does not support stdio redirection (?), so we wrap the
        # command into a small shell script to obtain that.  That also
        # includes pre-exec and post-exec.
        # NOTE:  this implies that pre- and post-exec are executed
        #        *per rank*, which can put significant strain on the
        #        file system.
        # FIXME: use env isolation
        # FLUX?: stdout / stderr
        # FLUX?: pre_exec / post_exec

        script = '%s/%s.sh' % (sbox, uid)
        with open(script, 'w') as fout:
            fout.write('#!/bin/sh\n')

            fout.write('\n# change to sandbox\n\n')
            fout.write('cd %s\n' % sbox)

            fout.write('\n# pre exec\n\n')
            for pe in td['pre_exec']:
                fout.write('%s\n' % pe)

            fout.write('\n# exec\n\n')
          # fout.write('%s %s > %s.out 2> %s.err\n'
          #           % (td['executable'], ' '.join(td['arguments']),
          #              uid, uid))
            fout.write('%s %s\n' % (td['executable'],
                                    ' '.join(td['arguments'])))

            fout.write('\n# post exec\n\n')
            for pe in td['post_exec']:
                fout.write('%s\n' % pe)

        spec = {'version'  : 1,
                'resources': [{
                    'type'   : 'node',
                    'count'  : 1,
                    'with'   : [{
                        'type' : 'slot',
                        'count': td['cpu_processes'],
                        'label': 'task_slot',
                        'with' : [{
                            'type' : 'core',
                            'count': td['cpu_threads']
                          # }, {
                          # 'type' : 'gpu',
                          # 'count': td['gpu_processes']
                            }]
                        }]
                    }],
                'tasks': [{
                    'command': ['/bin/sh', script],
                    'slot'   : 'task_slot',
                    'count'  : {
                        'per_slot': 1
                        }
                    }],
                'attributes': {
                    'system'       : {
                        'duration'   : 0.0,
                        'cwd'        : sbox,
                        'environment': env
                        }
                    }
                }

        ru.write_json(spec, '%s/%s.flux' % (sbox, uid))
        js = flux_job.JobspecV1(tasks=spec['tasks'],
                                resources=spec['resources'],
                                version=spec['version'],
                                attributes=spec['attributes'])

        # FLUX: not part of V1 spec?
        # FLUX: does not work?
        js.stdout = '%s/%s.out' % (sbox, uid)
        js.stderr = '%s/%s.err' % (sbox, uid)

      # import pprint
      # self._log.debug('=== js: %s', pprint.pformat(js.dumps()))
      #
      # # do a sanity check
      # td2 = self._flux_to_task(js)
      # ru.write_json('cud_1.json', td)
      # ru.write_json('cud_2.json', td2)

        return js


    # --------------------------------------------------------------------------
    #
    def _flux_to_task(self, js):

        from ...task_description import TaskDescription
        td = TaskDescription()

        td.executable  = js.tasks[0]['command'][0]
        td.arguments   = js.tasks[0]['command'][1:]
        td.environment = js.environment
        td.sandbox     = js.cwd
        td.stdout      = js.stdout
        td.stderr      = js.stderr
        td.stderr      = js.stderr

        import pprint
        n_procs   = 1
        n_gpus    = 0
        n_threads = 1
        for p, o, c in js.resource_walk():
            if   o['type'] == 'slot': n_procs   = c
            elif o['type'] == 'core': n_threads = c
            elif o['type'] == 'gpu' : n_gpus    = c

        td.cpu_processes = n_procs
        td.cpu_threads   = n_threads
        td.gpu_processes = n_gpus

        return td.as_dict()

  # # --------------------------------------------------------------------------
  # #
  # def _populate_task_environment(self):
  #
  #     import tempfile
  #
  #     self.gtod   = "%s/gtod" % self._pwd
  #     self.tmpdir = tempfile.gettempdir()
  #
  #     # if we need to transplant any original env into the Task, we dig the
  #     # respective keys from the dump made by bootstrap_0.sh
  #     self._env_task_export = dict()
  #     if self._cfg.get('export_to_task'):
  #         with open('env.orig', 'r') as f:
  #             for line in f.readlines():
  #                 if '=' in line:
  #                     k,v = line.split('=', 1)
  #                     key = k.strip()
  #                     val = v.strip()
  #                     if key in self._cfg['export_to_task']:
  #                         self._env_task_export[key] = val
  #
  #
  # # --------------------------------------------------------------------------
  # #
  # def _populate_task_environment(self):
  #     """Derive the environment for the t's from our own environment."""
  #
  #     # Get the environment of the agent
  #     new_env = copy.deepcopy(os.environ)
  #
  #     #
  #     # Mimic what virtualenv's "deactivate" would do
  #     #
  #     old_path = new_env.pop('_OLD_VIRTUAL_PATH', None)
  #     if old_path:
  #         new_env['PATH'] = old_path
  #
  #     old_ppath = new_env.pop('_OLD_VIRTUAL_PYTHONPATH', None)
  #     if old_ppath:
  #         new_env['PYTHONPATH'] = old_ppath
  #
  #     old_home = new_env.pop('_OLD_VIRTUAL_PYTHONHOME', None)
  #     if old_home:
  #         new_env['PYTHON_HOME'] = old_home
  #
  #     old_ps = new_env.pop('_OLD_VIRTUAL_PS1', None)
  #     if old_ps:
  #         new_env['PS1'] = old_ps
  #
  #     new_env.pop('VIRTUAL_ENV', None)
  #
  #     # Remove the configured set of environment variables from the
  #     # environment that we pass to Popen.
  #     for e in list(new_env.keys()):
  #         for r in self._lm.env_removables:
  #             if e.startswith(r):
  #                 new_env.pop(e, None)
  #
  #     return new_env


# ------------------------------------------------------------------------------

