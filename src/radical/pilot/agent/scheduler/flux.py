
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
    def work(self, units):

        # overload the base class work method

        from flux import job as flux_job

        self.advance(units, rps.AGENT_SCHEDULING, publish=True, push=False)

        for unit in units:

          # # FIXME: transfer from executor
          # self._cu_environment = self._populate_cu_environment()

            uid  = unit['uid']
            cud  = unit['description']
            sbox = unit['unit_sandbox_path']

            ru.rec_makedir(sbox)

            env = dict()
            env['RADICAL_BASE']      = self._pwd
            env['RP_SESSION_ID']     = self._cfg['sid']
            env['RP_PILOT_ID']       = self._cfg['pid']
            env['RP_AGENT_ID']       = self._cfg['aid']
            env['RP_SPAWNER_ID']     = self.uid
            env['RP_UNIT_ID']        = uid
            env['RP_UNIT_NAME']      = cud.get('name')
            env['RP_GTOD']           = self.gtod
            env['RP_PROF']           = self.prof
          # env['RP_TMP']            = self._cu_tmp
            env['RP_UNIT_SANDBOX']   = sbox
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

            script = '%s/%s.sh' % (sbox, uid)
            with open(script, 'w') as fout:
                fout.write('#!/bin/sh\n')

                fout.write('\n# change to sandbox\n\n')
                fout.write('cd %s\n' % sbox)

                fout.write('\n# pre exec\n\n')
                for pe in cud['pre_exec']:
                    fout.write('%s\n' % pe)

                fout.write('\n# exec\n\n')
                fout.write('%s %s > %s.out 2> %s.err\n' 
                          % (cud['executable'], ' '.join(cud['arguments']),
                             uid, uid))

                fout.write('\n# post exec\n\n')
                for pe in cud['post_exec']:
                    fout.write('%s\n' % pe)

            spec = {'version'  : 1,
                    'resources': [{
                        'type'   : 'node',
                        'count'  : 1,
                        'with'   : [{
                            'type' : 'slot',
                            'count': cud['cpu_processes'],
                            'label': 'task_slot',
                            'with' : [{
                                'type' : 'core',
                                'count': cud['cpu_threads']
                                }, {
                                'type' : 'gpu',
                                'count': cud['gpu_processes']
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
                            'duration'   : 1.0,
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

          # js = flux_job.JobspecV1.from_command(['/bin/date'])

            js.stdout = '%s/%s.js.out' % (sbox, uid)
            js.stderr = '%s/%s.js.err' % (sbox, uid)

            js.setattr_shell_option("output.stdout.type", "file")
            js.setattr_shell_option("output.stderr.type", "file")
            js.setattr_shell_option("output.stdout.path", '%s/%s.js.out' % (sbox, uid))
            js.setattr_shell_option("output.stderr.path", '%s/%s.js.err' % (sbox, uid))

            js.setattr_shell_option("output.stdout.label", True)
            js.setattr_shell_option("output.stderr.label", True)

            self._log.debug('=== js: %s', js.dumps())

            jid = flux_job.submit(self._flux, js, debug=True)
            unit['flux_id'] = jid

            # publish without state changes - those are retroactively applied
            # based on flux event timestamps.
            # TODO: apply some bulking, submission is not really fast.
            #       But at the end performance is determined by flux now, so
            #       communication only affects timelyness of state updates.
            self._q.put(unit)


  # # --------------------------------------------------------------------------
  # #
  # def _populate_cu_environment(self):
  #
  #     import tempfile
  #
  #     self.gtod   = "%s/gtod" % self._pwd
  #     self.tmpdir = tempfile.gettempdir()
  #
  #     # if we need to transplant any original env into the CU, we dig the
  #     # respective keys from the dump made by bootstrap_0.sh
  #     self._env_cu_export = dict()
  #     if self._cfg.get('export_to_cu'):
  #         with open('env.orig', 'r') as f:
  #             for line in f.readlines():
  #                 if '=' in line:
  #                     k,v = line.split('=', 1)
  #                     key = k.strip()
  #                     val = v.strip()
  #                     if key in self._cfg['export_to_cu']:
  #                         self._env_cu_export[key] = val
  #
  #
  # # --------------------------------------------------------------------------
  # #
  # def _populate_cu_environment(self):
  #     """Derive the environment for the cu's from our own environment."""
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

