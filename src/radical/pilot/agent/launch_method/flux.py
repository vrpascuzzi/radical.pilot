
__copyright__ = "Copyright 2016, http://radical.rutgers.edu"
__license__   = "MIT"

import os
import time
import threading       as mt
import subprocess      as sp

import radical.utils   as ru

from .base import LaunchMethod


# ------------------------------------------------------------------------------
#
class Flux(LaunchMethod):

    # --------------------------------------------------------------------------
    #
    def __init__(self, name, cfg, session):

        LaunchMethod.__init__(self, name, cfg, session)


    # --------------------------------------------------------------------------
    #
    @classmethod
    def rm_config_hook(cls, name, cfg, rm, logger, profiler):

        profiler.prof('flux_start')

        flux = ru.which('flux')
        if not flux:
            raise Exception("Couldn't find flux")

        try:
            import sys
            print(sys.path)
            import flux
        except:
            raise Exception("Couldn't import flux")

        with open('flux_launcher.sh', 'w') as fout:
            fout.write('''#/bin/sh
export PMIX_MCA_gds='^ds12,ds21'
echo "flux env; echo OK; while true; do echo ok; sleep 1; done" | \\
jsrun -a 1 -c ALL_CPUS -g ALL_GPUS -n %d --bind none --smpiargs '-disable_gpu_hooks' \\
flux start -o,-v,-S,log-filename=flux.log
''' % len(rm.node_list))

        cmd  = '/bin/sh ./flux_launcher.sh'
        proc = sp.Popen(cmd, shell=True,
                        stdin=sp.PIPE, stdout=sp.PIPE, stderr=sp.STDOUT)

        logger.debug('=== %s', cmd)

        flux_env = dict()
        while True:

            line = ru.as_string(proc.stdout.readline().strip())
            logger.debug('=== %s', line)

            if line.startswith('export '):
                k, v = line.split(' ', 1)[1].strip().split('=', 1)
                flux_env[k] = v.strip('"')
                logger.debug('%s = %s' % (k, v.strip('"')))

            elif line == 'OK':
                break


        assert('FLUX_URI' in flux_env)

        # TODO check perf implications
        flux_url             = flux_env['FLUX_URI']
      # flux_url             = ru.Url(flux_url)
      # flux_url.host        = ru.get_hostname()
      # flux_url.scheme      = 'ssh'
        flux_env['FLUX_URI'] = str(flux_url)

        profiler.prof('flux_started')

        # ----------------------------------------------------------------------
        def _watch_flux(proc):

            try:

                while True:

                    line = ru.as_string(proc.stdout.readline().strip())
                    logger.debug('=== %s', line)

            except Exception as e:
                logger.exception('ERROR: flux stopped?')
                # FIXME: trigger termination
                raise
        # ----------------------------------------------------------------------

        flux_watcher = mt.Thread(target=_watch_flux, args=[proc])
        flux_watcher.daemon = True
        flux_watcher.start()

        logger.info("flux startup successful: [%s]", flux_env['FLUX_URI'])

        lm_info = {'flux_env': flux_env,
                   'flux_pid': proc.pid}

        return lm_info


    # --------------------------------------------------------------------------
    #
    def _configure(self):

        pass


  # # --------------------------------------------------------------------------
  # #
  # def construct_command(self, unit, launch_script_hop=None):
  #
  #     uid          = unit['uid']
  #     cud          = unit['description']
  #     procs        = cud['cpu_processes']
  #     cpn          = cud['cpu_threads']
  #     gpn          = cud['gpu_processes']
  #     task_exec    = cud['executable']
  #     task_args    = cud.get('arguments') or list()
  #     task_sandbox = unit['unit_sandbox_path']
  #
  #     self._log.debug('prep %s', uid)
  #
  #     return spec, None


# ------------------------------------------------------------------------------

