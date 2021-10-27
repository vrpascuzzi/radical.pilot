
__copyright__ = "Copyright 2013-2016, http://radical.rutgers.edu"
__license__   = "MIT"


import os
import stat
import time
import queue
import threading as mt
import subprocess

import radical.utils as ru
import radical.pilot as rp

from ...  import utils     as rpu
from ...  import states    as rps
from ...  import constants as rpc

from ..   import LaunchMethod

from .base           import AgentExecutingComponent


# ------------------------------------------------------------------------------
#
class MPIFUNCS(AgentExecutingComponent) :

    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg, session):

        AgentExecutingComponent.__init__ (self, cfg, session)

        self._collector = None
        self._terminate = mt.Event()


    # --------------------------------------------------------------------------
    #
    def initialize(self):

        self._pwd = os.getcwd()
        self.gtod = "%s/gtod" % self._pwd

        self.register_input(rps.AGENT_EXECUTING_PENDING,
                            rpc.AGENT_EXECUTING_QUEUE, self.work)

        self.register_output(rps.AGENT_STAGING_OUTPUT_PENDING,
                             rpc.AGENT_STAGING_OUTPUT_QUEUE)

        self.register_publisher (rpc.AGENT_UNSCHEDULE_PUBSUB)
        self.register_subscriber(rpc.CONTROL_PUBSUB, self.command_cb)

        req_cfg = ru.read_json('funcs_req_queue.cfg')
        res_cfg = ru.read_json('funcs_res_queue.cfg')

        self._req_queue = ru.zmq.Putter('funcs_req_queue', req_cfg['put'])
        self._res_queue = ru.zmq.Getter('funcs_res_queue', res_cfg['get'])

        self._cancel_lock     = ru.RLock()
        self._tasks_to_cancel = list()
        self._tasks_to_watch  = list()
        self._watch_queue     = queue.Queue ()

        self._pid = self._cfg['pid']

        # run watcher thread
        self._collector = mt.Thread(target=self._collect)
        self._collector.daemon = True
        self._collector.start()

        # we need to launch the executors on all nodes, and use the
        # agent_launcher for that
        self._launcher = LaunchMethod.create(
                name    = self._cfg.get('agent_launch_method'),
                cfg     = self._cfg,
                session = self._session)

        # get address of control pubsub
        fname   = '%s/%s.cfg' % (self._cfg.path, rpc.CONTROL_PUBSUB)
        ctl_cfg = ru.read_json(fname)

        # now run the func launcher on all nodes
        ve  = os.environ.get('VIRTUAL_ENV',  '')
        exe = ru.which('radical-pilot-agent-funcs2-mpi')
        sbox = os.getcwd()

        if not exe:
            exe = '%s/rp_install/bin/radical-pilot-agent-funcs2-mpi' % self._pwd


        # Since we know that every task is a multinode, we take half of the nodes and
        # spawn executros on them, then the rest of the nodes (the other half) 
        # will be utilized by the mpi_workers inside every executor.
        # So the mpi worker will see 2 nodes for every task and occupy it

        #breakdown = self._cfg['rm_info']['node_list']

        if self._cfg['resource'].startswith('local'):
            pass

        else:
            spl = int(len(self._cfg['rm_info']['node_list'])/2)  
            #breakdown = self._cfg['rm_info']['node_list'][:spl]

        #for idx, node in enumerate(breakdown):
        for idx, node in enumerate(self._cfg['rm_info']['node_list']):
            uid   = 'func_exec.%04d' % idx
            pwd   = '%s/%s' % (self._pwd, uid)
            funcs = {'uid'        : uid,
                     'description': {'executable'   : exe,
                                     'arguments'    : [pwd, ve],
                                     'cpu_processes': 4,
                                     'environment'  : [],
                                    },
                     'task_sandbox_path': "./",                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            
                     'slots'      : {'nodes'        : [{'name'     : node[0],
                                                        'uid'      : node[1],
                                                        'core_map' : [[0], [1], [2], [3]],
                                                        'gpus'  : []
                                                       }]
                                    },
                     'cfg'        : {'req_get'      : req_cfg['get'],
                                     'res_put'      : res_cfg['put'],
                                     'ctrl'         : ctl_cfg['sub']
                                    }
                    }
            self._spawn(self._launcher, funcs)


    # --------------------------------------------------------------------------
    #
    def command_cb(self, topic, msg):

        self._log.info('command_cb [%s]: %s', topic, msg)

        cmd = msg['cmd']
        arg = msg['arg']

        if cmd == 'cancel_tasks':

            self._log.info("cancel_tasks command (%s)" % arg)
            with self._cancel_lock:
                self._tasks_to_cancel.extend(arg['uids'])

        return True


    # --------------------------------------------------------------------------
    #
    def _spawn(self, launcher, funcs):

        # NOTE: see documentation of funcs['sandbox'] semantics in the Task
        #       class definition.
        sandbox = '%s/%s'     % (self._pwd, funcs['uid'])
        fname   = '%s/%s.sh'  % (sandbox,   funcs['uid'])
        cfgname = '%s/%s.cfg' % (sandbox,   funcs['uid'])
        descr   = funcs['description']

        rpu.rec_makedir(sandbox)
        ru.write_json(funcs.get('cfg'), cfgname)

        launch_cmd, hop_cmd = launcher.construct_command(funcs, fname)

        if hop_cmd : cmdline = hop_cmd
        else       : cmdline = fname

        with open(fname, "w") as fout:

            fout.write('#!/bin/sh\n\n')

            # Create string for environment variable setting
            fout.write('export RP_SESSION_ID="%s"\n'      % self._cfg['sid'])
            fout.write('export RP_PILOT_ID="%s"\n'        % self._cfg['pid'])
            fout.write('export RP_AGENT_ID="%s"\n'        % self._cfg['aid'])
            fout.write('export RP_SPAWNER_ID="%s"\n'      % self.uid)
            fout.write('export RP_FUNCS_ID="%s"\n'        % funcs['uid'])
            fout.write('export RP_GTOD="%s"\n'            % self.gtod)
            fout.write('export RP_TMP="%s"\n'             % self._task_tmp)

            if self._cfg['resource'].startswith('local'):
                fout.write('export PILOT_SCHEMA="%s"\n'     % 'LOCAL')
            else:
                fout.write('export PILOT_SCHEMA="%s"\n'     % 'REMOTE')
                fout.write('export SLURM_NODELIST="%s"\n'     % os.environ['SLURM_NODELIST'])
                fout.write('export SLURM_CPUS_ON_NODE="%s"\n' % os.environ['SLURM_CPUS_ON_NODE'])
            

            # also add any env vars requested in the task description
            if descr.get('environment', []):
                for key,val in descr['environment'].items():
                    fout.write('export "%s=%s"\n' % (key, val))

            fout.write('\n%s\n\n' % launch_cmd)
            fout.write('RETVAL=$?\n')
            fout.write("exit $RETVAL\n")

        # done writing to launch script, get it ready for execution.
        st = os.stat(fname)
        os.chmod(fname, st.st_mode | stat.S_IEXEC)

        fout = open('%s/%s.out' % (sandbox, funcs['uid']), "w")
        ferr = open('%s/%s.err' % (sandbox, funcs['uid']), "w")

        self._prof.prof('exec_start', uid=funcs['uid'])
        # we really want to use preexec_fn:
        # pylint: disable=W1509
        funcs['proc'] = subprocess.Popen(args       = cmdline,
                                         executable = None,
                                         stdin      = None,
                                         stdout     = fout,
                                         stderr     = ferr,
                                         preexec_fn = os.setsid,
                                         close_fds  = True,
                                         shell      = True,
                                         cwd        = sandbox)

        self._prof.prof('exec_ok', uid=funcs['uid'])


    # --------------------------------------------------------------------------
    #
    def work(self, tasks):

        if not isinstance(tasks, list):
            tasks = [tasks]

        self.advance(tasks, rps.AGENT_EXECUTING, publish=True, push=False)

        for task in tasks:
            assert(task['description']['cpu_process_type'] == 'MPI_FUNC')
            self._req_queue.put(task)


    # --------------------------------------------------------------------------
    #
    def _collect(self):

        while not self._terminate.is_set():

            # pull tasks from "funcs_out_queue"
            tasks = self._res_queue.get_nowait(1000)

            if tasks:

                for task in tasks:
                    task['target_state'] = task['state']
                    task['pilot']        = self._pid

                  # self._log.debug('got %s [%s] [%s] [%s]',
                  #                 task['uid'],    task['state'],
                  #                 task['stdout'], task['stderr'])

                self.advance(tasks, rps.AGENT_STAGING_OUTPUT_PENDING,
                             publish=True, push=True)
            else:
                time.sleep(0.1)


# ------------------------------------------------------------------------------
