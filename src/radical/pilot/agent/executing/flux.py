# pylint: disable=import-error

__copyright__ = 'Copyright 2013-2020, http://radical.rutgers.edu'
__license__   = 'MIT'


import os
import time
import json
import queue
import errno

import threading     as mt
import radical.utils as ru

from ...  import states    as rps
from ...  import constants as rpc

from .base import AgentExecutingComponent


# ------------------------------------------------------------------------------
#
class Flux(AgentExecutingComponent) :

    # translate Flux states changes to RP state transitions (task
    # arrives here in `rps.AGENT_SCHEDULING` state
    #
    #    'NEW'      -->  `rps.AGENT_SCHEDULING`   (no transition)
    #    'DEPEND'   -->  `rps.AGENT_SCHEDULING`   (no transition)
    #    'SCHED'    -->  `rps.AGENT_EXECUTING_PENDING`
    #    'RUN'      -->  `rps.AGENT_EXECUTING`
    #    'CLEANUP'  -->  `rps.AGENT_EXECUTING`    (no transition)
    #    'INACTIVE' -->  `rps.AGENT_STAGING_OUTPUT_PENDING`
    #
    _event_map = {'NEW'     : None,   # rps.AGENT_SCHEDULING,
                  'DEPEND'  : None,
                  'SCHED'   : rps.AGENT_EXECUTING_PENDING,
                  'RUN'     : rps.AGENT_EXECUTING,
                  'CLEANUP' : None,
                  'INACTIVE': rps.AGENT_STAGING_OUTPUT_PENDING,
                 }

    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg, session):

        AgentExecutingComponent.__init__(self, cfg, session)


    # --------------------------------------------------------------------------
    #
    def initialize(self):
        '''
        This components has 3 strands of activity (threads):

          - the main thread listens for incoming tasks from the scheduler, and
            pushes them toward the watcher thread;
          - an event listener thread listens for flux events which signify task
            state updates, and pushes those events also to the watcher thread;
          - the watcher thread matches events and tasks, enacts state updates,
            and pushes completed tasks toward output staging.

        NOTE: we get tasks in *AGENT_SCHEDULING* state, and enact all
              further state changes in this component.
        '''

        # thread termination signal
        self._term    = mt.Event()

        # need two queues, for tasks and events
        self._task_q  = queue.Queue()
        self._event_q = queue.Queue()

        # run listener thread
        self._listener_setup  = mt.Event()
        self._listener        = mt.Thread(target=self._listen)
        self._listener.daemon = True
        self._listener.start()

        # run watcher thread
        self._watcher_setup  = mt.Event()
        self._watcher        = mt.Thread(target=self._watch)
        self._watcher.daemon = True
        self._watcher.start()

        # main thread waits for tasks to arrive from the scheduler
        self.register_input(rps.AGENT_SCHEDULING,
                            rpc.AGENT_EXECUTING_QUEUE, self.work)

        # also listen on the command channel for task cancellation requests
        self.register_subscriber(rpc.CONTROL_PUBSUB, self.command_cb)

        # wait for some time to get watcher and listener initialized
        start = time.time()
        while time.time() - start < 10.0:
            if self._watcher_setup.is_set() and \
               self._listener_setup.is_set():
                break

        assert(self._watcher_setup.is_set())
        assert(self._listener_setup.is_set())


    # --------------------------------------------------------------------------
    #
    def command_cb(self, topic, msg):

        self._log.info('command_cb [%s]: %s', topic, msg)

        cmd = msg['cmd']
      # arg = msg['arg']

        if cmd == 'cancel_tasks':

            # FIXME: clarify how to cancel tasks in Flux
            pass

        return True


    # --------------------------------------------------------------------------
    #
    def work(self, tasks):

        self._task_q.put(ru.as_list(tasks))

        if self._term.is_set():
            self._log.warn('threads triggered termination')
            self.stop()


    # --------------------------------------------------------------------------
    #
    def _get_flux_handle(self):

        import flux

        flux_uri = self._cfg['rm_info']['lm_info']['flux_env']['FLUX_URI']
        return flux.Flux(url=flux_uri)


    # --------------------------------------------------------------------------
    #
    def _listen(self):

        flux_handle = None

        try:
            # thread local initialization
            flux_handle = self._get_flux_handle()

            flux_handle.event_subscribe('job-state')

            # FIXME: how tot subscribe for task return code information?
            def _flux_cb(self, *args, **kwargs):
                self._log.debug('==== flux cb    %s' % [args, kwargs])

            # signal successful setup to main thread
            self._listener_setup.set()

            while not self._term.is_set():

                # `recv()` will raise an `OSError(errno=EIO)` exception once the
                # flux instance terminated.  That is to be expected during
                # termination, and we'll bail out peacefully in that case.
                try:
                    event = flux_handle.event_recv()
                except OSError as e:
                    if e.errno == errno.EIO and self._term.is_set():
                        self._log.debug('lost flux during termination')
                        break
                    else:
                        raise RuntimeError('list flux connection') from e

                self._log.debug('==== flux event %s' % [event.payload])

                if 'transitions' not in event.payload:
                    self._log.warn('unexpected flux event: %s' %
                                    event.payload)
                    continue

                transitions = ru.as_list(event.payload['transitions'])

                self._event_q.put(transitions)


        except Exception:

            self._log.exception('=== Error in listener loop')

            if flux_handle:
                flux_handle.event_unsubscribe('job-state')

            self._term.set()


    # --------------------------------------------------------------------------
    #
    def handle_events(self, flux_handle, task, events):
        '''
        Return `True` on final events so that caller can clean caches.
        Note that this relies on Flux events to arrive in order
        (or at least in ordered bulks).
        '''

        import flux.job as fjob

        uid = task['uid']
        for event in events:

            self._log.debug('=== event: %s',  event)

            flux_id    = event[0]
            flux_state = event[1]
            task['flux_states'].append(flux_state)

            # we don't need to do anything on most events and actually only wait
            # for task completion (`INACTIVE`)
            state = self._event_map[flux_state]

            if state is None:
                # ignore this state transition
                self._log.debug('ignore flux event %s:%s' %
                                (task['uid'], flux_state))
                continue

            self._log.debug('handle flux event %s:%s' %
                            (task['uid'], flux_state))

            # on `INACTIVE`, the task is always completed.  It might not have
            # executed though, so we set the default target state to `FAILED`
            # and only switch to `DONE` if we find an exit code `0`.
            task['target_state'] = rps.FAILED

            # task is completed - sift through the job's event log to see
            # what happened to it and determine return code
            for event in fjob.event_watch(flux_handle, flux_id, 'eventlog'):

                self._log.debug('==== el: %s', event)
                evt = event.name
                ts  = event.timestamp
                ctx = event.context
                msg = str(ctx)

                if evt == 'alloc':
                    # task got scheduled (resources assigned)
                    # FIXME: maybe `alloc` is `schedule_start`, and
                    #       `debug.start-request` maps to `schedule_ok`?
                    self._prof.prof('schedule_ok', comp='flux',
                                    state=rps.AGENT_SCHEDULING,
                                    uid=uid, ts=ts, msg=msg)
                    self.advance(task, rps.AGENT_EXECUTING_PENDING, ts=ts,
                                 publish=True, push=False)

                elif evt == 'start':
                    # task processes get started
                    self.advance(task, rps.AGENT_EXECUTING, ts=ts,
                                 publish=True, push=False)
                    self._prof.prof('exec_start', comp='flux',
                                    state=rps.AGENT_EXECUTING,
                                    uid=uid, ts=ts, msg=msg)
                    self._prof.prof('exec_ok', comp='flux',
                                    state=rps.AGENT_EXECUTING,
                                    uid=uid, ts=ts, msg=msg)
                    self._prof.prof('cu_start', comp='flux',
                                    state=rps.AGENT_EXECUTING,
                                    uid=uid, ts=ts, msg=msg)
                    self._prof.prof('cu_start', comp='flux',
                                    state=rps.AGENT_EXECUTING,
                                    uid=uid, ts=ts, msg=msg)
                    self._prof.prof('cu_exec_start', comp='flux',
                                    state=rps.AGENT_EXECUTING,
                                    uid=uid, ts=ts, msg=msg)

                elif evt == 'finish':
                    # task processes completed - extract return code
                    retval = ctx.get('status')
                    if retval == 0:
                        # exit val is zero - task succeeded
                        task['target_state'] = rps.DONE
                    self._prof.prof('cu_exec_stop', comp='flux',
                                    state=rps.AGENT_EXECUTING,
                                    uid=uid, ts=ts, msg=msg)
                    self._prof.prof('cu_stop', comp='flux',
                                    state=rps.AGENT_EXECUTING,
                                    uid=uid, ts=ts, msg=msg)

                elif evt == 'debug.free-request':
                    # request task resources to be freed
                    self._prof.prof('unschedule', comp='flux',
                                    state=rps.AGENT_EXECUTING,
                                    uid=uid, ts=ts, msg=msg)

                elif evt == 'free':
                    # task resources have been freed
                    self._prof.prof('unschedule_ok', comp='flux',
                                    state=rps.AGENT_EXECUTING,
                                    uid=uid, ts=ts, msg=msg)

            # also fetch resource information (only works for tasks which were
            # actually executed)
            # FIXME: why don't we get this after `SCHED`?
            if 'RUN' in task['flux_states']:
                self._log.debug('=== flux_states: %s', task['flux_states'])
                payload = {'id'   : flux_id,
                           'keys' : ['R'],
                           'flags': 0}
                result  = flux_handle.rpc('job-info.lookup', payload).get()
                if 'R' in result:
                    r = json.loads(result['R'])

                  # {
                  #     'version': 1,
                  #     'starttime': 1603360891,
                  #     'expiration': 1603965691
                  #     'execution': {
                  #         'R_lite': [{
                  #             'rank': '0',
                  #             'node': 'rivendell',
                  #             'children': {
                  #                 'core': '2-7'
                  #                 }
                  #             }],
                  #     }
                  # }
                    assert(r['version'] == 1)

                    sl = {'cores_per_node': 8,
                          'gpus_per_node' : 2,
                          'mem_per_node'  : 0,
                          'lfs_per_node'  : {'path': '/tmp', 'size': 1024},
                          'lm_info'       : {'version_info': {
                                                'FORK': {'version': '0.42',
                                                         'version_detail': ''}}
                                            },
                          'nodes': list()}
                    for rank in r['execution']['R_lite']:
                        cores = self._parse_res(rank['children'].get('core'))
                        gpus  = self._parse_res(rank['children'].get('gpu'))
                        slot  = {
                            'lfs'     : {'path': '/tmp', 'size': 0},
                            'mem'     : 0,
                            'name'    : 'localhost',    # FIXME: rank['node'],
                            'uid'     : 'localhost_1',  # FIXME: rank['node'],
                            'core_map': [cores],
                            'gpu_map' : [gpus]
                            }
                        sl['nodes'].append(slot)

                    task['slots'] = sl

                    slots_fname = '%s/%s.sl' % (task['unit_sandbox_path'],
                                                task['uid'])
                    ru.write_json(slots_fname, task['slots'])

                    slots_fname = '%s/%s.slf' % (task['unit_sandbox_path'],
                                                 task['uid'])
                    ru.write_json(slots_fname, r)

                self._log.debug('=== resources: %s', result)

              # {'cores_per_node': 8,
              #  'gpus_per_node': 2,
              #  'lfs_per_node': {'path': '/tmp', 'size': 1024},
              #  'lm_info': {'version_info': {'FORK': {'version': '0.42',
              #                                        'version_detail': 'There is no spoon'}}},
              #  'mem_per_node': 0,
              #  'nodes': [{'core_map': [[0, 1, 2]],
              #             'gpu_map': [[0]],
              #             'lfs': {'path': '/tmp', 'size': 0},
              #             'mem': 0,
              #             'name': 'localhost',
              #             'uid': 'localhost_0'},
              #            {'core_map': [[3, 4, 5]],
              #             'gpu_map': [[1]],
              #             'lfs': {'path': '/tmp', 'size': 0},
              #             'mem': 0,
              #             'name': 'localhost',
              #             'uid': 'localhost_0'}]}


    # --------------------------------------------------------------------------
    #
    def _watch(self):

        flux_handle = self._get_flux_handle()

        try:

            # thread local cache initialization
            tcache = dict()
            ecache = dict()

            self.register_output(rps.AGENT_STAGING_OUTPUT_PENDING,
                                 rpc.AGENT_STAGING_OUTPUT_QUEUE)

            # signal successful setup to main thread
            self._watcher_setup.set()

            while not self._term.is_set():

                tasks = list()
                try:
                    tasks = self._task_q.get_nowait()
                except queue.Empty:
                    pass

                for task in tasks:

                    flux_id = None
                    try:
                        flux_id = task['flux_id']
                        assert flux_id not in tasks
                        tcache[flux_id] = task

                        # handle and purge cached events for that task
                        events = list()
                        if flux_id in ecache:
                            # known task - handle events
                            events = ecache[flux_id]
                            self.handle_events(flux_handle, task, events)

                    except Exception:

                        self._log.exception("error collecting Task")
                        if task['stderr'] is None:
                            task['stderr'] = ''
                        task['stderr'] += '\nPilot cannot collect task:\n'
                        task['stderr'] += '\n'.join(ru.get_exception_trace())

                        # can't rely on the executor base to free the task resources
                        self._prof.prof('unschedule_start', uid=task['uid'])
                        self.publish(rpc.AGENT_UNSCHEDULE_PUBSUB, task)

                        # event handling failed - fail the task
                        self._log.exception('flux event handling failed')
                        task['target_state'] = rps.FAILED

                        self.advance(task, rps.AGENT_STAGING_OUTPUT_PENDING,
                                           publish=True, push=True)

                    # clean caches for final tasks
                    if flux_id and task.get('target_state'):
                        # task completed - purge data and push it out
                        # NOTE: this assumes events are ordered
                        if flux_id in ecache: del(ecache[flux_id])
                        if flux_id in tcache: del(tcache[flux_id])


                events = list()
                try:
                    events = self._event_q.get_nowait()
                except queue.Empty:
                    pass

                for event in events:

                    self._log.debug('=== ev : %s', event)
                    self._log.debug('=== ids: %s', tcache.keys())

                    flux_id = event[0]  # event: flux_id, flux_state

                    if flux_id in tcache:

                        task = tcache[flux_id]
                        try:
                            # known task - handle events
                            self.handle_events(flux_handle, task, [event])

                        except Exception as e:
                            # event handling failed - fail the task
                            self._log.exception('flux event handling failed')
                            task['target_state'] = rps.FAILED

                        if task.get('target_state'):
                            # task completed - purge data and push it out
                            # NOTE: this assumes events are ordered
                            if flux_id in ecache: del(ecache[flux_id])
                            if flux_id in tcache: del(tcache[flux_id])

                            self.advance(task, rps.AGENT_STAGING_OUTPUT_PENDING,
                                         publish=True, push=True)

                    else:
                        # unknown task, store events for later
                        if flux_id not in ecache:
                            ecache[flux_id] = list()
                        ecache[flux_id].append(event)


                if not events and not tasks:
                    # nothing done in this loop
                    time.sleep(0.01)


        except Exception:
            self._log.exception('=== Error in watcher loop')
            self._term.set()


    # --------------------------------------------------------------------------
    #
    def _parse_res(self, spec):

        # '1-2,3,5,8-10' -> [1,2,3,5,8,9,10]

        if not spec:
            return list()

        res = list()
        for part in spec.split(','):
            if '-' in part:
                start, stop = part.split('-')
                res.extend(list(range(int(start), int(stop))))
            else:
                res.append(int(part))
        return res


# ------------------------------------------------------------------------------

