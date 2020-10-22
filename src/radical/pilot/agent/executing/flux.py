
__copyright__ = 'Copyright 2013-2020, http://radical.rutgers.edu'
__license__   = 'MIT'


import os
import time
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

        if cmd == 'cancel_units':

            # FIXME: clarify how to cancel tasks in Flux
            pass

        return True


    # --------------------------------------------------------------------------
    #
    def work(self, units):

        self._task_q.put(ru.as_list(units))

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

        ret = False
        uid = task['uid']

        for event in events:

            flux_id    = event[0]
            flux_state = event[1]
            self._log.debug('=== full event: %s',  event)

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
            # we don't need to do anything on most events and actually only wait
            # for task completion (`INACTIVE`)
            if flux_state != 'INACTIVE':
                self._log.debug('ignore flux event %s:%s' %
                                (task['uid'], flux_state))
                continue

            self._log.debug('handle flux event %s:%s' %
                            (task['uid'], flux_state))

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
                    if retval == 0: task['target_state'] = rps.DONE
                    else          : task['target_state'] = rps.FAILED
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

            # also fetch resource information
            payload = {"id"   : flux_id,
                       "keys" : ["R"],
                       "flags": 0}
            result  = flux_handle.rpc("job-info.lookup", payload).get()
            self._log('=== resources: %s', result)


            # the task is completed, push it out to the output stager
            task['state'] = rps.AGENT_STAGING_OUTPUT_PENDING
            self.advance(task, publish=True, push=True)


        return ret


    # --------------------------------------------------------------------------
    #
    def _watch(self):

        flux_handle = self._get_flux_handle()

        try:

            # thread local initialization
            tasks  = dict()
            events = dict()

            self.register_output(rps.AGENT_STAGING_OUTPUT_PENDING,
                                 rpc.AGENT_STAGING_OUTPUT_QUEUE)

            # signal successful setup to main thread
            self._watcher_setup.set()

            while not self._term.is_set():

                active = False

                try:
                    for task in self._task_q.get_nowait():

                        flux_id = task['flux_id']
                        assert flux_id not in tasks
                        tasks[flux_id] = task

                        # handle and purge cached events for that task
                        if flux_id in events:
                            if self.handle_events(flux_handle, task,
                                                  events[flux_id]):
                                # task completed - purge data
                                # NOTE: this assumes events are ordered
                                if flux_id in events: del(events[flux_id])
                                if flux_id in tasks : del(tasks[flux_id])

                    active = True

                except queue.Empty:
                    # nothing found -- no problem, check if we got some events
                    pass


                try:

                    for event in self._event_q.get_nowait():

                        self._log.debug('=== ev : %s', event)
                        self._log.debug('=== ids: %s', tasks.keys())

                        flux_id = event[0]
                        if flux_id in tasks:

                            # known task - handle events
                            if self.handle_events(flux_handle, tasks[flux_id],
                                                               [event]):
                                # task completed - purge data
                                # NOTE: this assumes events are ordered
                                if flux_id in events: del(events[flux_id])
                                if flux_id in tasks : del(tasks[flux_id])

                        else:
                            # unknown task, store events for later
                            if flux_id not in events:
                                events[flux_id] = list()
                            events[flux_id].append(event)

                    active = True

                except queue.Empty:
                    # nothing found -- no problem, check if we got some tasks
                    pass

                if not active:
                    time.sleep(0.01)


        except Exception:
            self._log.exception('=== Error in watcher loop')
            self._term.set()


# ------------------------------------------------------------------------------

