
__copyright__ = "Copyright 2013-2016, http://radical.rutgers.edu"
__license__   = "MIT"

import copy

import radical.utils as ru

from .continuous import Continuous

from ... import states           as rps
from ... import task_description as rpcud


# The BOT Scheduler schedules tasks just like the continuous scheduler (and in
# fact calls the continuous scheduler to do so), but additionally adds structure
# to the stream of tasks, dividing them into chunks, aka 'bag of tasks' (BoT).
# These bag of tasks can have some additional constraints and relations:
#
#   - BoTs can be ordered, i.e., tasks from a BoT with order `n` are not started
#     before all tasks from all BoTs of order `m` with `m < n`.
#
#   - concurrent: tasks in a BoT can be configured to get started at
#     the same time (*)
#
#   - co-locate: tasks in a BoT can be configured to land on the same
#     set of compute nodes (**)
#
#   - de-locate: tasks in a BoT can be configured to land on a different
#     set of compute nodes (**)
#
# To make use of these facilities, tasks will need to be tagged to belong to
# a certain BoT.  Since the RP agent will receive tasks in a continuous stream,
# the tag information will also have to include the size of the BoT, so that the
# scheduler can judge is a bag is complete.  The tag can further include flags
# to trigger concurrency and/or locality constraints:
#
#    tags = {
#        'bot' : {
#            'bid'       : 'foo',  # mandatory
#            'size'      : 4,      # optional, default: 1
#            'order'     : 2,      # optional, default: None
#            'concurrent': False,  # optional, default: False
#            'co-locate' : False   # optional, default: False
#            'de-locate' : False   # optional, default: False
#        }
#    }
#
#
# Note that the tags for all tasks in a BoT must be consistent - otherwise all
# tasks in that bag are marked as `FAILED`.
#
# Note that a BoT ID can be reused.  For example, 4 tasks can share a BoT ID
# `bot1` of size `2`.  The scheduler will collect 2 tasks and run them.  If it
# encounters the same BoT ID again, it will again collect 2 tasks.  If
# `co-locate` is enabled, then the second batch will run on the same node as the
# first batch.  If the BoT has an order defined, then the first batch will need
# to see at least one BoT for each lower order completed before the BoT is
# eligible.  If a subsequent batch is received for the same BoT ID, then at
# least two batches of the lower order BoT need to run first, etc.
#
#
# (*)  'at the same time': small differences in startup time may occur due to
#      RP agent and HPC system configuration, RP though guarantees that the BoT
#      becomes eligible for execution at the exact same time.
#
# (**) 'set of compute nodes': the current implementation can only handle
#      `co-locate` and `de-locate` for tasks of size up to a single node.
#
#
# Examples:
#
#   task.1  bid=bot1  size=4
#   task.2  bid=bot1  size=4
#   task.3  bid=bot1  size=4
#   task.4  bid=bot1  size=4
#
#   The tasks 1-4 will be scheduled and executed individually - but only become
#   eligible for execution once all 4 tasks arrive in the scheduler.
#
#
#   task.1  bid=bot1  size=2  order=None  concurrent=True  co-locate=True
#   task.2  bid=bot1  size=2  order=None  concurrent=True  co-locate=True
#   task.3  bid=bot2  size=2  order=None  concurrent=True  co-locate=True
#   task.4  bid=bot2  size=2  order=None  concurrent=True  co-locate=True
#
#   tasks 1 and 2 will run concurrently on the same node, tasks 3 and 4 will
#   also run concurrently on one node (possibly at a different time).  The node
#   for the first batch may or may not be the same as for the second batch.
#
#
#   task.1  bid=bot1  size=2
#   task.2  bid=bot1  size=2
#   task.3  bid=bot1  size=2
#   task.4  bid=bot1  size=2
#
#   tasks 1 and 2 will run concurrently on the same node, tasks 3 and 4 will
#   also run concurrently on _the same_ node (possibly at a different time).
#   The node for the first batch may or may not be the same as for the second
#   batch.
#
#
#   task.1  bid=bot1  size=3
#   task.2  bid=bot1  size=3
#   task.3  bid=bot1  size=3
#   task.4  bid=bot1  size=3
#
#   tasks 1 to 3 will run concurrently on the same node, but task 4 will never
#   get scheduled (unless more tasks arrive to complete the batch).
#
#
#   task.1  bid=bot1
#   task.2  bid=bot1
#   task.3  bid=bot1
#   task.4  bid=bot1
#
#   tasks 1 to 4 will land on the same node, possibly at different times.
#
#
#   task.1  size=4
#   task.2  size=4
#   task.3  size=4
#   task.4  size=4
#
#   tasks 1 to 4 will run concurrently, but possibly on different nodes.
#
#   task.1  size=4 de-locate=True
#   task.2  size=4 de-locate=True
#   task.3  size=4 de-locate=True
#   task.4  size=4 de-locate=True
#
#   tasks 1 to 4 will run concurrently, but guaranteed on different nodes
#   (needs 4 nodes!)
#
#
# The dominant use case for this scheduler is the execution of coupled
# applications which exchange data via shared local files or shared memory.
#
#
# NOTE: tasks exit codes don't influence the scheduling algorithm: subsequent
#       task batches will be scheduled even if the first batch completed with
#       a non=zero exit code.
#
#       If a string is specified instead of a dict, it is interpreted as `node`.
#       If an integer is specified, it is interpreted a batch `size`.
#
#       If `node` is not specified, no node locality is enforced - the
#       algorithm only respects time locality (`size`).
#
#       If `size` is not specified, no time colocation if enforced - the
#       algorithm only respects node locality.  This is the same behaviour as
#       with `size=1`.
#
#
# Implementation:
#
# The scheduler operates on tasks in two distinct stages:
#
#   1 - eligibility: a task becomes eligible for placement when all dependencies
#       are resolved.  Dependencies are :
#       - all bags of lower order are completed (*)
#       - all tasks to co-locate with this task are available
#       - all tasks to de-locate with this task are available
#
#   2 - placement:  a set of tasks which is eligible for placement is scheduled
#       on the candidate nodes
#
#   (*) completed means that the scheduler received a notification that the task
#       entered `UMGR_STAGING_OUTPUT_PENDING` state, implying that task
#       execution completed and that all *local* staging directives have been
#       enacted.
#
#
# When scheduling a set of tasks, the following candidate nodes are considered
# for placement:
#
#   - co-location: the first task in the respective bag is scheduled on *any*
#     node.  Any subsequent task in the same bag will be scheduled on the *same*
#     node as the first task - no other nodes are considered.
#
#   - de-location: the first task is scheduled on *any* node.  The next task in
#     the same bag is scheduled on any node *but* the one the first is placed
#     on, and so on.  If no nodes remain available (BoT size > n_nodes), the
#     task will fail.
#
#   - concurrent: one task is scheduled after the other, according to the
#     constraints above.  If any task in the bag fails to schedule, all
#     previously scheduled tasks will be unscheduled and have to wait.  Tasks
#     are advanced in bulk only after all tasks have been placed successfully.
#
#   - all other tasks (without locality or concurrency constraints) are
#     scheduled individually.
#
#   - concurrent co-location (optimized): the resources for all tasks in the bag
#     are added up, and a single virtual task is scheduled on *any* node.  Once
#     the virtual task is placed, the allocation is again split up between the
#     individual tasks, resulting in their respective placement, and the bag is
#     advanced in bulk.
#
# NOTE: This functionality is dangerously close (and in fact overlapping) with
#       workflow orchstration.  As such, it should actually not live in the
#       scheduler but should move into a separate RCT component.  To cater for
#       that move at a later point, we render the specific code parts as filter
#       whose implementation we should be able to easily extract as needed.  The
#       filter
#
#         - receives a set of tasks
#         - filters tasks which are eligible to run under the given tag
#           constraints
#         - forwards eligible tasks to the scheduler proper
#         - retain non-eligible tasks until new information received via any
#           communication channels indicates that they become eligible to run
#
#       The wait list managed by that filer is different and independent from
#       the wait list maintained in the scheduler where tasks are waiting for
#       required resources to become available).
#
class ContinuousColo(Continuous):

    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg, session):

        Continuous.__init__(self, cfg, session)


    # --------------------------------------------------------------------------
    #
    def _configure(self):

        Continuous._configure(self)

        # a 'bag' entry will look like this:
        #
        #   {
        #      'size': 128,    # number of tasks to expect
        #      'uids': [...]}, # ids    of tasks to be scheduled
        #   }

        self._lock      = ru.RLock()   # lock on the bags
        self._tasks     = dict()       # task registry (we use uids otherwise)
        self._unordered = list()       # IDs of tasks which are not colocated
        self._bags      = dict()       # nothing has run, yet

        self._bag_init  = {'size' : 0,
                           'uids' : list()}


    # --------------------------------------------------------------------------
    #
    def _get_tags(self, descr):

        tags = descr.get('tags', {})
        if not tags:
            return {}

        node = tags.get('node')
        size = tags.get('size')

        if not node : node  = None
        if not size: size = 0

        return node, size


    # --------------------------------------------------------------------------
    # overload the main method from the base class
    def _schedule_tasks(self, tasks):

        if not isinstance(tasks, list):
            tasks = [tasks]

        self.advance(tasks, rps.AGENT_SCHEDULING, publish=True, push=False)

        with self._lock:

            # cache ID int to avoid repeated parsing
            for task in tasks:

                uid      = task['uid']
                descr    = task['description']
                colo_tag = descr.get('tags', {}).get('colocate')

                # tasks w/o order info are handled as usual, and we don't keep
                # any infos around
                if not colo_tag:
                  # self._log.debug('no tags for %s', uid)
                    self._unordered.append(task)
                    continue

                # this uniit wants to be ordered - keep it in our registry
                assert(uid not in self._tasks), 'duplicated task %s' % uid
                self._tasks[uid] = task

                # this task wants to be ordered - keep it in our registry
                assert(uid not in self._tasks), 'duplicated task %s' % uid
                self._tasks[uid] = task

              # self._log.debug('colo %s: %s : %d', uid, bag, size)

                # initiate bag if needed
                if bag not in self._bags:
                    self._bags[bag]         = copy.deepcopy(self._bag_init)
                    self._bags[bag]['size'] = size

                else:
                    assert(size == self._bags[bag]['size']), \
                           'inconsistent bag size'

                # add task to order
                self._bags[bag]['uids'].append(uid)

        # try to schedule known tasks
        self._try_schedule()

        return True


    # --------------------------------------------------------------------------
    def _try_schedule(self):
        '''
        Schedule all tasks in self._unordered.  Then for all name spaces,
        check if their `current` order has tasks to schedule.  If not and
        we see `size` tasks are `done`, consider the order completed and go
        to the next one.  Break once we find a BoT which is not completely
        schedulable, either because we did not yet get all its tasks, or
        because we run out of resources to place those tasks.
        '''

        self._log.debug('try schedule')
        scheduled = list()  # list of scheduled tasks

        # FIXME: this lock is very aggressive, it should not be held over
        #        the scheduling algorithm's activity.
        # first schedule unordered tasks (
        with self._lock:

            keep = list()
            for task in self._unordered:

                # attempt to schedule this task (use continuous algorithm)
                if Continuous._try_allocation(self, task):

                    # success - keep it and try the next one
                    scheduled.append(task)

                else:
                    # failure - keep task around
                    keep.append(task)

            # keep only unscheduled tasks
            self._unordered = keep


        # FIXME: this lock is very aggressive, it should not be held over
        #        the scheduling algorithm's activity.
        with self._lock:

            # now check all bags for eligibility, filter scheduled ones
            to_delete = list()
            for bag in self._bags:

                self._log.debug('try bag %s', bag)

                if self._bags[bag]['size'] < len(self._bags[bag]['uids']):
                    raise RuntimeError('inconsistent bag assembly')

                # if bag is complete, try to schedule it
                if self._bags[bag]['size'] == len(self._bags[bag]['uids']):

                    self._log.debug('try bag %s (full)', bag)
                    if self._try_schedule_bag(bag):

                        self._log.debug('try bag %s (placed)', bag)
                        # scheduling works - push tasks out and erase all traces
                        # of the bag (delayed until after iteration)
                        for uid in self._bags[bag]['uids']:

                            scheduled.append(self._tasks[uid])

                        to_delete.append(bag)

            # delete all bags which have been pushed out
            for bag in to_delete:

                del(self._bags[bag])


        # advance all scheduled tasks and push them out
        if scheduled:
            self.advance(scheduled, rps.AGENT_EXECUTING_PENDING,
                         publish=True, push=True)

      # self._log.debug('dump')
      # self._log.debug(pprint.pformat(self._bags))


    # --------------------------------------------------------------------------
    #
    def _try_schedule_bag(self, bag):
        '''
        This methods assembles the requirements of all tasks in a bag into
        a single pseudo-task.  We ask the cont scheduler to schedule that
        pseudo-task for us.  If that works, we disassemble the resulting
        resource slots and assign them to the bag's tasks again, and declare
        success.
        '''

        self._log.debug('try schedule bag %s ', bag)

        tasks  = [self._tasks[uid] for uid in self._bags[bag]['uids']]
        pseudo = copy.deepcopy(tasks[0])

        pseudo['uid'] = 'pseudo.'

        descr = pseudo['description']
        descr['cpu_process_type'] = rpcud.POSIX  # force single node
        descr['cpu_thread_type']  = rpcud.POSIX
        descr['cpu_processes']    = 0
        descr['cpu_threads']      = 1

        descr['gpu_process_type'] = rpcud.POSIX  # force single node
        descr['gpu_thread_type']  = rpcud.POSIX
        descr['gpu_processes']    = 0
        descr['gpu_threads']      = 1

        self._log.debug('try schedule uids  %s ', self._bags[bag]['uids'])
      # self._log.debug('try schedule tasks  %s ', pprint.pformat(tasks))

        for task in tasks:
            td = task['description']
            pseudo['uid'] += task['uid']

            descr['cpu_processes'] += td['cpu_processes'] * td['cpu_threads']
            descr['gpu_processes'] += td['gpu_processes']

      # self._log.debug('try schedule pseudo %s ', pprint.pformat(pseudo))

        if not Continuous._try_allocation(self, pseudo):

            # cannot schedule this pseudo task right now, bag has to wait
            return False

        # we got an allocation for the pseudo task, not disassemble the slots
        # and assign back to the individual tasks in the bag
        slots = copy.deepcopy(pseudo['slots'])
        cpus  = copy.deepcopy(pseudo['slots']['nodes'][0]['core_map'])
        gpus  = copy.deepcopy(pseudo['slots']['nodes'][0]['gpu_map'])

        slots['nodes'][0]['core_map'] = list()
        slots['nodes'][0]['gpu_map']  = list()

        for task in tasks:

            tslots = copy.deepcopy(slots)
            descr  = task['description']

            for _ in range(descr['cpu_processes']):
                block = list()
                for _ in range(descr['cpu_threads']):
                    block.append(cpus.pop(0)[0])
                tslots['nodes'][0]['core_map'].append(block)

            for _ in range(descr['gpu_processes']):

                block = list()
                block.append(gpus.pop(0)[0])
                tslots['nodes'][0]['gpu_map'].append(block)

            task['slots'] = tslots

        return True


    # --------------------------------------------------------------------------
    #
    def schedule_cb(self, topic, msg):
        '''
        This cb gets triggered after some tasks got unscheduled, i.e., their
        resources have been freed.  We attempt a new round of scheduling at that
        point.
        '''
        self._try_schedule()

        # keep the cb registered
        return True


# ------------------------------------------------------------------------------

