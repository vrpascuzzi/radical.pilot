#!/usr/bin/env python3

import os
import sys
import zmq
import time
import setproctitle

import radical.utils   as ru
import multiprocessing as mp

delay      = 0.0
n_msgs     = 1000000
n_sender   = 1
n_receiver = 5


# ------------------------------------------------------------------------------
#
def sender(idx, t_zero, n_msgs, delay):

    uid            = 'snd.%03d' % idx
    prof           = ru.Profiler('radical.zmq.%s' % uid)
    context        = zmq.Context()
    socket_src     = context.socket(zmq.PUSH)
    socket_src.hwm = 1024
    socket_src.connect("tcp://127.0.0.1:5000")

    setproctitle.setproctitle('python3_%s' % uid)

    try:
        for num in range(int(n_msgs)):
            msg = {'cnt':num}
            socket_src.send_json(msg)
          # print('sent %s' % msg)
            prof.prof('zmq_snd', uid=uid, msg=t_zero)

            if delay:
                time.sleep (delay)

        print(uid, n_msgs)
    finally:
        prof.close()

    time.sleep(10)



# ------------------------------------------------------------------------------
#
def forward(idx, tzero, n_msgs, delay):

    uid            = 'fwd.%03d' % idx
    prof           = ru.Profiler('radical.zmq.%s' % uid)
    context        = zmq.Context()
    socket_src     = context.socket(zmq.PULL)
    socket_src.hwm = 1024
    socket_src.bind("tcp://127.0.0.1:5000")

    context         = zmq.Context()
    socket_sink     = context.socket(zmq.REP)
    socket_sink.hwm = 1024
    socket_sink.bind("tcp://127.0.0.1:5001")

    setproctitle.setproctitle('python3_%s' % uid)

    try:
        cnt = 0
        while True:
            _ = socket_sink.recv()
            socket_sink.send(socket_src.recv())
            prof.prof('zmq_fwd', uid=uid, msg=t_zero)
          # print(msg)
            
            cnt += 1
            if cnt >= n_msgs:
                print(uid, cnt)
                break

            if delay:
                time.sleep (delay)

    finally:
        prof.close()

    time.sleep(10)


# ------------------------------------------------------------------------------
#
def receiver(idx, tzero, n_msgs, delay):

    uid            = 'rcv.%03d' % idx
    prof            = ru.Profiler('radical.zmq.%s' % uid)
    context         = zmq.Context()
    socket_sink     = context.socket(zmq.REQ)
    socket_sink.hwm = 1024
    socket_sink.connect("tcp://127.0.0.1:5001")

    setproctitle.setproctitle('python3_%s' % uid)

    try:
        cnt = 0
        while True:
            socket_sink.send(b'request')
            msg = socket_sink.recv_json()
            prof.prof('zmq_rcv', msg=t_zero)
          # print('got %s' % msg)
            cnt += 1

            if cnt >= n_msgs:
                print(uid, cnt)
                break

            if delay:
                time.sleep(delay)

    except Exception as e:
        print('ERROR', e)
    finally:
        prof.close()

    time.sleep(10)


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    t_zero = time.time()
    
    print('forward  %d' % 0)
    p = mp.Process(target=forward, args=[0, t_zero, n_msgs, delay])
    p.start()
    time.sleep(3)
    
    procs = list()
    for idx in range(n_sender):
        print('sender   %d' % idx)
        procs.append (mp.Process(target=sender,
                                 args=[idx, t_zero, n_msgs / n_sender, delay]))
    
    for idx in range(n_receiver):
        print('receiver %d' % idx)
        procs.append (mp.Process(target=receiver,
                                 args=[idx, t_zero, n_msgs / n_receiver, delay]))
    
    for p in procs:
        p.start()
    
    for p in procs:
        p.join()
    

# ------------------------------------------------------------------------------

