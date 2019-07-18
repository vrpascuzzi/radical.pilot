#!/bin/sh

# This script will pulll up two ZMQ queues: one for feeding messages from the
# client to the agent, one for the reverse direction.  The queue lifetimes are
# dteremined by heartbeats: once messages stop, the heartbeat timers will
# eventually time out and the queue processes finish.
#
# The queue startup is synchronous, input and output endpoints will be printed
# on stdout in the form:
#
#     ZMQ_PUT_IN  : tcp://192.168.8.100:44743
#     ZMQ_PUT_OUT : tcp://192.168.8.100:38121
#     ZMQ_GET_IN  : tcp://192.168.8.100:40219
#     ZMQ_GET_OUT : tcp://192.168.8.100:40205 
#
# The port numbers are random.  If the ports happen to be shielded by
# a firewall, ssh tunnels should be used to expose them on the required hosts.

SID="$1"
PID="$2"

VE_LOC="$HOME/.radical/ve_v1.0/"
VE_ACT="$VE_LOC/bin/activate"
RP_ZMQ="$VE_LOC/bin/radical-pilot-agent-bridge"
RP_DIR="$HOME/.radical/pilot/zmq_data/"

if test -z "$SID"
then
    echo "ERROR: missing argument (session ID)"
    exit 1
fi

if test -z "$PID"
then
    echo "ERROR: missing argument (pilot ID)"
    exit 1
fi

if ! test -f "$VE_ACT"
then
    echo "ERROR: invalid radical ve at $VE_LOG"
    exit 2
fi

if ! test -f "$VE_ACT"
then
    echo "ERROR: missing radical installation at $VE_LOG"
    exit 3
fi

mkdir -p "$RP_DIR"

RP_OUT="$RP_DIR/$SID.$PID.out"
RP_ERR="$RP_DIR/$SID.$PID.err"
RP_PID="$RP_DIR/$SID.$PID.pid"

. "$VE_ACT"

cd "$RP_DIR"
"$RP_ZMQ" "$SID" "$PID" > "$RP_OUT" 2> "$RP_ERR" & PROC=$!

echo "$PROC" > "$RP_PID"

if test -z "$PROC"
then
    echo "ERROR: failed to start $RP_ZMQ: $(cat $RP_ERR)"
    exit 4
fi

n=0
while test "$n" -lt "10"
do
    if ! ps -q "$PROC" -o pid= >/dev/null
    then
        echo "ERROR: $RP_ZMQ process $PROC is gone: $(cat $RP_ERR)"
    fi


    if test -s "$RP_OUT"
    then
        ZMQ_PUT=$(cat $RP_OUT | grep 'ZMQ_PUT' 2>/dev/null)
        ZMQ_GET=$(cat $RP_OUT | grep 'ZMQ_GET' 2>/dev/null)
    fi


    if test -z "$ZMQ_PUT" -o -z "$ZMQ_GET"
    then
        n=$((n+1))
        sleep 1
        continue
    fi

    echo "$ZMQ_PUT"
    echo "$ZMQ_GET"
    exit 0

done

echo "ERROR: timeout"
exit 1

