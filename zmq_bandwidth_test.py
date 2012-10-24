from datetime import datetime

import numpy as np
import zmq


def total_seconds(delta):
    return delta.microseconds * 1e-6 + delta.seconds


def timed_recv(sock):
    start = datetime.now()
    data = sock.recv_pyobj()
    end = datetime.now()
    return start, end, data


def send_bytes(sock, byte_count):
    sock.send_pyobj(np.ones(byte_count, dtype=np.int8))


def kbytes_per_second(start, end, data):
    return int(data.size * data.dtype.itemsize / total_seconds(end - start)) >> 10


if __name__ == '__main__':
    import sys

    def print_usage():
        raise SystemExit, 'usage: %s bind|connect' % sys.argv[0]

    if not len(sys.argv) == 2 or sys.argv[1] not in ('bind', 'connect'):
        print_usage()
    
    ctx = zmq.Context.instance()

    if sys.argv[1] == 'bind':
        sock = zmq.Socket(ctx, zmq.REP)
        sock.bind('tcp://*:54321')
    else:
        sock = zmq.Socket(ctx, zmq.REQ)
        sock.connect('tcp://remote.fobel.net:54321')
