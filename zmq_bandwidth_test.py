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



def test_rep(port):
    sock = zmq.Socket(ctx, zmq.REP)
    sock.bind('tcp://*:54321')

    while True:
        message = sock.recv_pyobj()
        if message == 'READY':
            sock.send_pyobj('OK')
            end, start, data = timed_recv(sock)
            sock.send_pyobj('DONE')

            message = sock.recv_pyobj()
            if message == 'OK':
                # Other end has started the blocking read, so send the data payload.
                sock.send_pyobj(data)
                message = sock.recv_pyobj()
                if message == 'DONE':
                    sock.send_pyobj('OK')


def test_req(port, byte_count):
    sock = zmq.Socket(ctx, zmq.REQ)
    sock.connect('tcp://localhost:54321')
    # Inform the other end that we are ready to send some data.  The other end
    # responds with 'OK' to let us know when it is about to start waiting on a
    # blocking receive of the data payload.
    sock.send_pyobj('READY')
    message = sock.recv_pyobj()
    if message == 'OK':
        # Other end has started the blocking read, so send the data payload.
        send_bytes(sock, byte_count)
        message = sock.recv_pyobj()

        if message == 'DONE':
            sock.send_pyobj('OK')
            end, start, data = timed_recv(sock)
            sock.send_pyobj('DONE')
            message = sock.recv_pyobj()
            if message == 'OK':
                print '[OK] Test complete for port={}, payload size (kB)={}'\
                        .format(port, byte_count)


if __name__ == '__main__':
    import sys

    def print_usage():
        raise SystemExit, 'usage: %s bind|connect' % sys.argv[0]

    if not len(sys.argv) == 2 or sys.argv[1] not in ('bind', 'connect'):
        print_usage()

    ctx = zmq.Context.instance()

    port = 54321
    if sys.argv[1] == 'bind':
        test_rep(port)
    else:
        test_req(port, 1 << 20)
