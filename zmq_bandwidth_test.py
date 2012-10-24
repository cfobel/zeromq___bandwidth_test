from __future__ import division
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


def mbytes_per_second(start, end, data):
    bytes_per_second = int(data.size * data.dtype.itemsize / total_seconds(end - start))
    return bytes_per_second / (1 << 20)


def kbytes_per_second(start, end, data):
    bytes_per_second = int(data.size * data.dtype.itemsize / total_seconds(end - start))
    return bytes_per_second / (1 << 10)


def test_rep(service_uri):
    sock = zmq.Socket(ctx, zmq.REP)
    sock.bind(service_uri)

    while True:
        message = sock.recv_pyobj()
        if message == 'READY':
            sock.send_pyobj('OK')
            end, start, data = timed_recv(sock)
            print '[RESULT] bandwidth of REQ -> REP: {:.1f} MB/s'.format(
                    mbytes_per_second(end, start, data))
            sock.send_pyobj('DONE')

            message = sock.recv_pyobj()
            if message == 'OK':
                # Other end has started the blocking read, so send the data payload.
                sock.send_pyobj(data)
                message = sock.recv_pyobj()
                if message == 'DONE':
                    sock.send_pyobj('OK')


def test_req(service_uri, byte_count):
    sock = zmq.Socket(ctx, zmq.REQ)
    sock.connect(service_uri)

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
            print '[RESULT] bandwidth of REP -> REQ: {:.1f} MB/s'.format(
                    mbytes_per_second(end, start, data))
            sock.send_pyobj('DONE')
            message = sock.recv_pyobj()
            if message == 'OK':
                print '[OK] Test complete for service_uri={}, payload size (kB)={}'\
                        .format(service_uri, byte_count >> 10)


def parse_args():
    """Parses arguments, returns ``(options, args)``."""
    from argparse import ArgumentParser

    parser = ArgumentParser(description="""\
Server/client for testing bandwidth between endpoints using zermoq.""",
                           )
    parser.add_argument(nargs=1, dest='action', type=str, default='connect',
            choices=['connect', 'bind'])
    parser.add_argument(nargs=1, dest='service_uri', type=str)
    parser.add_argument(nargs='?', dest='byte_count', type=int, default=10 << 20)

    args = parser.parse_args()
    args.action = args.action[0]
    args.service_uri = args.service_uri[0]

    return args


if __name__ == '__main__':
    args = parse_args()

    ctx = zmq.Context.instance()

    if args.action == 'bind':
        test_rep(args.service_uri)
    else:
        test_req(args.service_uri, args.byte_count)
