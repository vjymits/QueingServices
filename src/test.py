__author__ = 'Vijay Sharma'

import queue
import config
import random
import uuid
import time
from threading import Thread
import threading

"""
Created a one object of queue, and several reader and writer,

if auto_save is True, for operational function (enqueue or dequeue) than it saves after operation.

if load = True than Queue will load previous save by Operational Service.

"""


threads = []
qu = queue.Queue()

w1 = queue.Writer(qu)
w2 = queue.Writer(qu)
w3 = queue.Writer(qu)
w4 = queue.Writer(qu, auto_save=True)

r1 = queue.Reader(qu)
r2 = queue.Reader(qu)
r3 = queue.Reader(qu)
r4 = queue.Reader(qu, auto_save=True)



def test_big_read(reader):

    i=100000
    while i > 0:
        print(reader.dequeue())
        i -= 1
        time.sleep(.10)

def test_big_write(writer):
    i=100000
    print("test")
    while i > 0:
        d={"randomUUID": str(uuid.uuid4()), "counter": i, "thread": threading.get_ident()}
        writer.enqueue(d)
        i -= 1
        time.sleep(.30)

if __name__ == '__main__':
    print("Testing Queue: "+str(config.READER_WAIT_TIME_SEC))

    t = Thread(target=test_big_write, args=(w1,))
    t.start()
    threads.append(t)
    t = Thread(target=test_big_read, args=(r1,))
    t.start()
    threads.append(t)

    t = Thread(target=test_big_write, args=(w2,))
    t.start()
    threads.append(t)

    t = Thread(target=test_big_read, args=(r2,))
    t.start()
    threads.append(t)

    t = Thread(target=test_big_write, args=(w3,))
    t.start()
    threads.append(t)

    t = Thread(target=test_big_write, args=(w4,))
    t.start()
    threads.append(t)

    t = Thread(target=test_big_read, args=(r3,))
    t.start()
    threads.append(t)

    t = Thread(target=test_big_read, args=(r4,))
    t.start()
    threads.append(t)



    print("gonna to join.")
    for t in threads:
        t.join()





