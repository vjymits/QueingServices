__author__ = 'Viajy Sharma'

import threading
import time
import json
import config


class Queue:

    def __init__(self, load=False, path="queue_data.json"):
        """
        Queue represented as python list.
        :return:
        """
        self.q=[]
        if load:
            try:
                self.load(path=path)
            except json.decoder.JSONDecodeError as e:
                print("Failed to load previous data, going with empty queue.")


    def print(self):
        print(str(self.q))

    def get_queue(self):
        return self.q

    def save(self, path='queue_data.json'):
        """
        Save the queue, for future use if needed.
        :param path:
        :return:
        """
        with open(path, 'w') as outfile:
            d={'data': self.q}
            json.dump(d, outfile)

    def load(self, path='queue_data.json'):
        """
        Load, saved queue
        :param path:
        :return:
        """
        with open(path) as json_file:
            data = json.load(json_file)
            self.q = data.get('data', [])


class Writer:
    """
    Writes in given Queue.
    """

    lock = threading.Lock()
    object_count =0

    def __init__(self, q, auto_save=False):
        """
        Construct writer object, which enable user to insert into Queue.
        :param q: Object of Queue
        :param auto_save: If True save the queue on secondary storage, default False
        :return:
        """
        if not isinstance(q, Queue):
            raise ValueError("Given argument is not Queue.")

        with Writer.lock:
            # Thread safe
            if not Writer.object_count < config.WRITER_POOL_SIZE:
                raise PermissionError("No of instances reached max limit.")
            Writer.object_count += 1
        self.q = q
        self.auto_save = auto_save

    def __del__(self):
        """
        Destruct the object
        :return:
        """
        with Writer.lock:
            # Thread safe
            if Writer.object_count > 0:
                Writer.object_count -= 1


    def enqueue(self, d):
        """
        Function add a element in front of queue by using append() which is already a thread safe,
        :param d: user data
        :return: None
        """
        data = {"data": d, "writeTimestamp": int(round(time.time() * 1000))}   #adding miilliseconds as timestamp from epoch
        self.q.q.append(data)
        if self.auto_save:
            self.q.save()


class Reader:
    """
    It reads from given queue.
    """

    object_count = 0
    lock = threading.Lock()

    def __init__(self, q, auto_save = False):
        """
        Construct Reader object, which enable user to read/dequeue from Queue.
        :param q: Object of Queue
        :param auto_save: If True save the queue on secondary storage, default False
        :return:
        """
        if not isinstance(q, Queue):
            raise ValueError("Given argument is not Queue.")

        with Reader.lock:
            # Thread safe
            if not Reader.object_count < config.READER_POOL_SIZE:
                raise PermissionError("No of instances reached max limit.")
            Reader.object_count += 1
        self.q = q
        self.auto_save = auto_save

    def __del__(self):
        """
        Destruct the object
        :return:
        """
        with Reader.lock:
            # Thread safe
            if Reader.object_count > 0:
                Reader.object_count -= 1

    def dequeue(self):
        """
        Function read the element and delete from queue,
        This function should be thread safe because we reading by index in list: q
        :return: rear Element
        """
        with Reader.lock:
            if len(self.q.q) == 0:
                print("Sleeping for "+str(config.READER_WAIT_TIME_SEC)+" Sec.")
                time.sleep(config.READER_WAIT_TIME_SEC)
            if len(self.q.q) == 0:
                print("Queue is empty even after waiting for "+str(config.READER_WAIT_TIME_SEC)+" secs, STOPING.")
            res = self.q.q.pop(0)

            if self.auto_save:
                self.q.save()
            return res

