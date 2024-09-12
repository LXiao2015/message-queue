import threading

class Topic(object):
    def __init__(self, name):
        self.name = name
        self.mq = list()
        self.lock = threading.Lock()

    def addMessage(self, msg):
        self.lock.acquire()
        self.mq.append(msg)
        self.lock.release()

    def getMessages(self, offset):
        self.lock.acquire()
        msgs = self.mq[offset:]
        self.lock.release()
        return msgs