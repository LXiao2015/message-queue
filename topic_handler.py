import threading
import time

class TopicHandler(object):
    def __init__(self, name):
        self.name = name
        self.offset = 0
        self.lock = threading.Lock()

    def resetOffset(self):
        self.lock.acquire()
        self.offset = 0
        self.lock.release()

    def getMessages(self, ms):
        self.lock.acquire()
        msgs = ms.getMessagesByOffset(self.name, self.offset)
        self.offset += len(msgs)
        self.lock.release()
        return msgs
    
    def consume(self, subscriber, thread_id):
        # periodically polling for messages from server
        while True:
            time.sleep(1)
            msgs = self.getMessages(subscriber.ms)
            for msg in msgs:
                print("Subscriber", subscriber.sub_id, "-", thread_id, "consumes message", msg.data, "from topic", self.name)