import uuid
import threading
import topic_handler

class Subscriber(object):
    def __init__(self, ms):
        self.sub_id = uuid.uuid4()
        self.ms = ms
        self.topic_handler = dict()
        self.lock = threading.Lock()

    def subscribe(self, topic_name):
        self.lock.acquire()
        h = self.topic_handler.get(topic_name, None)
        new_created = False
        if not h:
            h = topic_handler.TopicHandler(topic_name)
            self.topic_handler[h.name] = h
            new_created = True
        self.lock.release()

        if new_created:
            print("Bring up two new threads to monitor topic", topic_name)
            ts = []
            for i in range(2):
                t = threading.Thread(target=h.consume, args=(self,i))
                t.start()
                ts.append(t)
            return ts

    def resetOffset(self, topic_name):
        self.lock.acquire()
        h = self.topic_handler.get(topic_name, None)
        if not h:
            print("Topic", topic_name, "was not subscribed")
        else:
            h.resetOffset()
        self.lock.release()