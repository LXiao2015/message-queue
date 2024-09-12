import threading
import subscriber
import producer
import topic

class MessagingServer(object):
    def __init__(self):
        self.topics = dict()
        self.lock = threading.Lock()

    def registerSubscriber(self) -> subscriber.Subscriber:
        sub = subscriber.Subscriber(self)
        print("Registered subscriber", sub.sub_id)
        return sub
    
    def registerProducer(self) -> producer.Producer:
        pro = producer.Producer(self)
        print("Registered producer", pro.pro_id)
        return pro

    def createTopic(self, name):
        self.lock.acquire()
        tpc = topic.Topic(name)
        self.topics[name] = tpc
        self.lock.release()

    def getMessagesByOffset(self, topic_name, offset):
        print("Getting messages by offset", offset)
        msgs = []
        self.lock.acquire()
        tpc = self.topics.get(topic_name, None)
        if not tpc:
            print("Topic %s was not created" % topic_name)
        else:
            msgs = tpc.getMessages(offset)
        self.lock.release()
        return msgs

    def addMessage(self, topic_name, msg):
        self.lock.acquire()
        tpc = self.topics.get(topic_name, None)
        if not tpc:
            print("Topic %s was not created" % topic_name)
        else:
            tpc.addMessage(msg)
        self.lock.release()
