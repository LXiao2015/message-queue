import uuid

class Producer(object):
    def __init__(self, ms):
        self.pro_id = uuid.uuid4()
        self.ms = ms
    
    def produce(self, topic_name, msg):
        print("Producer", self.pro_id, "produces msg", msg.data, "to topic", topic_name)
        self.ms.addMessage(topic_name, msg)