import threading
import time
import messaging_service
import message

if __name__ == "__main__":

    # init MessagingServer
    ms = messaging_service.MessagingServer()

    # run 2 Producers, each has 2 threads
    producers = []
    pro1 = ms.registerProducer()
    pro2 = ms.registerProducer()
    ms.createTopic("apple")
    ms.createTopic("banana")
    def business(topic_name, pro):
        for i in range(3):
            time.sleep(0.5)
            msg = message.Message(time.time())
            pro.produce(topic_name, msg)

    for i in range(2):
        t = threading.Thread(target=business, args=("apple",pro1))
        producers.append(t)
        t.start()
    for i in range(2):
        t = threading.Thread(target=business, args=("banana",pro2))
        producers.append(t)
        t.start()


    # run 2 Subscribers
    subscribers = []
    sub1 = ms.registerSubscriber()
    subscribers.extend(sub1.subscribe("apple"))
    subscribers.extend(sub1.subscribe("banana"))
    sub2 = ms.registerSubscriber()
    subscribers.extend(sub2.subscribe("apple"))

    time.sleep(10)
    sub1.resetOffset("banana")

    # block
    for t in producers:
        if not t:
            continue
        t.join()
    for t in subscribers:
        if not t:
            continue
        t.join()