import time
import hazelcast
from concurrent.futures import ThreadPoolExecutor

CLUSTER_NAME = "hw2"
MEMBERS = ["127.0.0.1:5701", "127.0.0.1:5702", "127.0.0.1:5703"]
QNAME = "bounded-q"

def producer():
    client = hazelcast.HazelcastClient(cluster_name=CLUSTER_NAME, cluster_members=MEMBERS)
    q = client.get_queue(QNAME).blocking()

    for i in range(1, 101):
        q.put(i)
        print(f"[P] put {i}")

    client.shutdown()

def consumer(cid):
    client = hazelcast.HazelcastClient(cluster_name=CLUSTER_NAME, cluster_members=MEMBERS)
    q = client.get_queue(QNAME).blocking()

    while True:
        x = q.take()
        print(f"[C{cid}] got {x}")

        if x == 100:
            break

    client.shutdown()

print("Starting...")

with ThreadPoolExecutor(max_workers=3) as ex:
    ex.submit(producer)
    ex.submit(consumer, 1)
    ex.submit(consumer, 2)