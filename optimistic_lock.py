import time
import hazelcast
from concurrent.futures import ThreadPoolExecutor

CLUSTER_NAME = "hw2"
MEMBERS = ["127.0.0.1:5701", "127.0.0.1:5702", "127.0.0.1:5703"]

MAP_NAME = "counter-map-optimistic-batch"
KEY = "key"

CLIENTS = 3
ITERS = 10_000
BATCH = 1000

def cas_add(m, delta):
    while True:
        old = m.get(KEY)
        new = old + delta
        if m.replace_if_same(KEY, old, new):
            return

def worker(worker_id: int):
    client = hazelcast.HazelcastClient(cluster_name=CLUSTER_NAME, cluster_members=MEMBERS)
    m = client.get_map(MAP_NAME).blocking()
    m.put_if_absent(KEY, 0)

    done = 0
    while done < ITERS:
        step = min(BATCH, ITERS - done)
        cas_add(m, step)
        done += step
        if done % 2000 == 0:
            print(f"[worker {worker_id}] done {done}")

    client.shutdown()

client = hazelcast.HazelcastClient(cluster_name=CLUSTER_NAME, cluster_members=MEMBERS)
m = client.get_map(MAP_NAME).blocking()
m.put(KEY, 0)
client.shutdown()

print("Starting")
start = time.perf_counter()

with ThreadPoolExecutor(max_workers=CLIENTS) as ex:
    list(ex.map(worker, range(CLIENTS)))

elapsed = time.perf_counter() - start

client = hazelcast.HazelcastClient(cluster_name=CLUSTER_NAME, cluster_members=MEMBERS)
m = client.get_map(MAP_NAME).blocking()
final_value = m.get(KEY)
client.shutdown()

print(f"Clients: {CLIENTS}, iters each: {ITERS}, expected: {CLIENTS * ITERS}")
print("Final value:", final_value)
print(f"Time (sec): {elapsed:.3f}")