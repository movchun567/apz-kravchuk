import hazelcast
from concurrent.futures import ThreadPoolExecutor

N = 1000
WORKERS = 10
MAP_NAME = "distributed-map"

client = hazelcast.HazelcastClient(
    cluster_name="hw2",
    cluster_members=["127.0.0.1:5701", "127.0.0.1:5702", "127.0.0.1:5703"],
)

m = client.get_map(MAP_NAME).blocking()

print("Connected. Clearing map...")
m.clear()

def put_range(start, end):
    for i in range(start, end):
        m.put(i, f"value-{i}")

chunk = (N + WORKERS - 1) // WORKERS
ranges = [(i, min(i + chunk, N)) for i in range(0, N, chunk)]

with ThreadPoolExecutor(max_workers=WORKERS) as ex:
    ex.map(lambda r: put_range(*r), ranges)

print("Inserted:", m.size())
client.shutdown()