import asyncio
import time
import httpx

FACADE = "http://localhost:8000"
TX = f"{FACADE}/transaction"

async def worker(client: httpx.AsyncClient, user_id: str, n: int, amount: int):
    for _ in range(n):
        r = await client.post(TX, json={"user_id": user_id, "amount": amount})
        r.raise_for_status()

async def run_scenario(title: str, user_ids, n_each: int, amount: int, concurrency: int):
    print(f"\n=== {title} ===")
    sem = asyncio.Semaphore(concurrency)

    async with httpx.AsyncClient(timeout=30.0) as client:
        await client.post(f"{FACADE}/metrics/reset")

        async def wrap(uid):
            async with sem:
                await worker(client, uid, n_each, amount)

        t0 = time.perf_counter()
        await asyncio.gather(*[wrap(uid) for uid in user_ids])
        dt = time.perf_counter() - t0

        total = len(user_ids) * n_each
        print(f"Total requests: {total}")
        print(f"Total time (sec): {dt:.3f}")
        print(f"Requests/sec: {total/dt:.1f}")

        metrics = (await client.get(f"{FACADE}/metrics")).json()
        print("Facade metrics:", metrics)

async def main():
    clients = 10
    n_each = 10_000
    amount = 1

    # 10 clients and 10 accounts
    user_ids_1 = [f"user{i}" for i in range(1, clients + 1)]
    await run_scenario("Scenario 1 (10 users, each own account)", user_ids_1, n_each, amount, concurrency=50)

    # 10 clients and 1 account
    user_ids_2 = ["shared"] * clients
    await run_scenario("Scenario 2 (all to same account)", user_ids_2, n_each, amount, concurrency=50)

    async with httpx.AsyncClient(timeout=10.0) as client:
        shared = (await client.get(f"{FACADE}/user/shared")).json()
        print("Shared result:", {"balance": shared["balance"], "tx_count": len(shared["transactions"])})

if __name__ == "__main__":
    asyncio.run(main())