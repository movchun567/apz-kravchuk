import argparse
import asyncio
import time

import httpx

DEFAULT_FACADE = "http://localhost:8000"

async def post_one(client, facade, user_id, amount):
    r = await client.post(f"{facade}/transaction", json={"user_id": user_id, "amount": amount})
    r.raise_for_status()

async def worker(client, facade, user_id, n, amount):
    for _ in range(n):
        await post_one(client, facade, user_id, amount)

def _print_metrics_breakdown(metrics, total_time_sec):
    logging_calls = metrics.get("logging_calls", 0) or 0
    counter_calls = metrics.get("counter_calls", 0) or 0
    logging_time = float(metrics.get("logging_time_sec", 0.0) or 0.0)
    counter_time = float(metrics.get("counter_time_sec", 0.0) or 0.0)

    logging_avg_ms = float(metrics.get("logging_avg_ms", 0.0) or 0.0)
    counter_avg_ms = float(metrics.get("counter_avg_ms", 0.0) or 0.0)

    logging_share = (logging_time / total_time_sec * 100.0) if total_time_sec else 0.0
    counter_share = (counter_time / total_time_sec * 100.0) if total_time_sec else 0.0

    print(f"facade.metrics.logging_calls={logging_calls} total_logging_time_sec={logging_time:.3f} avg_ms={logging_avg_ms:.2f} share_of_wall={logging_share:.1f}%")
    print(f"facade.metrics.counter_calls={counter_calls} total_counter_time_sec={counter_time:.3f} avg_ms={counter_avg_ms:.2f} share_of_wall={counter_share:.1f}%")
    if logging_share > 100.0 or counter_share > 100.0:
        print("note: share_of_wall may exceed 100% due to parallelism (logging/counter calls overlap in time).")

async def run_scenario(title, facade, user_ids, n_each, amount, concurrency):
    print(f"\n=== {title} ===")
    sem = asyncio.Semaphore(concurrency)

    async with httpx.AsyncClient(timeout=60.0) as client:
        await client.post(f"{facade}/metrics/reset")

        async def wrap(uid):
            async with sem:
                await worker(client, facade, uid, n_each, amount)

        t0 = time.perf_counter()
        await asyncio.gather(*[wrap(uid) for uid in user_ids])
        dt = time.perf_counter() - t0

        total = len(user_ids) * n_each
        rps = total / dt if dt else 0.0

        metrics = (await client.get(f"{facade}/metrics")).json()

        print(f"Total requests: {total}")
        print(f"Total time (sec): {dt:.3f}")
        print(f"Requests/sec: {rps:.1f}")
        _print_metrics_breakdown(metrics, dt)

        return {
            "title": title,
            "total_requests": total,
            "total_time_sec": dt,
            "rps": rps,
            "facade_metrics": metrics,
        }

async def get_balances(facade):
    async with httpx.AsyncClient(timeout=30.0) as client:
        r = await client.get(f"{facade}/accounts")
        r.raise_for_status()
        balances = r.json().get("balances", {})
        return {str(k): int(v) for k, v in balances.items()}

async def main():
    parser = argparse.ArgumentParser(description="Performance + correctness tests for facade-service.")
    parser.add_argument("--facade", default=DEFAULT_FACADE, help=f"Facade base URL (default: {DEFAULT_FACADE})")
    parser.add_argument("--clients", type=int, default=10)
    parser.add_argument("--n-each", type=int, default=10_000)
    parser.add_argument("--amount", type=int, default=1)
    parser.add_argument("--concurrency", type=int, default=50)
    args = parser.parse_args()

    run_id = str(int(time.time()))

    # Scenario 1: 10 clients -> 10 different accounts, each should end with n_each * amount
    user_ids_1 = [f"user{i}-{run_id}" for i in range(1, args.clients + 1)]
    await run_scenario(
        "Scenario 1 (10 clients -> 10 accounts)",
        args.facade,
        user_ids_1,
        args.n_each,
        args.amount,
        concurrency=args.concurrency,
    )

    balances_1 = await get_balances(args.facade)
    expected_1 = args.n_each * args.amount
    missing = [u for u in user_ids_1 if balances_1.get(u) != expected_1]
    if missing:
        sample = {u: balances_1.get(u) for u in missing[:3]}
        raise SystemExit(f"Scenario 1 failed: expected balance {expected_1} for each user; mismatches sample={sample}")
    print(f"Scenario 1 correctness OK: {len(user_ids_1)} users have balance={expected_1}")

    # Scenario 2: 10 clients -> 1 shared account, should end with clients * n_each * amount
    shared_user = f"shared-{run_id}"
    user_ids_2 = [shared_user] * args.clients
    await run_scenario(
        "Scenario 2 (10 clients -> 1 shared account)",
        args.facade,
        user_ids_2,
        args.n_each,
        args.amount,
        concurrency=args.concurrency,
    )

    balances_2 = await get_balances(args.facade)
    expected_2 = args.clients * args.n_each * args.amount
    got = balances_2.get(shared_user, 0)
    if got != expected_2:
        raise SystemExit(f"Scenario 2 failed: expected balance {expected_2} for {shared_user}, got {got}")
    print(f"Scenario 2 correctness OK: {shared_user} has balance={expected_2}")

if __name__ == "__main__":
    asyncio.run(main())
