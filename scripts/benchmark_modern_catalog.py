#!/usr/bin/env python3
import time
import subprocess
import statistics
import os
import sys

def benchmark_binary(name, cmd_args, iterations=20):
    latencies = []
    for _ in range(iterations):
        t0 = time.perf_counter()
        res = subprocess.run(cmd_args, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        t1 = time.perf_counter()
        if res.returncode != 0:
            print(f"Error running {name}: return code {res.returncode}", file=sys.stderr)
            return None
        latencies.append((t1 - t0) * 1000.0) # milliseconds

    mean_ms = statistics.mean(latencies)
    stdev_ms = statistics.stdev(latencies) if len(latencies) > 1 else 0.0
    p95_ms = statistics.quantiles(latencies, n=20)[18] if len(latencies) >= 20 else max(latencies)
    throughput = 1000.0 / mean_ms

    print(f"[{name:<32}] mean={mean_ms:6.2f}ms (stdev={stdev_ms:5.2f}ms) p95={p95_ms:6.2f}ms throughput={throughput:6.1f} runs/sec")
    return {
        "name": name,
        "iterations": iterations,
        "mean_ms": mean_ms,
        "stdev_ms": stdev_ms,
        "p95_ms": p95_ms,
        "throughput": throughput
    }

def main():
    build_dir = "/home/darkfell/dev/irods/build/unit_tests"
    benchmarks = [
        ("nanodbc_executor_statement_cache", [os.path.join(build_dir, "irods_nanodbc_executor")]),
        ("genquery2_builder_ast", [os.path.join(build_dir, "irods_genquery2_builder")]),
        ("genquery2_sql_dml_gen", [os.path.join(build_dir, "irods_genquery2_sql_dml")]),
        ("chl_data_obj_modern_ops", [os.path.join(build_dir, "irods_chl_data_obj_modern")]),
        ("chl_coll_modern_ops", [os.path.join(build_dir, "irods_chl_coll_modern")]),
        ("chl_resc_modern_ops", [os.path.join(build_dir, "irods_chl_resc_modern")]),
        ("chl_user_group_modern_ops", [os.path.join(build_dir, "irods_chl_user_group_modern")]),
        ("chl_delay_rule_modern_ops", [os.path.join(build_dir, "irods_chl_delay_rule_modern")]),
        ("chl_misc_modern_ops", [os.path.join(build_dir, "irods_chl_misc_modern")]),
    ]

    print("================================================================================")
    print("ICAT Modernization: GenQuery2 & Type-Safe ODBC Performance Microbenchmark Suite")
    print("================================================================================")

    results = []
    for name, args in benchmarks:
        if os.path.exists(args[0]):
            res = benchmark_binary(name, args, iterations=25)
            if res:
                results.append(res)
        else:
            print(f"Skipping {name}: binary not found at {args[0]}")

    print("================================================================================")
    print("Microbenchmarks complete.")

if __name__ == "__main__":
    main()
