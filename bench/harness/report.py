#!/usr/bin/env python3
"""Parse oha JSON output and generate barkeeper vs etcd comparison tables."""

import json
import os
import sys


def load_result(path):
    try:
        with open(path) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return None


def fmt_ms(seconds):
    """Format seconds as milliseconds string."""
    if seconds is None:
        return "N/A"
    ms = seconds * 1000
    if ms < 1:
        return f"{ms:.3f}ms"
    elif ms < 100:
        return f"{ms:.2f}ms"
    else:
        return f"{ms:.0f}ms"


def extract_metrics(data):
    if not data:
        return {"rps": "N/A", "p50": "N/A", "p95": "N/A", "p99": "N/A", "p999": "N/A",
                "rps_raw": 0, "success_rate": "N/A"}

    summary = data.get("summary", {})
    rps = summary.get("requestsPerSec", 0)
    success = summary.get("successRate", 0)

    percentiles = data.get("latencyPercentiles", {})

    return {
        "rps": f"{rps:,.0f}" if isinstance(rps, (int, float)) and rps > 0 else "N/A",
        "rps_raw": rps if isinstance(rps, (int, float)) else 0,
        "p50": fmt_ms(percentiles.get("p50")),
        "p95": fmt_ms(percentiles.get("p95")),
        "p99": fmt_ms(percentiles.get("p99")),
        "p999": fmt_ms(percentiles.get("p99.9")),
        "success_rate": f"{success * 100:.1f}%" if isinstance(success, (int, float)) else "N/A",
    }


def ratio_str(barkeeper_rps, etcd_rps):
    """Return a ratio string like '1.2x' or '0.8x'."""
    if barkeeper_rps > 0 and etcd_rps > 0:
        ratio = barkeeper_rps / etcd_rps
        return f"{ratio:.2f}x"
    return "N/A"


def generate_report(results_dir):
    stacks = ["barkeeper", "etcd"]
    lines = ["# Barkeeper vs etcd Benchmark Results\n"]
    lines.append("Single-node, same hardware. All requests via HTTP/JSON gateway (`/v3/kv/*`).")
    lines.append("Load generated with [oha](https://github.com/hatoo/oha).\n")

    # ── Write Throughput Ramp ──────────────────────────────────────────
    lines.append("## Write Throughput (PUT /v3/kv/put)\n")
    lines.append("| Concurrency | Barkeeper | etcd | Ratio |")
    lines.append("|---|---|---|---|")

    for c in [1, 10, 50, 100, 500]:
        bk = load_result(os.path.join(results_dir, "barkeeper", f"write_c{c}.json"))
        et = load_result(os.path.join(results_dir, "etcd", f"write_c{c}.json"))
        bk_m = extract_metrics(bk)
        et_m = extract_metrics(et)
        ratio = ratio_str(bk_m["rps_raw"], et_m["rps_raw"])
        lines.append(f"| c={c} | {bk_m['rps']} req/s | {et_m['rps']} req/s | {ratio} |")

    # ── Read Throughput ────────────────────────────────────────────────
    lines.append("\n## Read Throughput (POST /v3/kv/range, c=100, 30s)\n")
    lines.append("| Metric | Barkeeper | etcd |")
    lines.append("|---|---|---|")

    for metric, label in [("rps", "req/s"), ("p50", "P50"), ("p95", "P95"), ("p99", "P99"), ("p999", "P99.9")]:
        bk = load_result(os.path.join(results_dir, "barkeeper", "read_c100.json"))
        et = load_result(os.path.join(results_dir, "etcd", "read_c100.json"))
        lines.append(f"| {label} | {extract_metrics(bk)[metric]} | {extract_metrics(et)[metric]} |")

    # ── Mixed Workload ─────────────────────────────────────────────────
    lines.append("\n## Mixed Workload (80% read / 20% write, 30s)\n")
    lines.append("| Component | Metric | Barkeeper | etcd |")
    lines.append("|---|---|---|---|")

    for component, filename in [("Reads (c=80)", "mixed_read"), ("Writes (c=20)", "mixed_write")]:
        bk = load_result(os.path.join(results_dir, "barkeeper", f"{filename}.json"))
        et = load_result(os.path.join(results_dir, "etcd", f"{filename}.json"))
        bk_m = extract_metrics(bk)
        et_m = extract_metrics(et)
        lines.append(f"| {component} | req/s | {bk_m['rps']} | {et_m['rps']} |")
        lines.append(f"| | P50 | {bk_m['p50']} | {et_m['p50']} |")
        lines.append(f"| | P99 | {bk_m['p99']} | {et_m['p99']} |")

    # ── Large Values ───────────────────────────────────────────────────
    lines.append("\n## Large Values (64KB PUT, c=10, 5s)\n")
    lines.append("| Metric | Barkeeper | etcd |")
    lines.append("|---|---|---|")

    for metric, label in [("rps", "req/s"), ("p50", "P50"), ("p99", "P99"), ("success_rate", "Success")]:
        bk = load_result(os.path.join(results_dir, "barkeeper", "large_values.json"))
        et = load_result(os.path.join(results_dir, "etcd", "large_values.json"))
        lines.append(f"| {label} | {extract_metrics(bk)[metric]} | {extract_metrics(et)[metric]} |")

    # ── Connection Scaling ─────────────────────────────────────────────
    lines.append("\n## Connection Scaling (GET /v3/kv/range)\n")
    lines.append("| Concurrency | Barkeeper | etcd | Ratio |")
    lines.append("|---|---|---|---|")

    for c in [1, 10, 50, 100, 500, 1000]:
        bk = load_result(os.path.join(results_dir, "barkeeper", f"conn_c{c}.json"))
        et = load_result(os.path.join(results_dir, "etcd", f"conn_c{c}.json"))
        bk_m = extract_metrics(bk)
        et_m = extract_metrics(et)
        ratio = ratio_str(bk_m["rps_raw"], et_m["rps_raw"])
        lines.append(f"| c={c} | {bk_m['rps']} req/s | {et_m['rps']} req/s | {ratio} |")

    report = "\n".join(lines) + "\n"
    report_path = os.path.join(results_dir, "RESULTS.md")
    with open(report_path, "w") as f:
        f.write(report)

    # CSV export
    csv_path = os.path.join(results_dir, "results.csv")
    with open(csv_path, "w") as f:
        f.write("scenario,metric,barkeeper,etcd\n")
        for c in [1, 10, 50, 100, 500]:
            bk = load_result(os.path.join(results_dir, "barkeeper", f"write_c{c}.json"))
            et = load_result(os.path.join(results_dir, "etcd", f"write_c{c}.json"))
            f.write(f"write_c{c},rps,{extract_metrics(bk)['rps']},{extract_metrics(et)['rps']}\n")
        for scenario in ["read_c100", "mixed_read", "mixed_write", "large_values"]:
            for metric in ["rps", "p50", "p95", "p99"]:
                bk = load_result(os.path.join(results_dir, "barkeeper", f"{scenario}.json"))
                et = load_result(os.path.join(results_dir, "etcd", f"{scenario}.json"))
                f.write(f"{scenario},{metric},{extract_metrics(bk).get(metric, 'N/A')},{extract_metrics(et).get(metric, 'N/A')}\n")
        for c in [1, 10, 50, 100, 500, 1000]:
            bk = load_result(os.path.join(results_dir, "barkeeper", f"conn_c{c}.json"))
            et = load_result(os.path.join(results_dir, "etcd", f"conn_c{c}.json"))
            f.write(f"conn_c{c},rps,{extract_metrics(bk)['rps']},{extract_metrics(et)['rps']}\n")

    print(report)
    print(f"Report: {report_path}")
    print(f"CSV: {csv_path}")


if __name__ == "__main__":
    results_dir = sys.argv[1] if len(sys.argv) > 1 else "results"
    generate_report(results_dir)
