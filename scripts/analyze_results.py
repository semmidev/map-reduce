#!/usr/bin/env python3
"""
Analyzes MapReduce output and displays a threat intelligence report.

Usage:
    python3 analyze_results.py [output_dir]
"""

import os
import sys
import json
from collections import defaultdict

def load_results(output_dir):
    """Load all mr-out-* files from the output directory"""
    results = defaultdict(dict)
    
    for fname in sorted(os.listdir(output_dir)):
        if not fname.startswith("mr-out-"):
            continue
        
        filepath = os.path.join(output_dir, fname)
        with open(filepath) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                parts = line.split("\t", 1)
                if len(parts) != 2:
                    continue
                key, value_str = parts
                try:
                    value = json.loads(value_str)
                except json.JSONDecodeError:
                    value = value_str
                
                # Parse key category
                colon_idx = key.find(":")
                if colon_idx >= 0:
                    category = key[:colon_idx]
                    sub_key = key[colon_idx+1:]
                    if category not in results:
                        results[category] = {}
                    results[category][sub_key] = value
    
    return results

def print_separator(char="═", width=70):
    print(char * width)

def print_header(title):
    print_separator()
    print(f"  {title}")
    print_separator()

def analyze(output_dir):
    print("\n")
    print("╔" + "═"*68 + "╗")
    print("║" + " "*18 + "🔐 THREAT INTELLIGENCE REPORT" + " "*21 + "║")
    print("║" + " "*15 + "MapReduce Cyber Log Analysis" + " "*25 + "║")
    print("╚" + "═"*68 + "╝\n")
    
    results = load_results(output_dir)
    
    if not results:
        print("  ⚠️  No results found in", output_dir)
        return
    
    # ── 1. Top Attacker IPs ──
    print_header("📡 TOP ATTACKER IPs (by request volume)")
    attacker_ips = results.get("ATTACKER_IP", {})
    sorted_ips = sorted(
        attacker_ips.items(),
        key=lambda x: x[1].get("requests", 0) if isinstance(x[1], dict) else 0,
        reverse=True
    )
    
    if sorted_ips:
        print(f"  {'IP Address':<22} {'Requests':>10}  {'Severity':<12}")
        print("  " + "-"*48)
        for ip, data in sorted_ips[:15]:
            if isinstance(data, dict):
                req = data.get("requests", 0)
                sev = data.get("severity", "?")
                icon = {"CRITICAL": "🔴", "HIGH": "🟠", "MEDIUM": "🟡", "LOW": "🟢"}.get(sev, "⚪")
                print(f"  {ip:<22} {req:>10,}  {icon} {sev}")
    print()
    
    # ── 2. Attack Types ──
    print_header("⚔️  ATTACK TYPE BREAKDOWN")
    attack_types = results.get("ATTACK_TYPE", {})
    sorted_attacks = sorted(
        attack_types.items(),
        key=lambda x: x[1].get("total_hits", 0) if isinstance(x[1], dict) else 0,
        reverse=True
    )
    
    icons = {
        "SQL_INJECTION": "💉",
        "XSS": "🕷️",
        "PATH_TRAVERSAL": "🗂️",
        "ACTIVE_SCANNER": "🔍",
    }
    
    if sorted_attacks:
        print(f"  {'Attack Type':<22} {'Total Hits':>12} {'Unique IPs':>12}")
        print("  " + "-"*50)
        for attack_type, data in sorted_attacks:
            if isinstance(data, dict):
                hits = data.get("total_hits", 0)
                unique = data.get("unique_ips", 0)
                icon = icons.get(attack_type, "⚠️")
                print(f"  {icon} {attack_type:<20} {hits:>12,} {unique:>12,}")
    print()
    
    # ── 3. Most Targeted Endpoints ──
    print_header("🎯 MOST TARGETED ENDPOINTS")
    endpoints = results.get("TARGET_ENDPOINT", {})
    sorted_eps = sorted(
        endpoints.items(),
        key=lambda x: x[1].get("hits", 0) if isinstance(x[1], dict) else 0,
        reverse=True
    )
    
    if sorted_eps:
        print(f"  {'Endpoint':<45} {'Hits':>8} {'Risk':<8}")
        print("  " + "-"*65)
        for ep, data in sorted_eps[:12]:
            if isinstance(data, dict):
                hits = data.get("hits", 0)
                risk = data.get("risk_level", "?")
                risk_icon = "🔴" if risk == "HIGH" else "🟢"
                print(f"  {ep[:44]:<45} {hits:>8,} {risk_icon} {risk}")
    print()
    
    # ── 4. Suspicious User Agents ──
    print_header("🤖 SUSPICIOUS USER AGENTS (Automated Tools Detected)")
    uas = results.get("SUSPICIOUS_UA", {})
    sorted_uas = sorted(
        uas.items(),
        key=lambda x: x[1].get("total_requests", 0) if isinstance(x[1], dict) else 0,
        reverse=True
    )
    
    if sorted_uas:
        print(f"  {'Tool/Agent':<25} {'Requests':>10} {'Sources':>10}")
        print("  " + "-"*50)
        for ua, data in sorted_uas:
            if isinstance(data, dict):
                reqs = data.get("total_requests", 0)
                srcs = data.get("unique_sources", 0)
                print(f"  {ua:<25} {reqs:>10,} {srcs:>10,}")
    print()
    
    # ── 5. Error Status Floods ──
    print_header("🌊 ERROR STATUS CODE FLOODS")
    status_floods = results.get("STATUS_FLOOD", {})
    sorted_status = sorted(
        status_floods.items(),
        key=lambda x: x[1].get("total_errors", 0) if isinstance(x[1], dict) else 0,
        reverse=True
    )
    
    status_meanings = {
        "400": "Bad Request", "401": "Unauthorized", "403": "Forbidden",
        "404": "Not Found", "429": "Too Many Requests", "500": "Server Error",
        "503": "Service Unavailable",
    }
    
    if sorted_status:
        print(f"  {'Code':<6} {'Meaning':<22} {'Total Errors':>14} {'Unique IPs':>12}")
        print("  " + "-"*58)
        for code, data in sorted_status:
            if isinstance(data, dict):
                errors = data.get("total_errors", 0)
                unique = data.get("unique_ips", 0)
                meaning = status_meanings.get(code, "Unknown")
                print(f"  {code:<6} {meaning:<22} {errors:>14,} {unique:>12,}")
    print()
    
    # ── 6. Active Scanners ──
    print_header("🔍 ACTIVE RECONNAISSANCE / SCANNERS")
    scanners = results.get("SCANNER_IP", {})
    sorted_scanners = sorted(
        scanners.items(),
        key=lambda x: x[1].get("unique_paths_scanned", 0) if isinstance(x[1], dict) else 0,
        reverse=True
    )
    
    if sorted_scanners:
        print(f"  {'IP Address':<22} {'Unique Paths Scanned':>22}")
        print("  " + "-"*46)
        for ip, data in sorted_scanners[:10]:
            if isinstance(data, dict):
                paths = data.get("unique_paths_scanned", 0)
                samples = data.get("sample_paths", [])
                print(f"  {ip:<22} {paths:>22,}")
                if samples:
                    for s in samples[:2]:
                        print(f"  {'':22}   ↳ {s}")
    print()
    
    print_separator("─")
    print(f"  Report generated from: {output_dir}")
    print(f"  Categories analyzed  : {len(results)}")
    print_separator("─")
    print()

if __name__ == "__main__":
    output_dir = sys.argv[1] if len(sys.argv) > 1 else "/output"
    analyze(output_dir)
