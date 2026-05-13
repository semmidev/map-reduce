# MapReduce — Distributed Cyber Attack Log Analysis

An educational **distributed MapReduce simulation** built with Go and orchestrated using **Docker Compose**, where each container emulates an independent machine communicating over Docker’s internal DNS network.

**Use Case**: Demonstrates parallel processing of large-scale server access logs to identify cyber attack patterns — including SQL injection, XSS, path traversal, and active scanners — and generate a structured **Threat Intelligence Report**.

---

## Daftar Isi

1. [Apa itu MapReduce?](#apa-itu-mapreduce)
2. [Cara Kerja — Penjelasan Mendalam](#cara-kerja--penjelasan-mendalam)
3. [Arsitektur](#arsitektur)
4. [Use Case: Analisis Log Serangan Siber](#use-case-analisis-log-serangan-siber)
5. [Struktur Proyek](#struktur-proyek)
6. [Menjalankan Kluster](#menjalankan-kluster)
7. [Internal Kluster](#internal-kluster)
8. [Toleransi Kesalahan](#toleransi-kesalahan)
9. [Menambahkan Job Baru](#menambahkan-job-baru)

---

## Apa itu MapReduce?

MapReduce adalah **model pemrograman untuk memproses dataset besar secara paralel** di atas kluster mesin. Pertama kali dideskripsikan dalam [makalah Google tahun 2004](https://research.google/pubs/pub62/), model ini memecah komputasi menjadi dua fase:

```
File Input
    │
    ▼
┌──────────┐   ┌──────────┐   ┌──────────┐
│  Map(f)  │   │  Map(f)  │   │  Map(f)  │  ← Paralel, satu per chunk
└────┬─────┘   └────┬─────┘   └────┬─────┘
     │               │               │
     └───────────────┼───────────────┘
                     │
               Shuffle & Sort
                     │
     ┌───────────────┼───────────────┐
     │               │               │
┌────▼─────┐   ┌────▼─────┐   ┌────▼─────┐
│Reduce(f) │   │Reduce(f) │   │Reduce(f) │  ← Paralel, satu per bucket
└────┬─────┘   └────┬─────┘   └────┬─────┘
     │               │               │
     └───────────────┼───────────────┘
                     │
                     ▼
               Output Akhir
```

### Dua Operasi Utama

| Operasi    | Input                 | Output       | Yang perlu kamu tulis                         |
| ---------- | --------------------- | ------------ | --------------------------------------------- |
| **Map**    | `(filename, content)` | `[]KeyValue` | Emit satu pasang KV per sinyal yang ditemukan |
| **Reduce** | `(key, []values)`     | `string`     | Agregasi semua nilai untuk satu key           |

**Framework menangani semua yang ada di antaranya**: partisi, shuffling, pengurutan, paralelisme, pemulihan dari kegagalan, dan penugasan task.

---

## Cara Kerja — Penjelasan Mendalam

### Fase 1: MAP

```
                  ┌──────────────────────────────────────────┐
                  │                 MASTER                   │
                  │                                          │
                  │  mapTasks = [                            │
  File Input      │    { id:0, file: "server-1.log" },       │
  server-1.log ──►│    { id:1, file: "server-2.log" },       │
  server-2.log ──►│    { id:2, file: "server-3.log" },       │
  server-3.log ──►│    ...                                   │
                  │  ]                                       │
                  └──────────────┬───────────────────────────┘
                                 │ Distribusi task
                  ┌──────────────┼──────────────┐
                  │              │              │
            ┌─────▼────┐  ┌─────▼────┐  ┌─────▼────┐
            │ Worker-1 │  │ Worker-2 │  │ Worker-3 │
            │          │  │          │  │          │
            │ map(     │  │ map(     │  │ map(     │
            │  file-0, │  │  file-1, │  │  file-2, │
            │  content │  │  content │  │  content │
            │ )        │  │ )        │  │ )        │
            └────┬─────┘  └────┬─────┘  └────┬─────┘
                 │              │              │
           Emit []KV      Emit []KV      Emit []KV
           (key, val)     (key, val)     (key, val)
                 │              │              │
                 └──────────────┼──────────────┘
                                │
                     Partisi berdasarkan hash key
                     ke dalam NReduce bucket

  File intermediate ditulis ke /tmp/:
  ┌───────────┬───────────┬───────────┬───────────┐
  │mr-map-0-0 │mr-map-0-1 │mr-map-0-2 │mr-map-0-3 │ ← mapID=0
  │mr-map-1-0 │mr-map-1-1 │mr-map-1-2 │mr-map-1-3 │ ← mapID=1
  │mr-map-2-0 │mr-map-2-1 │mr-map-2-2 │mr-map-2-3 │ ← mapID=2
  └───────────┴───────────┴───────────┴───────────┘
       ▲                        ▲
  reduceID=0              reduceID=2
```

**Partisi**: Setiap pasangan key-value diarahkan ke bucket reduce menggunakan hash:
```go
bucket := ihash(kv.Key) % task.NReduce
```
Ini memastikan semua nilai untuk key yang sama selalu berakhir di task reduce yang sama — tidak peduli worker mana yang menghasilkannya.

---

### Fase 2: SHUFFLE & SORT (otomatis)

Master melacak file intermediate mana yang termasuk ke bucket reduce mana. Setelah semua map task selesai, master beralih ke fase REDUCE. Setiap reduce task menerima semua file intermediate untuk bucket-nya:

```
Reduce task 0 membaca:  [ mr-map-0-0, mr-map-1-0, mr-map-2-0, ... ]
Reduce task 1 membaca:  [ mr-map-0-1, mr-map-1-1, mr-map-2-1, ... ]
Reduce task 2 membaca:  [ mr-map-0-2, mr-map-1-2, mr-map-2-2, ... ]
```

Worker membaca semuanya, **mengurutkan berdasarkan key**, lalu mengelompokkan key yang identik secara berurutan — sehingga fungsi reduce melihat:

```
key="ATTACKER_IP:185.220.101.45"   values=["1", "1", "1", ...]
key="ATTACKER_IP:45.33.201.11"     values=["1", "1", ...]
key="ATTACK_TYPE:SQL_INJECTION"    values=["ip1", "ip2", "ip3", ...]
```

---

### Fase 3: REDUCE

```
                  ┌──────────────────────────────────────────┐
                  │                 MASTER                   │
  File            │                                          │
  intermediate    │  reduceTasks = [                         │
  per bucket  ───►│    { id:0, files: [...bucket-0...] },    │
                  │    { id:1, files: [...bucket-1...] },    │
                  │    ...                                   │
                  └──────────────┬───────────────────────────┘
                                 │ Distribusi task
                  ┌──────────────┼──────────────┐
                  │              │              │
            ┌─────▼────┐  ┌─────▼────┐  ┌─────▼────┐
            │ Worker-1 │  │ Worker-2 │  │ Worker-3 │
            │          │  │          │  │          │
            │ reduce(  │  │ reduce(  │  │ reduce(  │
            │  key,    │  │  key,    │  │  key,    │
            │  []vals  │  │  []vals  │  │  []vals  │
            │ )        │  │ )        │  │ )        │
            └────┬─────┘  └────┬─────┘  └────┬─────┘
                 │              │              │
            mr-out-0       mr-out-1       mr-out-2
```

File output akhir berupa teks biasa, satu hasil per baris:
```
ATTACKER_IP:185.220.101.45   {"requests":12043,"severity":"CRITICAL"}
ATTACK_TYPE:SQL_INJECTION    {"total_hits":8291,"unique_ips":47}
TARGET_ENDPOINT:/admin/login {"hits":3102,"risk_level":"HIGH"}
```

---

## Arsitektur

### Gambaran Komponen

```
┌──────────────────────────────────────────────────────────────────┐
│                    Docker Compose Network                        │
│                    (mapreduce-cluster)                           │
│                                                                  │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │  MASTER  (container: master, DNS: master:8080)            │  │
│  │                                                           │  │
│  │  HTTP Endpoints:                                          │  │
│  │    POST /register     ← Worker mendaftarkan diri          │  │
│  │    POST /get-task     ← Worker meminta task               │  │
│  │    POST /report-task  ← Worker melaporkan hasil           │  │
│  │    POST /heartbeat    ← Worker membuktikan masih hidup    │  │
│  │    GET  /status       ← Monitoring / query manual         │  │
│  │                                                           │  │
│  │  State internal:                                          │  │
│  │    mapTasks[]     → Fase & status setiap map task         │  │
│  │    reduceTasks[]  → Registri file intermediate            │  │
│  │    workers{}      → Registri worker + heartbeat           │  │
│  │    watchdog()     → Goroutine: deteksi task timeout       │  │
│  └──────────────────────────────┬────────────────────────────┘  │
│                                 │ HTTP RPC                       │
│             ┌───────────────────┼───────────────────┐           │
│             │                   │                   │           │
│  ┌──────────▼──────┐  ┌─────────▼──────┐  ┌────────▼───────┐  │
│  │ worker-         │  │ worker-        │  │ worker-        │  │
│  │ jakarta-1       │  │ singapore-1    │  │ us-east-1      │  │
│  │                 │  │                │  │                │  │
│  │  1. Register    │  │  1. Register   │  │  1. Register   │  │
│  │  2. Poll task   │  │  2. Poll task  │  │  2. Poll task  │  │
│  │  3. Execute     │  │  3. Execute    │  │  3. Execute    │  │
│  │  4. Report      │  │  4. Report     │  │  4. Report     │  │
│  │  5. Heartbeat   │  │  5. Heartbeat  │  │  5. Heartbeat  │  │
│  └─────────────────┘  └────────────────┘  └────────────────┘  │
│                                                                  │
│  Shared volumes:                                                 │
│    data-vol   → /data    (log input, read-only untuk worker)    │
│    output-vol → /output  (output akhir, writable untuk worker)  │
└──────────────────────────────────────────────────────────────────┘
```

### Siklus Hidup Worker

```
Start
  │
  ▼
Daftar ke master ──► (retry sampai master siap)
  │
  ▼
Jalankan goroutine heartbeat (setiap 5 detik)
  │
  ▼
┌────────────────────┐
│   Poll: GET TASK   │◄─────────────────────────┐
└─────────┬──────────┘                          │
          │                                     │
    ┌─────┴──────────────────┐                 │
    │            │            │                 │
  WAIT        MAP/REDUCE    DONE                │
    │            │            │                 │
  sleep 1s      │          keluar               │
    │       Jalankan task   dengan graceful      │
    └►poll       │                              │
    lagi    Laporkan hasil ────────────────────►┘
```

---

## Use Case: Analisis Log Serangan Siber

### Masalahnya

Sebuah perusahaan menjalankan 6 server di Jakarta, Singapura, dan US East/West. Setiap server menghasilkan ribuan baris access log per jam. Tim keamanan perlu:

- Mengidentifikasi **IP botnet** yang membanjiri infrastruktur
- Mendeteksi **pola serangan** (SQLi, XSS, path traversal)
- Menemukan **endpoint yang paling sering diserang** (panel admin, file konfigurasi)
- Menandai **tool berbahaya** (sqlmap, nikto, masscan)
- Mendeteksi **active scanner** yang memetakan permukaan serangan

Melakukan ini secara serial di satu mesin bisa memakan waktu berjam-jam. MapReduce menyelesaikannya dalam **hitungan detik di seluruh kluster**.

### Format Log

Format Combined Log standar (nginx/apache):
```
185.220.101.45 - - [13/May/2026:10:23:44 +0700] "GET /login?user=' OR '1'='1 HTTP/1.1" 200 4521 "-" "sqlmap/1.7"
45.33.201.11 - - [13/May/2026:10:23:45 +0700] "GET /.env HTTP/1.1" 404 162 "-" "Go-http-client/1.1"
103.41.22.15 - - [13/May/2026:10:23:46 +0700] "GET /dashboard HTTP/1.1" 200 8102 "-" "Mozilla/5.0..."
```

### Fungsi Map

Untuk setiap baris log, fungsi map mengemisikan **sinyal ancaman** sebagai pasangan key-value:

```go
// Satu baris log seperti:
// 185.220.101.45 ... "GET /login?user='+OR+'1'='1 ..." 200 ... "sqlmap/1.7"
// Menghasilkan beberapa sinyal:

{ Key: "ATTACKER_IP:185.220.101.45",        Value: "1"              }
{ Key: "ATTACK_TYPE:SQL_INJECTION",         Value: "185.220.101.45" }
{ Key: "IP_ATTACK:185.220.101.45:SQL_INJECTION", Value: "1"        }
{ Key: "TARGET_ENDPOINT:/login",            Value: "1"              }
{ Key: "SUSPICIOUS_UA:sqlmap",              Value: "185.220.101.45" }
{ Key: "STATUS_FLOOD:200",                  Value: "185.220.101.45" }
```

### Fungsi Reduce

Fungsi reduce mengagregasi semua nilai per key menjadi data ancaman terstruktur:

```go
// Untuk key="ATTACKER_IP:185.220.101.45", values=["1","1","1",...] (12.043 kali):
→ `{"requests":12043,"severity":"CRITICAL"}`

// Untuk key="ATTACK_TYPE:SQL_INJECTION", values=["ip1","ip2",...] (8.291 total):
→ `{"total_hits":8291,"unique_ips":47}`

// Untuk key="SUSPICIOUS_UA:sqlmap", values=["ip1","ip2",...]:
→ `{"total_requests":892,"unique_sources":3}`
```

### Contoh Output

Setelah job selesai, `python3 scripts/analyze_results.py /output` menampilkan:

```
╔════════════════════════════════════════════════════════════════════╗
║                  🔐 THREAT INTELLIGENCE REPORT                    ║
║                  MapReduce Cyber Log Analysis                     ║
╚════════════════════════════════════════════════════════════════════╝

══════════════════════════════════════════════════════════════════════
  📡 TOP ATTACKER IPs (berdasarkan volume request)
══════════════════════════════════════════════════════════════════════
  IP Address               Requests  Severity
  ────────────────────────────────────────────────
  185.220.101.45             12.043  🔴 CRITICAL
  45.33.201.11                3.891  🟠 HIGH
  185.220.156.20              2.211  🟠 HIGH
  ...

══════════════════════════════════════════════════════════════════════
  ⚔️  BREAKDOWN JENIS SERANGAN
══════════════════════════════════════════════════════════════════════
  Jenis Serangan            Total Hit   Unique IP
  ──────────────────────────────────────────────────
  💉 SQL_INJECTION               8.291          47
  🔍 ACTIVE_SCANNER              5.102          23
  🗂️  PATH_TRAVERSAL              3.441          31
  🕷️  XSS                         1.892          18
```

---

## Struktur Proyek

```
map-reduce/
├── cmd/
│   ├── master/
│   │   └── main.go          # Entrypoint master — baca INPUT_DIR, jalankan server
│   └── worker/
│       └── main.go          # Entrypoint worker — koneksi ke MASTER_HOST
│
├── internal/
│   ├── common/
│   │   └── types.go         # Tipe bersama: Task, KeyValue, struct RPC
│   ├── master/
│   │   └── master.go        # Logika master: penjadwalan task, watchdog, handler RPC
│   └── worker/
│       └── worker.go        # Logika worker: eksekusi task, heartbeat, file I/O
│
├── jobs/
│   └── cyberlog.go          # 🔐 Fungsi Map & Reduce (logika bisnis kamu)
│
├── scripts/
│   ├── generate_logs.py     # Membuat log serangan realistis (Python, jalan di Docker)
│   └── analyze_results.py   # Memformat output MapReduce sebagai laporan ancaman
│
├── Dockerfile.master        # Multi-stage build untuk binary master
├── Dockerfile.worker        # Multi-stage build untuk binary worker
├── docker-compose.yml       # Definisi kluster lengkap (6 worker + master + tooling)
├── Makefile                 # Shortcut manajemen kluster
└── go.mod
```

**Insight utama**: `jobs/cyberlog.go` hanya berisi **logika bisnis kamu** — dua fungsi `CyberLogMap` dan `CyberLogReduce`. Semua yang lain (paralelisme, toleransi kesalahan, shuffling, komunikasi jaringan) ditangani oleh framework.

---

## Menjalankan Kluster

### Prasyarat

- Docker Engine 24+
- Docker Compose v2
- Python 3 (untuk pembuatan log dan analisis hasil)
- RAM 4 GB disarankan (6 worker + master)

### Quick Start

```bash
# 1. Clone dan masuk ke direktori
git clone https://github.com/semmidev/map-reduce
cd map-reduce

# 2. Build semua image
make build
# atau: docker compose build

# 3. Jalankan kluster lengkap
make up
# Ini menjalankan secara berurutan:
#   a. log-generator  → membuat 48.000 baris log di 6 file
#   b. master         → mulai melayani RPC di :8080
#   c. 6 worker       → daftar, poll task, eksekusi

# 4. Pantau prosesnya
make logs
# atau ikuti container tertentu:
make logs-master
make logs-workers

# 5. Cek progres job
make status
# Output:
#   Phase     : MAP
#   Map       : 3/6 selesai
#   Reduce    : 0/4 selesai
#   Workers   : 6
#     - worker-jakarta-1 [BUSY] tasks_done=1
#     - worker-singapore-1 [IDLE] tasks_done=2
#     ...

# 6. Setelah job selesai — lihat laporan ancaman
make analyze

# 7. Matikan kluster
make down
```

### Mengakses Master API

Selama kluster berjalan, kamu bisa query master secara langsung:

```bash
# Status job (JSON)
curl http://localhost:8080/status | jq .

# Contoh respons:
{
  "phase": "REDUCE",
  "total_map_tasks": 6,
  "done_map_tasks": 6,
  "total_reduce_tasks": 4,
  "done_reduce_tasks": 2,
  "workers": [
    {
      "id": "worker-jakarta-1",
      "status": "BUSY",
      "tasks_handled": 2,
      "current_task": { "id": 1, "type": "REDUCE" }
    },
    ...
  ]
}
```

### Menambah Worker

Untuk menambah worker, duplikat salah satu blok worker di `docker-compose.yml` dengan nama dan `WORKER_ID` baru, lalu:

```bash
docker compose up -d worker-mesin-baru
```

Worker baru akan otomatis mendaftar ke master dan mulai menerima task. **Tidak perlu merestart master atau worker lain.**

### Menyesuaikan Job

| Environment Variable | Default   | Keterangan                                   |
| -------------------- | --------- | -------------------------------------------- |
| `N_REDUCE`           | `4`       | Jumlah partisi reduce (= jumlah file output) |
| `INPUT_DIR`          | `/data`   | Direktori yang dipindai untuk file `*.log`   |
| `OUTPUT_DIR`         | `/output` | Tempat file `mr-out-*` ditulis               |
| `MASTER_HOST`        | `master`  | Nama service Docker untuk master             |
| `MASTER_PORT`        | `8080`    | Port HTTP master                             |

---

## Internal Kluster

### Bagaimana Docker DNS Menggerakkan Kluster

Semua container berbagi jaringan bridge `mapreduce-cluster`. Server DNS bawaan Docker secara otomatis **memetakan nama service ke IP container**:

```
worker-jakarta-1 → MASTER_HOST=master → di-resolve ke 172.28.0.X
```

Artinya:
- Worker tidak perlu tahu IP hardcoded master
- Worker tidak perlu tahu mesin mana yang menjalankan master
- Menambah worker baru cukup dengan mengatur `MASTER_HOST=master` — tidak ada konfigurasi lain
- Ini mencerminkan cara kerja Kubernetes service di produksi (`serviceName.namespace.svc.cluster.local`)

### Identifikasi Worker

**Hostname setiap container menjadi Worker ID-nya**:

```yaml
worker-jakarta-1:
  hostname: worker-jakarta-1
  environment:
    WORKER_ID: "worker-jakarta-1"
```

Master melacak worker berdasarkan ID dan mencatat mesin mana yang memproses task mana:
```
[MASTER] Assigned MAP task 3 → worker-singapore-2 (file: web-server-jakarta.log)
[MASTER] Task 3 COMPLETED by worker-singapore-2 in 0.83s
```

---

## Toleransi Kesalahan

### Timeout Task & Re-queue

Master menjalankan **goroutine watchdog** yang berjalan setiap 5 detik. Jika sebuah task sudah dalam status "sedang dikerjakan" selama lebih dari 30 detik tanpa penyelesaian, task tersebut otomatis direset ke `IDLE` dan ditugaskan ke worker lain:

```
[MASTER] ⏰ MAP task 2 timed out (worker-us-east-1), re-queueing
```

Mekanisme ini menangani:
- Worker yang crash di tengah eksekusi task
- Partisi jaringan
- Container yang lambat atau kelebihan beban

### Heartbeat Worker

Setiap worker mengirim POST heartbeat ke `/heartbeat` setiap 5 detik. Jika master tidak mendengar dari worker selama 15 detik, worker tersebut ditandai `DEAD`:

```
[MASTER] 💀 Worker worker-us-west-1 heartbeat timeout, marking dead
```

Task yang ditugaskan ke worker mati secara otomatis di-re-queue oleh watchdog.

### Output Idempoten

Map task menulis file intermediate ke `/tmp/mr-map-<mapID>-<reduceID>`. Jika map task yang sama dijalankan dua kali (karena timeout), eksekusi kedua cukup menimpa yang pertama — outputnya deterministik. Reduce task pun demikian, file outputnya akan ditimpa.

---

## Menambahkan Job Baru

Untuk menganalisis dataset berbeda (misalnya clickstream e-commerce, log DNS, transaksi keuangan):

**1. Buat file baru di `jobs/`:**

```go
// jobs/clickstream.go
package jobs

import "github.com/semmidev/map-reduce/internal/common"

func ClickstreamMap(filename, content string) []common.KeyValue {
    var kvs []common.KeyValue
    // Parse setiap baris, emit sinyal:
    // { Key: "USER_FUNNEL:checkout_abandoned", Value: userID }
    // { Key: "PRODUCT_VIEW:product-123",       Value: "1" }
    return kvs
}

func ClickstreamReduce(key string, values []string) string {
    // Agregasi: hitung, user unik, conversion rate, dll.
    return result
}
```

**2. Update `cmd/worker/main.go`** untuk menyambungkan fungsi baru:

```go
w := worker.New(workerID, masterAddr, jobs.ClickstreamMap, jobs.ClickstreamReduce)
```

**3. Update `cmd/master/main.go`** untuk mengarahkan ke file input baru:

```go
inputFiles, _ = filepath.Glob("/data/*.json")  // atau format apapun yang kamu butuhkan
```

Selesai. Seluruh eksekusi terdistribusi, toleransi kesalahan, dan pengumpulan output ditangani secara otomatis.

---

## Keputusan Desain Utama

### Mengapa HTTP dan bukan gRPC/raw TCP?

HTTP + JSON membuat sistem ini **bisa di-debug dengan curl**, bisa diobservasi tanpa tooling khusus, dan bekerja secara alami dengan jaringan Docker. Di sistem produksi, kamu bisa mengganti dengan gRPC untuk efisiensi biner — antarmuka (tipe di `internal/common/types.go`) adalah kontrak yang stabil.

### Mengapa pull-based (worker polling ke master) dan bukan push?

Penugasan task berbasis pull membuat worker dapat mengatur dirinya sendiri. Worker yang lambat cukup polling lebih jarang — master tidak perlu mengetahui kapasitas worker di muka. Ini juga menyederhanakan penanganan kesalahan: worker yang tidak polling dianggap mati.

### Mengapa sort sebelum reduce?

Mengurutkan pasangan KV intermediate berdasarkan key (MapReduce kanonik) berarti fungsi reduce menerima semua nilai untuk satu key secara berurutan — memungkinkan agregasi streaming sederhana tanpa hash map. Ini juga membuat output deterministik dan file intermediate lebih mudah di-debug.

---

## Lisensi

MIT — gunakan dengan bebas, pelajari dengan mendalam.
