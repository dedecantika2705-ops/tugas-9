"""
=============================================================
  TUGAS #9 - Distributed File Indexing berbasis P2P
  UMC Week 9 | Versi: 4 Node (4 Laptop)
=============================================================
PENGGUNAAN:
  python p2p_node.py <IP_SAYA> <PORT_SAYA>
  Contoh: python p2p_node.py 192.168.1.10 5001

KONFIGURASI WAJIB:
  Edit bagian KNOWN_PEERS di bawah ini dengan IP asli keempat laptop Anda.
=============================================================
"""

import socket
import threading
import sys
import json
import os
import time
from datetime import datetime

# ─────────────────────────────────────────────
# 1. VALIDASI ARGUMEN
# ─────────────────────────────────────────────
if len(sys.argv) < 3:
    print("Format salah! Gunakan: python p2p_node.py <IP_SAYA> <PORT_SAYA>")
    sys.exit()

MY_IP   = sys.argv[1]
MY_PORT = int(sys.argv[2])

# ─────────────────────────────────────────────
# 2. KONFIGURASI JARINGAN (edit IP sesuai kondisi)
# ─────────────────────────────────────────────
KNOWN_PEERS = [
    ('10.1.253.158', 5001),   # Node A - Laptop 1
    ('10.1.253.150', 5002),   # Node B - Laptop 2
    ('10.1.253.146', 5003),   # Node C - Laptop 3
    ('10.1.253.164', 5004),   # Node D - Laptop 4
]

# ─────────────────────────────────────────────
# 3. STATE GLOBAL
# ─────────────────────────────────────────────
local_files   = []          # Daftar file yang di-share node ini
search_counter = 0          # Counter untuk Percobaan 3
lock          = threading.Lock()

# ─────────────────────────────────────────────
# 4. UTILITAS
# ─────────────────────────────────────────────
def log(tag: str, msg: str):
    """Cetak log berformat dengan timestamp."""
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}][{tag}] {msg}")

def save_config():
    """Simpan daftar file lokal ke config.json (Spesifikasi Tugas)."""
    config = {
        "node": {"ip": MY_IP, "port": MY_PORT},
        "files": local_files,
        "peers": [{"ip": ip, "port": port} for ip, port in KNOWN_PEERS]
    }
    with open("config.json", "w") as f:
        json.dump(config, f, indent=2)

def load_config():
    """Muat config.json jika sudah ada."""
    global local_files
    if os.path.exists("config.json"):
        with open("config.json", "r") as f:
            cfg = json.load(f)
            local_files = cfg.get("files", [])
        log("CONFIG", f"File lokal dimuat dari config.json: {local_files}")

# ─────────────────────────────────────────────
# 5. NODE SERVER — menerima koneksi dari peer lain
# ─────────────────────────────────────────────
def handle_client(conn, addr):
    """Tangani satu koneksi masuk dari peer."""
    global search_counter
    try:
        data = conn.recv(4096).decode()

        # ── SEARCH: peer meminta file ──
        if data.startswith("SEARCH:"):
            filename = data.split(":", 1)[1].strip()

            with lock:
                search_counter += 1
                current_count = search_counter

            log("REQUEST", f"Peer {addr} mencari '{filename}' | Total request diterima: {current_count}")

            if filename in local_files:
                reply = f"FOUND:{MY_IP}:{MY_PORT}:{filename}"
                log("FOUND", f"File '{filename}' ADA di node ini → mengirim lokasi ke {addr}")
            else:
                reply = f"NOT_FOUND:{filename}"
                log("NOT_FOUND", f"File '{filename}' tidak ada di node ini")

            conn.send(reply.encode())

        # ── REGISTER: peer meminta kita untuk mendaftarkan file (opsional) ──
        elif data.startswith("REGISTER:"):
            filename = data.split(":", 1)[1].strip()
            with lock:
                if filename not in local_files:
                    local_files.append(filename)
                    save_config()
                    reply = f"REGISTERED:{filename}"
                    log("REGISTER", f"File '{filename}' berhasil didaftarkan dari {addr}")
                else:
                    reply = f"ALREADY_EXISTS:{filename}"
            conn.send(reply.encode())

        # ── PING: cek apakah node aktif ──
        elif data == "PING":
            conn.send(b"PONG")

    except Exception as e:
        log("ERROR", f"Gagal menangani koneksi dari {addr}: {e}")
    finally:
        conn.close()

def server_mode():
    """Jalankan server TCP yang terus mendengarkan koneksi masuk."""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind((MY_IP, MY_PORT))
    s.listen(10)
    log("SERVER", f"Node aktif di {MY_IP}:{MY_PORT} | File lokal: {local_files}")
    while True:
        conn, addr = s.accept()
        threading.Thread(target=handle_client, args=(conn, addr), daemon=True).start()

# ─────────────────────────────────────────────
# 6. NODE CLIENT — mengirim request ke peer lain
# ─────────────────────────────────────────────
def search_file(filename: str):
    """
    Broadcast pencarian ke semua peer yang terdaftar.
    Menghitung total pesan yang dikirim (Percobaan 1).
    """
    log("SEARCH", f"Mencari file '{filename}' ke semua peer...")
    messages_sent = 0
    found_at = []

    for p_ip, p_port in KNOWN_PEERS:
        if p_ip == MY_IP and p_port == MY_PORT:
            # Cek file di node sendiri
            if filename in local_files:
                log("LOCAL", f"File '{filename}' ditemukan di node sendiri!")
                found_at.append(f"{p_ip}:{p_port} (lokal)")
            continue

        try:
            c = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            c.settimeout(2)   # Timeout 2 detik → node mati tidak akan hang
            c.connect((p_ip, p_port))
            c.send(f"SEARCH:{filename}".encode())
            messages_sent += 1

            response = c.recv(1024).decode()
            c.close()

            if response.startswith("FOUND:"):
                parts = response.split(":")
                loc_ip, loc_port, loc_file = parts[1], parts[2], parts[3]
                log("RESPONSE", f"✅ DITEMUKAN → {loc_ip}:{loc_port} memiliki '{loc_file}'")
                found_at.append(f"{loc_ip}:{loc_port}")
            elif response.startswith("NOT_FOUND"):
                log("RESPONSE", f"❌ {p_ip}:{p_port} → file tidak ada")

        except socket.timeout:
            log("TIMEOUT", f"Peer {p_ip}:{p_port} tidak merespons (timeout 2 detik)")
        except ConnectionRefusedError:
            log("ERROR", f"Peer {p_ip}:{p_port} tidak dapat dijangkau (connection refused)")
        except Exception as e:
            log("ERROR", f"Peer {p_ip}:{p_port} → {e}")

    # Ringkasan hasil pencarian
    print(f"\n{'='*50}")
    print(f"  HASIL PENCARIAN: '{filename}'")
    print(f"  Pesan SEARCH dikirim : {messages_sent}")
    print(f"  File ditemukan di    : {found_at if found_at else 'Tidak ditemukan'}")
    print(f"{'='*50}\n")


def register_file(filename: str):
    """
    Daftarkan file ke dalam daftar sharing lokal (Fitur Wajib: register_file).
    File disimpan ke config.json agar persisten.
    """
    with lock:
        if filename not in local_files:
            local_files.append(filename)
            save_config()
            log("REGISTER", f"File '{filename}' berhasil didaftarkan di node ini")
        else:
            log("REGISTER", f"File '{filename}' sudah terdaftar sebelumnya")


def check_peers():
    """Ping semua peer dan tampilkan status jaringan."""
    log("PING", "Memeriksa status semua peer...")
    for p_ip, p_port in KNOWN_PEERS:
        if p_ip == MY_IP and p_port == MY_PORT:
            print(f"  [{MY_IP}:{MY_PORT}] → DIRI SENDIRI (aktif)")
            continue
        try:
            c = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            c.settimeout(1)
            c.connect((p_ip, p_port))
            c.send(b"PING")
            resp = c.recv(10).decode()
            c.close()
            status = "✅ AKTIF" if resp == "PONG" else "⚠️ RESP TIDAK DIKENAL"
        except:
            status = "❌ MATI / TIDAK TERJANGKAU"
        print(f"  [{p_ip}:{p_port}] → {status}")


def show_stats():
    """Tampilkan statistik node saat ini."""
    print(f"\n{'='*50}")
    print(f"  STATISTIK NODE {MY_IP}:{MY_PORT}")
    print(f"  File lokal terdaftar : {local_files}")
    print(f"  Total request masuk  : {search_counter}")
    print(f"  Jumlah peer dikenal  : {len(KNOWN_PEERS) - 1}")
    print(f"{'='*50}\n")

# ─────────────────────────────────────────────
# 7. MAIN — jalankan server & tampilkan menu
# ─────────────────────────────────────────────
def main():
    load_config()
    threading.Thread(target=server_mode, daemon=True).start()
    time.sleep(0.5)  # Beri waktu server bind

    print(f"\n{'='*50}")
    print(f"  P2P NODE AKTIF: {MY_IP}:{MY_PORT}")
    print(f"  Jumlah Peer Dikenal : {len(KNOWN_PEERS)}")
    print(f"{'='*50}")

    while True:
        print("\n─── P2P MENU ───────────────────────────────")
        print("  1. Search File      (broadcast ke semua peer)")
        print("  2. Register File    (daftarkan file lokal)")
        print("  3. Lihat Status Peer")
        print("  4. Lihat Statistik Node")
        print("  5. Exit")
        print("────────────────────────────────────────────")
        cmd = input("Pilihan: ").strip()

        if cmd == '1':
            fname = input("Nama file yang dicari: ").strip()
            if fname:
                search_file(fname)
        elif cmd == '2':
            fname = input("Nama file untuk didaftarkan: ").strip()
            if fname:
                register_file(fname)
        elif cmd == '3':
            check_peers()
        elif cmd == '4':
            show_stats()
        elif cmd == '5':
            log("EXIT", "Node dimatikan.")
            break
        else:
            print("Pilihan tidak valid.")

if __name__ == "__main__":
    main()
