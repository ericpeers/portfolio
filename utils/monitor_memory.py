#!/usr/bin/env python3
import psutil
import time
import sys
import os

def format_bytes(n):
    for unit in ['B', 'KiB', 'MiB', 'GiB', 'TiB']:
        if n < 1024:
            return f"{n:7.2f} {unit}"
        n /= 1024

def main():
    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} <process_substring>")
        sys.exit(1)

    target_substring = sys.argv[1]
    min_rss = float('inf')
    max_rss = 0.0

    print(f"Monitoring processes matching: '{target_substring}'")
    print("Press Ctrl+C to stop.\n")

    try:
        while True:
            current_rss = 0
            matching_pids = []

            for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
                try:
                    # Check name or full cmdline for the substring
                    name = proc.info['name'] or ""
                    cmdline = " ".join(proc.info['cmdline'] or [])
                    if target_substring in name or target_substring in cmdline:
                        if proc.pid != os.getpid(): # Don't track ourselves
                            matching_pids.append(proc.pid)
                            current_rss += proc.memory_info().rss
                except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                    continue

            if matching_pids:
                if current_rss < min_rss:
                    min_rss = current_rss
                if current_rss > max_rss:
                    max_rss = current_rss

                # ANSI escape: \033[K clears to end of line, \r returns to start
                output = (
                    f"\rPIDs: {len(matching_pids):2d} | "
                    f"CUR: {format_bytes(current_rss)} | "
                    f"MIN: {format_bytes(min_rss)} | "
                    f"MAX: {format_bytes(max_rss)}"
                )
                sys.stdout.write(output)
                sys.stdout.flush()
            else:
                sys.stdout.write(f"\rNo processes matching '{target_substring}' found.          ")
                sys.stdout.flush()

            time.sleep(1)
    except KeyboardInterrupt:
        print("\nMonitoring stopped.")

if __name__ == "__main__":
    main()
