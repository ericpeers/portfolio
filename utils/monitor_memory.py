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

    per_process_stats = {}

    try:
        while True:
            current_rss = 0
            processes_info = []

            for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
                try:
                    # Check name or full cmdline for the substring
                    pid = proc.pid
                    name = proc.info['name'] or ""
                    cmdline = " ".join(proc.info['cmdline'] or [])
                    if target_substring in name or target_substring in cmdline:
                        if pid != os.getpid(): # Don't track ourselves
                            rss = proc.memory_info().rss
                            current_rss += rss
                            
                            # Track per-process stats
                            if pid not in per_process_stats:
                                per_process_stats[pid] = {'min': rss, 'max': rss}
                            else:
                                per_process_stats[pid]['min'] = min(per_process_stats[pid]['min'], rss)
                                per_process_stats[pid]['max'] = max(per_process_stats[pid]['max'], rss)

                            # Use cmdline if available for better identification, else name
                            display_name = cmdline if cmdline else name
                            processes_info.append({
                                'pid': pid,
                                'name': display_name[:80],
                                'rss': rss,
                                'min': per_process_stats[pid]['min'],
                                'max': per_process_stats[pid]['max']
                            })
                except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                    continue

            # Clear screen and move cursor to top-left
            sys.stdout.write("\033[2J\033[H")
            print(f"Monitoring processes matching: '{target_substring}'")
            print("Press Ctrl+C to stop.\n")

            if processes_info:
                if current_rss < min_rss:
                    min_rss = current_rss
                if current_rss > max_rss:
                    max_rss = current_rss

                print(f"{'PID':>7} | {'CUR Memory':>12} | {'MIN Memory':>12} | {'MAX Memory':>12} | {'Process (truncated to 80 chars)':<80}")
                print("-" * 135)
                for p in processes_info:
                    print(f"{p['pid']:7d} | {format_bytes(p['rss']):>12} | {format_bytes(p['min']):>12} | {format_bytes(p['max']):>12} | {p['name']}")
                
                print("-" * 135)
                output = (
                    f"TOTAL PIDs: {len(processes_info):2d} | "
                    f"TOTAL CUR: {format_bytes(current_rss)} | "
                    f"TOTAL MIN: {format_bytes(min_rss)} | "
                    f"TOTAL MAX: {format_bytes(max_rss)}"
                )
                print(output)
            else:
                print(f"No processes matching '{target_substring}' found.")

            sys.stdout.flush()
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nMonitoring stopped.")

if __name__ == "__main__":
    main()
