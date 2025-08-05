from args_parser import parse_arguments_monitor, stringify_args
from pathlib import Path
import os
import psutil
import subprocess
import time
import sys
from rich import print as rprint

def monitor_print(*args, **kwargs):
    prefix = "[ResourceMonitor]"
    message = " ".join(str(a) for a in args)
    rprint(f"[red]{prefix} {message}[/red]", **kwargs)

def monitor_process_tree(pid):
    try:
        parent = psutil.Process(pid)
        children = parent.children(recursive=True)
        procs = [parent] + children

        total_mem = sum(p.memory_info().rss for p in procs if p.is_running())
        total_cpu = sum(p.cpu_percent(interval=0.1) for p in procs if p.is_running())
        
        print(procs)
        
        total_cpu /= psutil.cpu_count()
        return total_mem, total_cpu
    except psutil.NoSuchProcess:
        return 0, 0

def launch_and_monitor(cmd):
    monitor_print(cmd)
    monitor_print(os.getcwd())
    process = subprocess.Popen(cmd, cwd=os.getcwd())
    pid = process.pid

    monitor_print(f"Started process {pid}. Monitoring... (Press Ctrl+C to stop)")
    try:
        while process.poll() is None:
            mem, cpu = monitor_process_tree(pid)
            monitor_print(f"Memory: {mem / (1024*1024):.2f} MB | CPU: {cpu:.2f} %")
            time.sleep(0.5) 
    except KeyboardInterrupt:
        monitor_print("Stopping monitor...")
        process.terminate()
    finally:
        process.wait()
        monitor_print("Process finished.")

if __name__ == "__main__":

    args = parse_arguments_monitor()
    dataset_name = Path(args.g).stem
    
    # Build the output path for this experiment
    args.o = os.path.join(args.o, f"{dataset_name}_l{args.l}_w{args.w}_g{args.G}_p{args.p}")
    command = [sys.executable, "main.py"] 
    command.extend(stringify_args(args))
    launch_and_monitor(command)