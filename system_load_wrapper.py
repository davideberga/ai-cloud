from args_parser import parse_arguments_monitor, stringify_args
from pathlib import Path
import os
import psutil
import subprocess
import time
import sys
from rich import print as rprint
import numpy as np


class ResourceUtilizationMonitor:
    def __init__(self, output_path: str):
        self.output_path = output_path

        self.recorded_timestamp = []
        self.recorded_main_python_mem = []
        self.recorded_main_python_cpu = []
        self.recorded_java_mem = []
        self.recorded_java_cpu = []
        self.recorded_workers_mem = []
        self.recorded_workers_cpu = []

    def record(
        self,
        main_python_mem,
        main_python_cpu,
        java_mem,
        java_cpu,
        workers_mem,
        workers_cpu,
    ):
        timestamp = time.time()

        self.recorded_timestamp.append(timestamp)
        self.recorded_main_python_mem.append(main_python_mem)
        self.recorded_main_python_cpu.append(main_python_cpu)
        self.recorded_java_mem.append(java_mem)
        self.recorded_java_cpu.append(java_cpu)
        self.recorded_workers_mem.append(workers_mem)
        self.recorded_workers_cpu.append(workers_cpu)

    def save(self):
        np.savez(
            os.path.join(self.output_path, "resources.npz"),
            timestamp=np.array(self.recorded_timestamp),
            main_python_mem=np.array(self.recorded_main_python_mem),
            main_python_cpu=np.array(self.recorded_main_python_cpu),
            java_mem=np.array(self.recorded_java_mem),
            java_cpu=np.array(self.recorded_java_cpu),
            workers_mem=np.array(self.recorded_workers_mem),
            workers_cpu=np.array(self.recorded_workers_cpu),
        )


def monitor_print(*args, **kwargs):
    prefix = "[ResourceMonitor]"
    message = " ".join(str(a) for a in args)
    rprint(f"[red]{prefix} {message}[/red]", **kwargs)


def get_process_stats(p: psutil.Process):
    return p.memory_info().rss, p.cpu_percent(interval=0.1) / psutil.cpu_count()


def monitor_process_tree(pid):
    main_python_mem, main_python_cpu = 0, 0
    java_mem, java_cpu = 0, 0
    workers_mem, workers_cpu = 0, 0
    try:
        parent = psutil.Process(pid)
        children = parent.children(recursive=True)

        if parent.is_running():
            main_python_mem, main_python_cpu = get_process_stats(parent)

        for p in children:
            if p.is_running():
                mem, cpu = get_process_stats(p)
                if p.name() == "java":
                    java_mem, java_cpu = mem, cpu
                else:
                    workers_mem += mem
                    workers_cpu += cpu

        return (
            main_python_mem,
            main_python_cpu,
            java_mem,
            java_cpu,
            workers_mem,
            workers_cpu,
        )
    except psutil.NoSuchProcess:
        return 0, 0, 0, 0, 0, 0


def launch_and_monitor(cmd, output_path):
    resources_monitor = ResourceUtilizationMonitor(output_path)
    process = subprocess.Popen(cmd, cwd=os.getcwd())
    pid = process.pid

    monitor_print(f"Monitoring started process {pid}")
    try:
        while process.poll() is None:
            
            (
                main_python_mem,
                main_python_cpu,
                java_mem,
                java_cpu,
                workers_mem,
                workers_cpu,
            ) = monitor_process_tree(pid)
            
            resources_monitor.record(
                main_python_mem,
                main_python_cpu,
                java_mem,
                java_cpu,
                workers_mem,
                workers_cpu,
            )
            # monitor_print(
            #     f"Main mem: {main_python_mem / (1024 * 1024):.2f} MB | Main CPU: {main_python_cpu:.2f} %, Java mem: {java_mem / (1024 * 1024):.2f} MB | Java CPU: {java_cpu:.2f} %, , Workers mem: {workers_mem / (1024 * 1024):.2f} MB | Workers CPU: {workers_cpu:.2f} %"
            # )
            time.sleep(0.3)
    except KeyboardInterrupt:
        monitor_print("Stopping monitor...")
        process.terminate()
    finally:
        process.wait()
        resources_monitor.save()
        monitor_print("Process finished. Data saved.")


if __name__ == "__main__":
    args = parse_arguments_monitor()
    dataset_name = Path(args.g).stem

    # Build the output path for this experiment
    args.o = os.path.join(
        args.o, f"{dataset_name}_l{args.l}_w{args.w}_g{args.G}_p{args.p}_sm{args.sm}"
    )
    print( args.o)
    command = [sys.executable, "main.py"]
    command.extend(stringify_args(args))
    launch_and_monitor(command, args.o)
