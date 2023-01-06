"""
Common system tools
"""
import sys
from time import time, sleep
from os.path import join, abspath
import importlib
from multiprocessing import Queue, Process
import psutil

import matplotlib.pyplot as plt

from o2tuner.io import dump_yaml
from o2tuner.log import Log

LOG = Log()

LOADED_MODULES = {}


def run_command(cmd, *, cwd="./", log_file=None, wait=True):
    """
    Prepare command and run
    """
    if log_file is None:
        log_file = "log.log"
    cmd = f"{cmd} >{log_file} 2>&1"
    LOG.info(f"Running command {cmd}")
    proc = psutil.Popen(["/bin/bash", "-c", cmd], cwd=cwd)
    if wait:
        proc.wait()
        if ret := proc.returncode:
            # check the return code and exit if != 0
            LOG.error(f"There seems to have been a problem with the process launched with {cmd}. Its exit code was {ret}.")
            sys.exit(ret)
    return proc, join(cwd, log_file)


def load_file_as_module(path, module_name):
    """
    load path as module
    """
    lookup_key = abspath(path)
    if path in LOADED_MODULES:
        return LOADED_MODULES[lookup_key].__name__

    if module_name in sys.modules:
        LOG.error(f"Module name {module_name} already present, cannot load")
        sys.exit(1)
    spec = importlib.util.spec_from_file_location(module_name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    LOADED_MODULES[lookup_key] = module
    return module.__name__


def import_function_from_module(module_name, function_name):
    module = importlib.import_module(module_name)
    if not hasattr(module, function_name):
        LOG.error(f"Cannot find function {function_name} in module {module.__name__}")
        sys.exit(1)
    return getattr(module, function_name)


def import_function_from_file(path, function_name):
    path = abspath(path)
    module_name = load_file_as_module(path, path.replace("/", "_").replace(".", "_"))
    return import_function_from_module(module_name, function_name)


class Monitor:
    """
    Monitor resources of processes
    """
    RESOURCE_KEYS = ("time", "cpu_percent", "pss", "uss", "rss", "vms", "swap")
    MEMORY_KEYS_OFFSET = 2
    RESOURCE_LABELS = ("Time since start [s]", "CPU [%]", "PSS [MB]", "USS [MB]", "RSS [MB]", "VMS [MB]", "SWAP [MB]")

    def __init__(self):
        # dictionary of resources key(PID): resources
        self.resources = {}
        # monitored pids
        self.pids = []
        # cache processes
        self.procs = []
        # time to sleep between metrics extraction steps
        self.delta_count = None
        # the actual process that does the monitoring
        self.monitor_process = None
        # 2 multiprocessing.Queues to share some information between an instance of this class and the actual monitoring process
        self.resource_queue = None
        self.stop_flag = None

    def add_pid(self, pid):
        self.pids.append(pid)

    def start(self, delta_count=5):
        self.delta_count = delta_count

        # if we find that some processes don't exist anymore already now
        pids_found = []
        for proc in psutil.process_iter():
            if proc.pid in self.pids:
                pids_found.append(proc.pid)
                self.procs.append(proc)
        self.pids = pids_found

        for pid in self.pids:
            self.resources[pid] = [[] for _ in self.RESOURCE_KEYS]

        # prepare the 2 multiprocessing.Queues
        self.resource_queue = Queue()
        self.resource_queue.put(self.resources)
        self.stop_flag = Queue()
        self.stop_flag.put(False)
        self.monitor_process = Process(target=self.count, args=(self.resource_queue, self.pids, self.procs, time(), self.stop_flag, self.delta_count))
        self.monitor_process.start()

    def count(self, queue, pids, procs, time_start, stop, delta_count=5):
        """
        Add resource metrics for a particular process
        This is executed as a sub-process
        """
        while True:
            if stop.get():
                break
            stop.put(False)
            resources = queue.get()
            # count the number of processes that do not need monitoring anymore
            procs_gone = 0
            for pid, proc in zip(pids, procs):
                if not proc.is_running() or proc.status() == psutil.STATUS_ZOMBIE:
                    procs_gone += 1
                    continue

                # obtain children
                procs_with_children = proc.children(recursive=True)
                procs_with_children.append(proc)

                values = [time() - time_start] + [0] * (len(self.RESOURCE_KEYS) - 1)

                for proc in procs_with_children:
                    # sum over all child processes
                    if not proc.is_running() or proc.status() == psutil.STATUS_ZOMBIE:
                        continue

                    mem, cpu_percent = (None, None)

                    try:
                        with proc.oneshot():
                            mem = proc.memory_full_info()
                            cpu_percent = proc.cpu_percent()
                    except (psutil.NoSuchProcess, psutil.AccessDenied):
                        continue

                    values[1] += cpu_percent
                    for i, mem_key in enumerate(self.RESOURCE_KEYS[self.MEMORY_KEYS_OFFSET:], self.MEMORY_KEYS_OFFSET):
                        value = getattr(mem, mem_key, -1)
                        if value >= 0:
                            # convert to MB
                            value = value / 1024 / 1024
                        values[i] += value
                for this_list, value in zip(resources[pid], values):
                    this_list.append(value)
            queue.put(resources)
            if procs_gone == len(pids):
                # all procs are gone (killed, finished etc), no need to monitor
                break
            sleep(delta_count)

    def stop(self):
        self.stop_flag.put(True)
        self.monitor_process.join()
        self.monitor_process.close()
        self.resources = self.resource_queue.get()

    def plot(self, out_dir):
        """
        Plot to output directory out_dir
        """
        axes = []
        for label in self.RESOURCE_LABELS[1:]:
            # collect and prepare axes
            figure, ax = plt.subplots(figsize=(30, 10))
            axes.append(ax)
            ax.set_xlabel(self.RESOURCE_LABELS[0], fontsize=20)
            ax.set_ylabel(label, fontsize=20)
            figure.suptitle(f"{label} per process")

        for metrics in self.resources.values():
            # plot all metrics for each registered process
            times = metrics[0]
            for i, _ in enumerate(self.RESOURCE_LABELS[1:], 1):
                axes[i - 1].plot(times, metrics[i])

        for key, ax in zip(self.RESOURCE_KEYS[1:], axes):
            # conclude, save and close figures
            figure = ax.get_figure()
            figure.tight_layout()
            figure.savefig(join(out_dir, f"{key}.png"))
            plt.close(figure)

    def save(self, out_dir):
        """
        Write YAML and some plots to path
        """
        to_dump = list(self.resources.values())
        dump_yaml(to_dump, join(out_dir, "resources.yaml"))

        self.plot(out_dir)
