"""
Meta TaskWave Benchmark Utilities Module

This module calculates various performance metrics from benchmark logs.
Primary metrics used in the dissertation:
- Cold start metrics (cold_start_rate, penalties, thermal efficiency)
- Phase-based analysis (warm-up vs measurement)
- Worker state distribution

Secondary metrics (collected but not analysed):
- Throughput and latency
- Load balancing fairness
- Network utilisation
"""
import os
import time
import re
import subprocess
import signal
import logging
import json
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Any, Optional
import platform

from config import WARM_UP_PERIOD, MEASUREMENT_DURATION

import pandas as pd

# ------------------------------- LOGGING SETUP ----------------------------------- #

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

logger = logging.getLogger("benchmark_utils")


# ------------------------------- PROCESS MANAGEMENT FUNCTIONS -------------------------------- #

def start_process(command, env_vars=None):
    """
     Start a subprocess with the given command and environment variables.
    
    Args:
        command (list): Command and arguments as a list
        env_vars (dict): Environment variables to set
        
    Returns:
        subprocess.Popen: Process handle
    """
    
    try:
        # Prepare environment - merge with current environment
        env = os.environ.copy()
        if env_vars:
            env.update(env_vars)
            
        # Start process with env vars
        process = subprocess.Popen(
            command,
            env=env,  # Use merged environment
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True,
            preexec_fn=os.setsid if platform.system() != "Windows" else None
        )
        
        logger.info(f"Started process: {' '.join(command)}, PID: {process.pid}")
        return process
    
    except Exception as e:
        logger.error(f"Error starting process: {e}")
        return None
    
def stop_process(process):
    """
    Stops a subprocess gracefully.

    Args:
        process (subprocess.Popen): Process to stop
    """
    
    if process is None:
        return
    
    try:
        if platform.system() == "Windows":
            process.terminate()
        else:
            # Send SIGTERM to process group (Unix/Linux/macOS)
            os.killpg(os.getpgid(process.pid), signal.SIGTERM)
        
        # Wait for process to terminate with timeout
        process.wait(timeout=5)
        logger.info(f"Stopped process with PID: {process.pid}")
        
    except subprocess.TimeoutExpired:
        logger.warning(f"Process {process.pid} did not terminate gracefully, forcing...")
        if platform.system() == "Windows":
            process.kill()
        else:
            # Kill the entire process group
            os.killpg(os.getpgid(process.pid), signal.SIGKILL)
        logger.info(f"Forcibly terminated process with PID: {process.pid}")
        
    except Exception as e:
        logger.error(f"Error stopping process {process.pid}: {e}")


def find_latest_log_dir():
    """
    Find the most recent log directory.
    
    Returns:
        Path: Path to the most recent log directory
    """
    
    logs_dir = Path("logs")
    if not logs_dir.exists():
        return None
    
    run_folders = [f for f in logs_dir.iterdir() if f.is_dir() and f.name.startswith("run_")]
    if not run_folders:
        return None
    
    # Sort by creation time and return the latest
    return max(run_folders, key=lambda x: x.stat().st_mtime)


# ------------------------------- LOG READING FUNCTIONS -------------------------------- #

def get_completed_job_count():
    """
    Get the count of completed jobs from the latest scheduler log.
    
    Returns:
        int: Number of completed jobs
    """
    latest_log_dir = find_latest_log_dir()
    if not latest_log_dir:
        return 0
    
    scheduler_log = latest_log_dir / "scheduler.log"
    if not scheduler_log.exists():
        return 0
    
    try:
        # Use simple grep-like functionality to count completed jobs
        completed_count = 0
        with open(scheduler_log, "r") as f:
            for line in f:
                if "completed successfully" in line:
                    completed_count += 1
        
        return completed_count
    
    except Exception as e:
        logger.error(f"Error counting completed jobs: {e}")
        return 0
    
def parse_scheduler_logs(log_path):
    """
    Enhanced scheduler log parsing that captures completed job data with cold start information.
    """
    if not log_path.exists():
        logger.error(f"Scheduler log not found: {log_path}")
        return {}

    data = {
        "jobs": {},
        "completed_jobs": {},  # Add this to capture detailed completion data
        "job_queue_lengths": [],
        "worker_statuses": {},
        "scheduling_decisions": [],
        "phase_stats": {
            "warm_up": {
                "jobs_completed": 0,
                "jobs_failed": set(),
                "sample_job_ids": set(),
            },
            "measurement": {
                "jobs_completed": 0,
                "jobs_failed": set(),
                "sample_job_ids": set(),
            }
        }
    }

    current_queue_length = 0
    current_phase = "warm_up"
    last_warmup_job_id = None
    measurement_phase_started = False

    try:
        with open(log_path, "r") as f:
            for line in f:
                timestamp_match = re.search(r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})", line)
                if not timestamp_match:
                    continue
                timestamp = timestamp_match.group(1)
                timestamp = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S").timestamp()

                # Phase transitions
                if "★★★ Scheduler transitioning to measurement phase ★★★" in line:
                    current_phase = "measurement"
                    measurement_phase_started = True
                last_warmup_id_match = re.search(r"Last warm-up job ID: (\d+)", line)
                if last_warmup_id_match:
                    last_warmup_job_id = int(last_warmup_id_match.group(1))

                # Track job added to queue
                if "added to queue" in line:
                    job_id_match = re.search(r"Job (\d+) added", line)
                    if job_id_match:
                        job_id = int(job_id_match.group(1))
                        data["jobs"][job_id] = {
                            "job_id": job_id,
                            "received_time": timestamp,
                            "status": "pending",
                            "phase": current_phase
                        }
                        current_queue_length += 1
                        data["job_queue_lengths"].append({
                            "timestamp": timestamp,
                            "queue_length": current_queue_length
                        })

                # Track job assigned to worker
                elif "Assigned job" in line:
                    job_id_match = re.search(r"Assigned job (\d+) to worker", line)
                    worker_id_match = re.search(r"to worker ([A-Z0-9_]+)", line)
                    if job_id_match and worker_id_match:
                        job_id = int(job_id_match.group(1))
                        worker_id = worker_id_match.group(1)
                        if job_id in data["jobs"]:
                            data["jobs"][job_id].update({
                                "assigned_time": timestamp,
                                "assigned_worker": worker_id,
                                "status": "assigned"
                            })
                        current_queue_length = max(0, current_queue_length - 1)

                # Enhanced job completion tracking with cold start data
                elif "completed successfully" in line and "Job" in line:
                    job_id_match = re.search(r"Job (\d+) completed", line)
                    if job_id_match:
                        job_id = int(job_id_match.group(1))
                        
                        # Extract detailed completion information from enhanced scheduler logs
                        operation_match = re.search(r"Operation: (\w+) on (\[.*?\])", line)
                        result_match = re.search(r"Result: ([^.]+)", line)
                        processing_time_match = re.search(r"Processing time: ([\d.]+)s", line)
                        cold_start_penalty_match = re.search(r"Cold start penalty: ([\d.]+)ms", line)
                        warmth_state_match = re.search(r"Warmth state: (\w+)", line)
                        is_cold_start_match = re.search(r"Is cold start: (True|False)", line)
                        warmth_level_match = re.search(r"Warmth level: ([\d.]+|None)", line)
                        phase_match = re.search(r"phase=(\w+)", line)
                        
                        phase = None
                        if phase_match:
                            phase = phase_match.group(1)
                        elif "phase=measurement" in line:
                            phase = "measurement"
                        elif "phase=warm_up" in line:
                            phase = "warm_up"
                        elif last_warmup_job_id is not None:
                            if job_id > last_warmup_job_id:
                                phase = "measurement"
                            else:
                                phase = "warm_up"
                        else:
                            phase = current_phase
                            
                        if job_id in data["jobs"]:
                            data["jobs"][job_id].update({
                                "completed_time": timestamp,
                                "status": "completed",
                                "phase": phase
                            })
                        
                        # Store detailed completion data
                        completion_data = {
                            "job_id": job_id,
                            "completed_time": timestamp,
                            "phase": phase
                        }
                        
                        try:
                            if operation_match:
                                completion_data["operation"] = operation_match.group(1)
                                try:
                                    # Safely parse the numbers array
                                    numbers_str = operation_match.group(2)
                                    completion_data["numbers"] = eval(numbers_str)  # Safe since it's from our own logs
                                except:
                                    completion_data["numbers"] = operation_match.group(2)  # Use raw string if eval fails
                        except Exception as e:
                            logger.warning(f"Error parsing operation data for job {job_id}: {e}")
                            completion_data["operation"] = "unknown"
                            completion_data["numbers"] = []
                        if result_match:
                            completion_data["result"] = result_match.group(1).strip()
                        if processing_time_match:
                            completion_data["processing_time"] = float(processing_time_match.group(1))
                        if cold_start_penalty_match:
                            completion_data["cold_start_penalty"] = float(cold_start_penalty_match.group(1))
                        if warmth_state_match:
                            completion_data["warmth_state"] = warmth_state_match.group(1)
                        if is_cold_start_match:
                            completion_data["is_cold_start"] = is_cold_start_match.group(1) == "True"
                        if warmth_level_match and warmth_level_match.group(1) != "None":
                            try:
                                completion_data["warmth_level"] = float(warmth_level_match.group(1))
                            except:
                                completion_data["warmth_level"] = None
                        
                        data["completed_jobs"][job_id] = completion_data
                        
                        # Track phase stats
                        if phase in data["phase_stats"]:
                            data["phase_stats"][phase]["jobs_completed"] += 1
                            data["phase_stats"][phase]["sample_job_ids"].add(job_id)

                # Track job failure
                elif "failed with error" in line:
                    job_id_match = re.search(r"Job (\d+) failed", line)
                    if job_id_match:
                        job_id = int(job_id_match.group(1))
                        phase = None
                        if "phase=measurement" in line:
                            phase = "measurement"
                        elif "phase=warm_up" in line:
                            phase = "warm_up"
                        elif last_warmup_job_id is not None:
                            if job_id > last_warmup_job_id:
                                phase = "measurement"
                            else:
                                phase = "warm_up"
                        else:
                            phase = current_phase
                        if job_id in data["jobs"]:
                            data["jobs"][job_id].update({
                                "failed_time": timestamp,
                                "status": "failed",
                                "phase": phase
                            })
                        # Track phase stats
                        if phase in data["phase_stats"]:
                            data["phase_stats"][phase]["jobs_failed"].add(job_id)

                # Track worker status changes
                elif "Worker" in line and ("status:" in line or "registered with" in line):
                    worker_id_match = re.search(r"Worker ([A-Z0-9_]+)", line)
                    status_match = re.search(r"status: (\w+)", line)
                    if worker_id_match:
                        worker_id = worker_id_match.group(1)
                        if worker_id not in data["worker_statuses"]:
                            data["worker_statuses"][worker_id] = []
                        if "registered with" in line:
                            status = "active"
                        elif status_match:
                            status = status_match.group(1)
                        else:
                            continue
                        data["worker_statuses"][worker_id].append({
                            "timestamp": timestamp,
                            "status": status
                        })

                # Track scheduling algorithm decision
                elif "SCHEDULING_DECISION" in line:
                    decision_match = re.search(r"SCHEDULING_DECISION: (.+)", line)
                    if decision_match:
                        try:
                            decision = json.loads(decision_match.group(1))
                            data["scheduling_decisions"].append(decision)
                        except json.JSONDecodeError as e:
                            logger.warning(f"Failed to parse scheduling decision: {e}")
                            continue

        return data
    except Exception as e:
        logger.error(f"Error parsing scheduler log {log_path}: {e}")
        return {}

    
    
def parse_worker_logs(log_paths):
    """
    Parse worker logs to extract processing data, phase-aware.

    Args:
        log_paths (List of Path): Paths to worker log files

    Returns:
        dict: Structured data extracted from logs including:
        - workers: Dict of Worker lifecycle events
        - job_processing: Dict of Job Processing events
        - phase_stats: Per-phase statistics (warm_up, measurement)
    """
    data = {
        "workers": {},
        "job_processing": {},
        "phase_stats": {
            "warm_up": {
                "jobs_completed": 0,
                "jobs_failed": set(),
                "processing_times": [],
                "cold_starts": 0,
                "sample_job_ids": set(),
            },
            "measurement": {
                "jobs_completed": 0,
                "jobs_failed": set(),
                "processing_times": [],
                "cold_starts": 0,
                "sample_job_ids": set(),
            }
        }
    }

    for log_path in log_paths:
        if not log_path.exists():
            logger.warning(f"Worker log not found: {log_path}")
            continue

        worker_id_match = re.search(r"worker_(.+?)\.log", log_path.name)
        if not worker_id_match:
            logger.warning(f"Could not extract worker ID from log filename: {log_path.name}")
            continue

        worker_id = worker_id_match.group(1)
        data["workers"][worker_id] = {
            "worker_id": worker_id,
            "jobs_completed": 0,
            "jobs_failed": 0,
            "processing_times": [],
            "cold_start_events": []
        }

        current_phase = "warm_up"
        last_warmup_job_id = None
        measurement_phase_started = False

        try:
            with open(log_path, "r") as f:
                for line in f:
                    timestamp_match = re.search(r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})", line)
                    if not timestamp_match:
                        continue
                    timestamp = timestamp_match.group(1)
                    timestamp = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S").timestamp()

                    # Phase transitions
                    if "★★★ Scheduler transitioning to measurement phase ★★★" in line:
                        current_phase = "measurement"
                        measurement_phase_started = True
                    last_warmup_id_match = re.search(r"Last warm-up job ID: (\d+)", line)
                    if last_warmup_id_match:
                        last_warmup_job_id = int(last_warmup_id_match.group(1))

                    # Track cold start events
                    if "Cold start penalty" in line:
                        penalty_match = re.search(r"Cold start penalty: (\d+\.\d+)ms", line)
                        warmth_match = re.search(r"Warmth level: (\d+\.\d+)", line)
                        job_id_match = re.search(r"Job (\d+)", line)
                        warmth_state_match = re.search(r"Warmth state: (\w+)", line)
                        cold_start_bool_match = re.search(r"cold_start=(True|False)", line)
                        if penalty_match and warmth_match:
                            penalty = float(penalty_match.group(1))
                            warmth = float(warmth_match.group(1))
                            job_id = int(job_id_match.group(1)) if job_id_match else None
                            warmth_state = warmth_state_match.group(1) if warmth_state_match else None
                            is_cold_start = None
                            if cold_start_bool_match:
                                is_cold_start = cold_start_bool_match.group(1).lower() == "true"
                            else:
                                is_cold_start = warmth_state == "COLD"
                            data["workers"][worker_id]["cold_start_events"].append({
                                "timestamp": timestamp,
                                "penalty_ms": penalty,
                                "warmth_level": warmth,
                                "warmth_state": warmth_state,
                                "is_cold_start": is_cold_start,
                                "job_id": job_id
                            })
                            # Attribute cold start to phase
                            phase = None
                            if "phase=measurement" in line:
                                phase = "measurement"
                            elif "phase=warm_up" in line:
                                phase = "warm_up"
                            elif last_warmup_job_id is not None and job_id is not None:
                                if job_id > last_warmup_job_id:
                                    phase = "measurement"
                                else:
                                    phase = "warm_up"
                            else:
                                phase = current_phase
                            if phase in data["phase_stats"]:
                                data["phase_stats"][phase]["cold_starts"] += 1

                    # Track start of job processing
                    if "Processing job" in line:
                        job_id_match = re.search(r"Processing job (\d+)", line)
                        if job_id_match:
                            job_id = int(job_id_match.group(1))
                            data["job_processing"][job_id] = {
                                "job_id": job_id,
                                "worker_id": worker_id,
                                "processing_start": timestamp,
                                "status": "processing",
                                "cold_start_penalty": 0.0
                            }
                            load_match = re.search(r"with load (\d+)", line)
                            if load_match:
                                load = int(load_match.group(1))
                                data["job_processing"][job_id]["load"] = load
                            # Try to assign phase to job_processing
                            phase = None
                            if "phase=measurement" in line:
                                phase = "measurement"
                            elif "phase=warm_up" in line:
                                phase = "warm_up"
                            elif last_warmup_job_id is not None:
                                if job_id > last_warmup_job_id:
                                    phase = "measurement"
                                else:
                                    phase = "warm_up"
                            else:
                                phase = current_phase
                            data["job_processing"][job_id]["phase"] = phase

                    # Track job completion
                    elif "completed successfully" in line:
                        job_id_match = re.search(r"Job (\d+) completed", line)
                        processing_time_match = re.search(r"processed in ([\d.]+)s", line)
                        cold_start_match = re.search(r"with cold start penalty of ([\d.]+)ms", line)
                        if job_id_match:
                            job_id = int(job_id_match.group(1))
                            data["workers"][worker_id]["jobs_completed"] += 1
                            phase = None
                            if "phase=measurement" in line:
                                phase = "measurement"
                            elif "phase=warm_up" in line:
                                phase = "warm_up"
                            elif last_warmup_job_id is not None:
                                if job_id > last_warmup_job_id:
                                    phase = "measurement"
                                else:
                                    phase = "warm_up"
                            else:
                                # Try to get phase from job_processing if available
                                phase = data["job_processing"].get(job_id, {}).get("phase", current_phase)
                            if processing_time_match:
                                processing_time = float(processing_time_match.group(1))
                                data["workers"][worker_id]["processing_times"].append(processing_time)
                                if job_id in data["job_processing"]:
                                    data["job_processing"][job_id].update({
                                        "processing_time": processing_time,
                                        "completed_time": timestamp,
                                        "status": "completed",
                                        "phase": phase
                                    })
                                # Phase stats
                                if phase in data["phase_stats"]:
                                    data["phase_stats"][phase]["jobs_completed"] += 1
                                    data["phase_stats"][phase]["processing_times"].append(processing_time)
                                    data["phase_stats"][phase]["sample_job_ids"].add(job_id)
                            # Extract cold start penalty if present
                            if cold_start_match and job_id in data["job_processing"]:
                                cold_start_penalty = float(cold_start_match.group(1))
                                data["job_processing"][job_id]["cold_start_penalty"] = cold_start_penalty

                    # Track job failures
                    elif "Failed with error processing job" in line:
                        job_id_match = re.search(r"Failed with error processing job (\d+)", line)
                        if job_id_match:
                            job_id = int(job_id_match.group(1))
                            data["workers"][worker_id]["jobs_failed"] += 1
                            phase = None
                            if "phase=measurement" in line:
                                phase = "measurement"
                            elif "phase=warm_up" in line:
                                phase = "warm_up"
                            elif last_warmup_job_id is not None:
                                if job_id > last_warmup_job_id:
                                    phase = "measurement"
                                else:
                                    phase = "warm_up"
                            else:
                                phase = data["job_processing"].get(job_id, {}).get("phase", current_phase)
                            if job_id in data["job_processing"]:
                                data["job_processing"][job_id].update({
                                    "failed_time": timestamp,
                                    "status": "failed",
                                    "phase": phase
                                })
                            # Phase stats
                            if phase in data["phase_stats"]:
                                data["phase_stats"][phase]["jobs_failed"].add(job_id)
        except Exception as e:
            logger.error(f"Error parsing worker log {log_path}: {e}")
            continue

    return data




# ------------------------------- METRICS CALCULATION FUNCTIONS -------------------------------- #
# IMPORTANT NOTE: The functions below are kept for completeness but not used in the final dissertation analysis.

def calculate_throughput_metrics(scheduler_data: dict, worker_data: dict, phase: Optional[str] = None):
    """
    Calculate throughput metrics, optionally for a specific phase.
    For phase-aware and detailed metrics, use calculate_phase_metrics().
    This function provides backward-compatible overall metrics and phase-aware output.

    Args:
        scheduler_data (dict): Parsed scheduler log data from `parse_scheduler_logs`
        worker_data (dict): Parsed worker log data from `parse_worker_logs`
        phase (str, optional): Restrict calculation to jobs in this phase (if present in job data)

    Returns:
        dict: Throughput metrics including:
            - jobs_completed
            - throughput_jobs_per_second
            - avg_worker_utilisation_percent
            
    Note: These metrics are collected for completeness but were not used in the final dissertation analysis, which focused on cold start and thermal efficiency metrics.
    """
    jobs = scheduler_data.get("jobs", {})
    workers = worker_data.get("workers", {})

    # If phase specified, filter jobs by phase
    if phase is not None:
        jobs = {jid: j for jid, j in jobs.items() if j.get("phase") == phase}

    total_completed_jobs = sum(1 for job in jobs.values() if job.get("status") == "completed")
    if total_completed_jobs == 0:
        return {
            "jobs_completed": 0,
            "throughput_jobs_per_second": 0.0,
            "avg_worker_utilisation_percent": 0.0
        }
    # Calculate the time span for the jobs in this phase
    all_times = []
    for job in jobs.values():
        if "received_time" in job:
            all_times.append(job["received_time"])
        if "completed_time" in job:
            all_times.append(job["completed_time"])
    if len(all_times) < 2:
        duration = 1.0
    else:
        duration = max(all_times) - min(all_times)
    throughput = total_completed_jobs / max(duration, 1.0)
    # System utilisation (for all workers, but processing times filtered by jobs in this phase)
    total_workers = len(workers)
    if total_workers == 0:
        avg_utilisation = 0.0
    else:
        # Only sum processing times for jobs in this phase
        phase_job_ids = set(jobs.keys())
        total_processing_time = 0.0
        for worker in workers.values():
            # If phase is None, sum all; else, only times for jobs in phase
            if phase is None:
                total_processing_time += sum(worker.get("processing_times", []))
            else:
                # Sum processing times for jobs this worker ran and that are in phase
                for job_id in worker_data.get("job_processing", {}):
                    jp = worker_data["job_processing"][job_id]
                    if jp.get("worker_id") == worker["worker_id"] and job_id in phase_job_ids and "processing_time" in jp:
                        total_processing_time += jp["processing_time"]
        total_available_time = duration * total_workers
        avg_utilisation = (total_processing_time / max(total_available_time, 1.0)) * 100
    return {
        "jobs_completed": total_completed_jobs,
        "throughput_jobs_per_second": round(throughput, 3),
        "avg_worker_utilisation_percent": round(min(avg_utilisation, 100.0), 2)
    }


def calculate_latency_metrics(scheduler_data: dict, worker_data: dict, phase: Optional[str] = None):
    """
    Calculate latency metrics (queue and processing time), optionally for a specific phase.
    For phase-aware and detailed metrics, use calculate_phase_metrics().
    This function provides backward-compatible overall metrics and phase-aware output.

    Args:
        scheduler_data: Parsed scheduler log data
        worker_data: Parsed worker log data
        phase (str, optional): Restrict calculation to jobs in this phase (if present in job data)

    Returns:
        dict: Latency metrics in milliseconds including:
            - avg_queue_time_ms
            - avg_processing_time_ms
            
    Note: These metrics are collected for completeness but were not used in the final dissertation analysis, which focused on cold start and thermal efficiency metrics.
    """
    jobs = scheduler_data.get("jobs", {})
    job_processing = worker_data.get("job_processing", {})
    # If phase specified, filter jobs by phase
    if phase is not None:
        jobs = {jid: j for jid, j in jobs.items() if j.get("phase") == phase}
    queue_times = []
    processing_times = []
    for job_id, job_data in jobs.items():
        if job_data.get("status") == "completed":
            # Queue time: from received to assigned
            if "received_time" in job_data and "assigned_time" in job_data:
                queue_time = job_data["assigned_time"] - job_data["received_time"]
                queue_times.append(queue_time * 1000)  # ms
            # Processing time: from worker processing data
            if job_id in job_processing and "processing_time" in job_processing[job_id]:
                proc_time = job_processing[job_id]["processing_time"]
                processing_times.append(proc_time * 1000)
    avg_queue_time = sum(queue_times) / max(len(queue_times), 1)
    avg_processing_time = sum(processing_times) / max(len(processing_times), 1)
    return {
        "avg_queue_time_ms": round(avg_queue_time, 2),
        "avg_processing_time_ms": round(avg_processing_time, 2)
    }


def calculate_reliability_metrics(scheduler_data: dict, worker_data: dict, phase: Optional[str] = None):
    """
    Calculate reliability metrics including success rate and job reallocation speed, optionally for a specific phase.
    For phase-aware and detailed metrics, use calculate_phase_metrics().
    This function provides backward-compatible overall metrics and phase-aware output.

    Args:
        scheduler_data: Parsed scheduler log data
        worker_data: Parsed worker log data
        phase (str, optional): Restrict calculation to jobs in this phase (if present in job data)

    Returns:
        dict: Reliability metrics with clear semantics
    """
    jobs = scheduler_data.get("jobs", {})
    if phase is not None:
        jobs = {jid: j for jid, j in jobs.items() if j.get("phase") == phase}
    total_jobs = len(jobs)
    job_counts = {
        "completed": sum(1 for job in jobs.values() if job.get("status") == "completed"),
        "failed": sum(1 for job in jobs.values() if job.get("status") == "failed"),
        "pending": sum(1 for job in jobs.values() if job.get("status") == "pending"),
        "assigned": sum(1 for job in jobs.values() if job.get("status") == "assigned"),
        "processing": sum(1 for job in jobs.values() if job.get("status") == "processing"),
        "other": sum(1 for job in jobs.values() if job.get("status") not in ["completed", "failed", "pending", "assigned", "processing"])
    }
    processed_jobs = job_counts["completed"] + job_counts["failed"]
    if processed_jobs > 0:
        success_rate = (job_counts["completed"] / processed_jobs) * 100
    else:
        success_rate = 0.0
    reallocated_jobs = 0
    reallocation_times = []
    for job_id, job_data in jobs.items():
        requeue_events = job_data.get("requeue_events", [])
        if requeue_events:
            reallocated_jobs += 1
            first_requeue_time = requeue_events[0]["timestamp"]
            assigned_time = job_data.get("assigned_time", 0)
            if assigned_time > first_requeue_time:
                recovery_time = (assigned_time - first_requeue_time) * 1000
                reallocation_times.append(recovery_time)
    avg_recovery_time = sum(reallocation_times) / max(1, len(reallocation_times))
    metrics = {
        "total_received_jobs": total_jobs,
        "completed_jobs": job_counts["completed"],
        "failed_jobs": job_counts["failed"],
        "pending_jobs": job_counts["pending"],
        "in_progress_jobs": job_counts["assigned"] + job_counts["processing"],
        "job_success_rate": round(success_rate, 2),
        "reallocated_jobs": reallocated_jobs,
        "avg_recovery_time_ms": round(avg_recovery_time, 2)
    }
    logger.debug(f"Job status distribution: {job_counts}")
    logger.debug(f"Processed jobs: {processed_jobs} out of {total_jobs} total received jobs\n\n")
    return metrics


def calculate_load_balancing_metrics(scheduler_data: dict, worker_data: dict, phase: Optional[str] = None):
    """
    Calculate load balancing metrics to assess job distribution fairness, optionally for a specific phase.
    For phase-aware and detailed metrics, use calculate_phase_metrics().
    This function provides backward-compatible overall metrics and phase-aware output.

    Args:
        scheduler_data: Parsed scheduler log data
        worker_data: Parsed worker log data
        phase (str, optional): Restrict calculation to jobs in this phase (if present in job data)

    Returns:
        dict: Load balancing metrics including job distribution and fairness index
    Note: These metrics are collected for completeness but were not used in the final dissertation analysis, which focused on cold start and thermal efficiency metrics.
    """
    jobs = scheduler_data.get("jobs", {})
    # If phase specified, filter jobs by phase
    if phase is not None:
        jobs = {jid: j for jid, j in jobs.items() if j.get("phase") == phase}
    all_workers = set()
    for job in jobs.values():
        assigned_worker = job.get("assigned_worker")
        if assigned_worker:
            all_workers.add(assigned_worker)
    scheduler_workers = scheduler_data.get("worker_statuses", {})
    all_workers.update(scheduler_workers.keys())
    jobs_per_worker = {worker_id: 0 for worker_id in all_workers}
    for job in jobs.values():
        assigned_worker = job.get("assigned_worker")
        if assigned_worker and job.get("status") == "completed":
            if assigned_worker in jobs_per_worker:
                jobs_per_worker[assigned_worker] += 1
    # Debug output
    print(f"DEBUG: All workers found: {sorted(all_workers)}")
    print(f"DEBUG: Jobs per worker: {jobs_per_worker}")
    job_counts = list(jobs_per_worker.values())
    total_workers = len(job_counts)
    total_jobs = sum(job_counts)
    if total_workers == 0 or total_jobs == 0:
        return {
            "jobs_per_worker": jobs_per_worker,
            "fairness_index": 0.0,
            "load_distribution_std": 0.0,
            "active_workers": 0,
            "total_workers": 0,
            "worker_participation_rate": 0.0
        }
    sum_jobs = sum(job_counts)
    sum_jobs_squared = sum(x * x for x in job_counts)
    if sum_jobs_squared == 0:
        fairness_index = 1.0
    else:
        fairness_index = (sum_jobs * sum_jobs) / (total_workers * sum_jobs_squared)
    mean_jobs = sum_jobs / total_workers if total_workers > 0 else 0
    variance = sum((x - mean_jobs) ** 2 for x in job_counts) / total_workers
    load_distribution_std = variance ** 0.5
    active_workers = sum(1 for count in job_counts if count > 0)
    return {
        "jobs_per_worker": jobs_per_worker,
        "fairness_index": round(fairness_index, 3),
        "load_distribution_std": round(load_distribution_std, 2),
        "active_workers": active_workers,
        "total_workers": total_workers,
        "worker_participation_rate": round((active_workers / total_workers) * 100, 1) if total_workers > 0 else 0
    }


def calculate_enhanced_cold_start_metrics(scheduler_data: dict, worker_data: dict, phase: Optional[str] = None) -> Dict[str, Any]:
    """
    Calculate comprehensive cold start metrics using enhanced data sources.
    
    This function replaces the legacy calculate_cold_start_metrics() and uses the enhanced
    data structure from completed_jobs in scheduler_data.
    
    Args:
        scheduler_data: Parsed scheduler log data with completed_jobs
        worker_data: Parsed worker log data 
        phase: Optional phase filter ('warm_up', 'measurement', or None for all)
        
    Returns:
        Dict with comprehensive cold start metrics in milliseconds
    
    Note: These metrics are collected for completeness but were not used in the final dissertation analysis, which focused on cold start and thermal efficiency metrics.
    """
    
    # Primary data source: Enhanced completed_jobs from scheduler
    completed_jobs = scheduler_data.get("completed_jobs", {})
    jobs = scheduler_data.get("jobs", {})
    
    # Filter jobs by phase if specified
    if phase:
        filtered_jobs = {jid: job for jid, job in jobs.items() if job.get("phase") == phase}
        filtered_completed = {jid: comp for jid, comp in completed_jobs.items() 
                            if jid in filtered_jobs}
    else:
        filtered_jobs = jobs
        filtered_completed = completed_jobs
    
    # Initialize counters
    total_completed_jobs = 0
    cold_start_jobs = 0
    warm_start_jobs = 0
    hot_start_jobs = 0
    
    cold_start_penalties = []
    warm_start_penalties = []
    total_execution_time = 0.0
    
    # Analyze each completed job
    for job_id, completion_data in filtered_completed.items():
        # Skip if job not in filtered jobs
        if job_id not in filtered_jobs:
            continue
            
        # Skip if not completed successfully
        job_info = filtered_jobs.get(job_id, {})
        if job_info.get("status") != "completed":
            continue
            
        total_completed_jobs += 1
        
        # Extract cold start data from completion_data
        warmth_state = completion_data.get("warmth_state", "unknown")
        is_cold_start = completion_data.get("is_cold_start", False)
        cold_start_penalty = completion_data.get("cold_start_penalty", 0.0)
        processing_time = completion_data.get("processing_time", 0.0)
        
        # Add to total execution time
        total_execution_time += processing_time
        
        # Categorize by warmth state
        if warmth_state == "COLD" or is_cold_start:
            cold_start_jobs += 1
            if cold_start_penalty > 0:
                cold_start_penalties.append(cold_start_penalty)
        elif warmth_state == "WARM":
            warm_start_jobs += 1
            if cold_start_penalty > 0:  # WARM jobs can have small penalties
                warm_start_penalties.append(cold_start_penalty)
        else:  # HOT or unknown
            hot_start_jobs += 1
    
    # Fallback: Check worker cold start events if no enhanced data found
    if total_completed_jobs == 0 or (cold_start_jobs == 0 and len(cold_start_penalties) == 0):
        logger.warning("No cold start data found in completed_jobs, checking worker events...")
        
        for worker_id, worker_info in worker_data.get("workers", {}).items():
            for event in worker_info.get("cold_start_events", []):
                event_job_id = event.get("job_id")
                if event_job_id and event_job_id in filtered_jobs:
                    if event.get("warmth_state") == "COLD":
                        cold_start_jobs += 1
                        penalty = event.get("penalty_ms", 0.0)
                        if penalty > 0:
                            cold_start_penalties.append(penalty)
    
    # Calculate metrics
    if total_completed_jobs == 0:
        return {
            "cold_start_rate_percent": 0.0,
            "avg_cold_start_penalty_ms": 0.0,
            "total_cold_starts": 0,
            "avg_warm_penalty_ms": 0.0,
            "cold_penalty_percent_of_execution": 0.0,
            "warm_penalty_percent_of_execution": 0.0,
            "total_penalty_percent_of_execution": 0.0,
            "cold_state_percent": 0.0,
            "warm_state_percent": 0.0,
            "hot_state_percent": 0.0,
            "total_completed_jobs": 0,
            "data_source": "no_data"
        }
    
    # Core metrics
    cold_start_rate = (cold_start_jobs / total_completed_jobs) * 100
    avg_cold_penalty = sum(cold_start_penalties) / max(1, len(cold_start_penalties))
    avg_warm_penalty = sum(warm_start_penalties) / max(1, len(warm_start_penalties))
    
    # State distribution percentages
    cold_state_percent = (cold_start_jobs / total_completed_jobs) * 100
    warm_state_percent = (warm_start_jobs / total_completed_jobs) * 100
    hot_state_percent = (hot_start_jobs / total_completed_jobs) * 100
    
    # Execution time impact (convert penalties from ms to seconds for calculation)
    total_cold_penalty_time = sum(cold_start_penalties) / 1000.0  # ms to seconds
    total_warm_penalty_time = sum(warm_start_penalties) / 1000.0  # ms to seconds
    
    cold_penalty_percent = 0.0
    warm_penalty_percent = 0.0
    total_penalty_percent = 0.0
    
    if total_execution_time > 0:
        cold_penalty_percent = (total_cold_penalty_time / total_execution_time) * 100
        warm_penalty_percent = (total_warm_penalty_time / total_execution_time) * 100
        total_penalty_percent = ((total_cold_penalty_time + total_warm_penalty_time) / total_execution_time) * 100
    
    # Determine data source for debugging
    data_source = "completed_jobs" if len(filtered_completed) > 0 else "worker_events"
    
    return {
        # Main cold start metrics
        "cold_start_rate_percent": round(cold_start_rate, 2),
        "avg_cold_start_penalty_ms": round(avg_cold_penalty, 2),
        "total_cold_starts": cold_start_jobs,
        
        # Warm penalty metrics
        "avg_warm_penalty_ms": round(avg_warm_penalty, 2),
        
        # Execution time impact percentages
        "cold_penalty_percent_of_execution": round(cold_penalty_percent, 2),
        "warm_penalty_percent_of_execution": round(warm_penalty_percent, 2),
        "total_penalty_percent_of_execution": round(total_penalty_percent, 2),
        
        # State distribution percentages
        "cold_state_percent": round(cold_state_percent, 2),
        "warm_state_percent": round(warm_state_percent, 2),
        "hot_state_percent": round(hot_state_percent, 2),
        
        # Debugging info
        "total_completed_jobs": total_completed_jobs,
        "data_source": data_source
    }


# ------------------------------- PHASE-AWARE COMPREHENSIVE METRICS -------------------------------- #

def calculate_phase_metrics(scheduler_data: dict, worker_data: dict) -> dict:
    """
    Calculate comprehensive metrics separately for warm-up and measurement phases using
    signal-based detection rather than timing assumptions.
    
    Args:
        scheduler_data: Dict containing parsed scheduler log data
        worker_data: Dict containing parsed worker log data
        
    Returns:
        dict: Metrics for each phase and overall execution
    """
    jobs = scheduler_data.get("jobs", {})
    job_processing = worker_data.get("job_processing", {})
    workers = worker_data.get("workers", {})
    
    # Extract signal-based phase transition markers
    phase_transition_timestamp = None
    last_warmup_job_id = None
    
    # Look for explicit phase transition marker in scheduler data
    phase_events = scheduler_data.get("phase_events", [])
    for event in phase_events:
        if event.get("type") == "PHASE_TRANSITION" and event.get("from") == "warm_up" and event.get("to") == "measurement":
            phase_transition_timestamp = event.get("timestamp")
            last_warmup_job_id = event.get("last_warmup_job_id")
            break
    
    # Fallback: Look for phase transition markers in job data if not found in events
    if phase_transition_timestamp is None:
        for jid, job in jobs.items():
            if job.get("phase_transition_marker"):
                phase_transition_timestamp = job.get("phase_transition_timestamp")
                last_warmup_job_id = job.get("job_id")
                break
    
    # Signal-based classification: use explicit phase markers in job data
    warm_up_jobs = set()
    measurement_jobs = set()
    after_jobs = set()
    
    for jid, job in jobs.items():
        # First priority: Use explicit phase marker if present
        if "phase" in job:
            if job["phase"] == "warm_up":
                warm_up_jobs.add(jid)
            elif job["phase"] == "measurement":
                measurement_jobs.add(jid)
            elif job["phase"] == "after":
                after_jobs.add(jid)
        # Second priority: Use job ID comparison with last_warmup_job_id
        elif last_warmup_job_id is not None:
            if jid <= last_warmup_job_id:
                warm_up_jobs.add(jid)
            else:
                measurement_jobs.add(jid)
        # Last resort: Use timestamp if available
        elif phase_transition_timestamp is not None and "received_time" in job:
            if job["received_time"] < phase_transition_timestamp:
                warm_up_jobs.add(jid)
            else:
                measurement_jobs.add(jid)
        else:
            # If no reliable phase indicator, default to warm_up
            warm_up_jobs.add(jid)
    
    def phase_metrics(job_ids: set, phase_name: str):
        """Calculate metrics for jobs in a specific phase"""
        # Duration: from first to last completed in phase
        times = []
        for jid in job_ids:
            job = jobs.get(jid, {})
            if "completed_time" in job:
                times.append(job["completed_time"])
            elif "failed_time" in job:
                times.append(job["failed_time"])
        
        if times:
            duration = max(times) - min(times)
        else:
            duration = 1.0
            
        # Throughput: completed jobs / duration
        completed = set(jid for jid in job_ids if jobs.get(jid, {}).get("status") == "completed")
        throughput = len(completed) / max(duration, 1.0)
        
        # Latency: average processing + queue time
        queue_times = []
        processing_times = []
        for jid in completed:
            job = jobs[jid]
            # Queue time
            if "received_time" in job and "assigned_time" in job:
                queue_times.append((job["assigned_time"] - job["received_time"]) * 1000)
            # Processing time
            jp = job_processing.get(jid)
            if jp and "processing_time" in jp:
                processing_times.append(jp["processing_time"] * 1000)
                
        avg_queue_time = sum(queue_times) / max(len(queue_times), 1)
        avg_processing_time = sum(processing_times) / max(len(processing_times), 1)
        
        # Cold start rate (newer logic: see calculate_cold_start_metrics)
        cold_state_jobs = set()
        all_cold_start_penalties = []
        for jid in job_ids:
            jp = job_processing.get(jid)
            warmth_state = jp.get("warmth_state") if jp else None
            if jp and jp.get("status") == "completed":
                if warmth_state == "COLD":
                    cold_state_jobs.add(jid)
                    all_cold_start_penalties.append(jp.get("cold_start_penalty", 0))
                elif warmth_state is None and jp.get("is_cold_start"):
                    cold_state_jobs.add(jid)
                    all_cold_start_penalties.append(jp.get("cold_start_penalty", 0))
        # Also check worker cold start events (for jobs not in job_processing)
        for worker_info in workers.values():
            for event in worker_info.get("cold_start_events", []):
                eid = event.get("job_id")
                warmth_state = event.get("warmth_state")
                if eid in job_ids:
                    if warmth_state == "COLD" or (warmth_state is None and event.get("is_cold_start")):
                        cold_state_jobs.add(eid)
                        all_cold_start_penalties.append(event.get("penalty_ms", 0))
        cold_start_rate = (len(cold_state_jobs) / max(1, len(completed))) * 100 if completed else 0
        avg_cold_start_penalty = sum(all_cold_start_penalties) / max(1, len(all_cold_start_penalties))
        
        # Success rate
        failed = set(jid for jid in job_ids if jobs.get(jid, {}).get("status") == "failed")
        processed = len(completed) + len(failed)
        success_rate = (len(completed) / processed) * 100 if processed > 0 else 0
        
        # Fairness index (Jain's): job distribution among workers for this phase
        jobs_per_worker = {}
        for jid in completed:
            worker = jobs[jid].get("assigned_worker")
            if worker:
                jobs_per_worker.setdefault(worker, 0)
                jobs_per_worker[worker] += 1
                
        job_counts = list(jobs_per_worker.values())
        n_workers = len(job_counts)
        if n_workers and sum(x*x for x in job_counts) > 0:
            fairness = (sum(job_counts) ** 2) / (n_workers * sum(x*x for x in job_counts))
        else:
            fairness = 1.0 if n_workers else 0.0
            
        return {
            "phase": phase_name,
            "duration": round(duration, 2),
            "jobs_completed": len(completed),
            "jobs_failed": len(failed),
            "jobs_failed_set": failed,
            "throughput_jobs_per_sec": round(throughput, 3),
            "avg_queue_time_ms": round(avg_queue_time, 2),
            "avg_processing_time_ms": round(avg_processing_time, 2),
            "cold_start_count": len(cold_state_jobs),
            "cold_start_rate_percent": round(cold_start_rate, 2),
            "avg_cold_start_penalty_ms": round(avg_cold_start_penalty, 2),
            "success_rate_percent": round(success_rate, 2),
            "fairness_index": round(fairness, 3),
            "jobs_per_worker": jobs_per_worker,
            "sample_job_ids": set(list(job_ids)[:10])  # Just include a sample of 10 job IDs
        }
    
    warm_up_metrics = phase_metrics(warm_up_jobs, "warm_up")
    measurement_metrics = phase_metrics(measurement_jobs, "measurement")
    
    # Calculate total duration
    all_times = []
    for job in jobs.values():
        for time_field in ["received_time", "assigned_time", "completed_time", "failed_time"]:
            if time_field in job:
                all_times.append(job[time_field])
    
    if all_times:
        total_duration = max(all_times) - min(all_times)
    else:
        total_duration = 0
        
    total_completed = sum(1 for job in jobs.values() if job.get("status") == "completed")
    avg_job_throughput = total_completed / max(total_duration, 1.0)
    
    return {
        "warm_up_phase": warm_up_metrics,
        "measurement_phase": measurement_metrics,
        "total_duration": round(total_duration, 2),
        "avg_job_throughput": round(avg_job_throughput, 3)
    }


def job_result_phase_consistency_check(scheduler_data: dict) -> None:
    """
    Verifies phase consistency across job lifecycle using signal-based detection.
    Raises ValueError if any job has inconsistent phase assignments.
    
    Args:
        scheduler_data: Dict containing parsed scheduler log data
    """
    jobs = scheduler_data.get("jobs", {})
    
    # Extract phase transition markers
    phase_events = scheduler_data.get("phase_events", [])
    phase_transition_timestamp = None
    last_warmup_job_id = None
    
    for event in phase_events:
        if event.get("type") == "PHASE_TRANSITION" and event.get("from") == "warm_up" and event.get("to") == "measurement":
            phase_transition_timestamp = event.get("timestamp")
            last_warmup_job_id = event.get("last_warmup_job_id")
            break
    
    # Find phase inconsistencies
    for jid, job in jobs.items():
        # Skip jobs without both receive and complete times
        if "received_time" not in job or not ("completed_time" in job or "failed_time" in job):
            continue
            
        # Get job's assigned phase
        job_phase = job.get("phase")
        if job_phase is not None:
            # Check if phase marking is consistent throughout job lifecycle
            result_phase = job.get("result_phase")
            if result_phase is not None and job_phase != result_phase:
                raise ValueError(
                    f"Job {jid} has inconsistent phase markers: assigned phase '{job_phase}' "
                    f"but result has phase '{result_phase}'"
                )
    
    # No inconsistencies found
    return


def is_measurement_phase_started(scheduler_data: dict) -> bool:
    """
    Returns True if the measurement phase has started (i.e. at least one job received after warm-up).
    """
    jobs = scheduler_data.get("jobs", {})
    all_received_times = [job.get("received_time") for job in jobs.values() if "received_time" in job]
    if not all_received_times:
        return False
    t0 = min(all_received_times)
    warm_up_end = t0 + WARM_UP_PERIOD
    for job in jobs.values():
        rt = job.get("received_time")
        if rt is not None and rt >= warm_up_end:
            return True
    return False


def get_phase_sample_jobs(scheduler_data: dict, phase: str) -> set:
    """
    Returns a set of job IDs for a given phase ('warm_up', 'measurement', or 'after').
    Raises ValueError for invalid phase.
    """
    if phase not in ("warm_up", "measurement", "after"):
        raise ValueError("phase must be one of 'warm_up', 'measurement', or 'after'")
    jobs = scheduler_data.get("jobs", {})
    all_received_times = [job.get("received_time") for job in jobs.values() if "received_time" in job]
    if not all_received_times:
        return set()
    t0 = min(all_received_times)
    warm_up_end = t0 + WARM_UP_PERIOD
    measurement_end = warm_up_end + MEASUREMENT_DURATION
    sample_job_ids = set()
    for jid, job in jobs.items():
        rt = job.get("received_time")
        if rt is None:
            continue
        if phase == "warm_up" and rt < warm_up_end:
            sample_job_ids.add(jid)
        elif phase == "measurement" and warm_up_end <= rt < measurement_end:
            sample_job_ids.add(jid)
        elif phase == "after" and rt >= measurement_end:
            sample_job_ids.add(jid)
    return sample_job_ids

def get_all_worker_logs(log_dir):
    """
    Get all worker log files from a log directory.
    
    Args:
        log_dir (Path): Directory containing log files
        
    Returns:
        List[Path]: List of worker log file paths
    """
    if not log_dir.exists():
        return []
    
    worker_logs = []
    for log_file in log_dir.glob("worker_*.log"):
        worker_logs.append(log_file)
    
    return worker_logs


def get_measurement_phase_job_count(log_dir: Path) -> int:
    """
    Count jobs completed specifically in the measurement phase using signal-based 
    detection rather than timing assumptions.
    
    Args:
        log_dir: Directory containing logs
        
    Returns:
        int: Number of jobs completed in the measurement phase
    """
    count = 0
    scheduler_log = log_dir / "scheduler.log"
    
    if not scheduler_log.exists():
        return 0
    
    try:
        measurement_phase_started = False
        last_warmup_job_id = None
        
        with open(scheduler_log, "r") as f:
            for line in f:
                # Explicit phase transition signal
                if "★★★ Scheduler transitioning to measurement phase ★★★" in line:
                    measurement_phase_started = True
                
                # Get last warm-up job ID marker
                last_warmup_id_match = re.search(r"Last warm-up job ID: (\d+)", line)
                if last_warmup_id_match:
                    last_warmup_job_id = int(last_warmup_id_match.group(1))
                
                # Count completed jobs in measurement phase
                if "completed successfully" in line:
                    # First priority: Check for explicit phase marker
                    if "phase=measurement" in line:
                        count += 1
                        continue
                    
                    # Second priority: Job ID comparison (if measurement phase has started)
                    if measurement_phase_started and last_warmup_job_id is not None:
                        job_id_match = re.search(r"Job (\d+) completed", line)
                        if job_id_match:
                            job_id = int(job_id_match.group(1))
                            if job_id > last_warmup_job_id:
                                count += 1
                                continue
                    
                    # If in measurement phase and not explicitly marked as warm-up
                    if measurement_phase_started and "phase=warm_up" not in line:
                        count += 1
    
    except Exception as e:
        logger.error(f"Error counting measurement phase jobs: {e}")
    
    return count


def export_job_level_data(scheduler_data: dict, worker_data: dict, run_name: str,
                         job_algorithm: Optional[str] = None, 
                         worker_algorithm: Optional[str] = None,
                         iteration: Optional[int] = None, 
                         benchmark_mode: Optional[str] = None) -> Path:
    """
    Export enhanced job-level data combining scheduler and worker logs into a single CSV.
    Includes comprehensive cold start tracking and warmth state information.
    All time values are in the CSV are standardized to milliseconds except processing_time (seconds).

    Args:
        scheduler_data: Parsed data from `parse_scheduler_logs`
        worker_data: Parsed data from `parse_worker_logs`
        run_name: Benchmark run name (used for folder and filename)

    Returns:
        Path to the exported job-level CSV file
    """
    jobs = scheduler_data.get("jobs", {})
    job_processing = worker_data.get("job_processing", {})
    completed_jobs = scheduler_data.get("completed_jobs", {})  # Get completed jobs from scheduler
    
    # Parse run_name to extract missing info if not provided
    if not job_algorithm or not worker_algorithm:
        # Extract from run_name format: run_2025-06-25_17-26_rr_job_fastest_worker_iter01
        parts = run_name.split('_')
        if len(parts) >= 6:
            job_algorithm = job_algorithm or parts[3]  # 'rr_job'
            worker_algorithm = worker_algorithm or parts[4]  # 'fastest_worker' 
            if not iteration:
                iter_part = parts[5]  # 'iter01'
                iteration = int(iter_part.replace('iter', ''))
    
    # Determine benchmark characteristics from config/environment
    if not benchmark_mode:
        benchmark_mode = os.getenv("TASKWAVE_BENCHMARK_MODE", "default")
    
    # Map benchmark mode to inter-arrival characteristics
    inter_arrival_mapping = {
        'light': {'category': 'extended', 'range': '8-15s'},
        'moderate': {'category': 'typical', 'range': '2-5s'}, 
        'heavy': {'category': 'high_frequency', 'range': '0.5-1.5s'},
        'test': {'category': 'test', 'range': '1-3s'},
        'default': {'category': 'default', 'range': '0.5-1.5s'}
    }
    
    arrival_info = inter_arrival_mapping.get(benchmark_mode, inter_arrival_mapping['default'])
    
    log_dir = Path("../logs") / run_name
    log_dir.mkdir(parents=True, exist_ok=True)

    rows = []

    for job_id, job_info in jobs.items():
        worker_info = job_processing.get(job_id, {})
        completed_info = completed_jobs.get(job_id, {})  # Get additional data from scheduler's completed_jobs
        
        # Multi-source cold start data collection with priority order
        cold_start_penalty = 0.0
        warmth_state = None
        is_cold_start = None
        warmth_level = None
        operation = None
        numbers = None
        result = None
        
        # Priority 1: Scheduler completed jobs data (most comprehensive)
        if completed_info:
            cold_start_penalty = completed_info.get("cold_start_penalty", 0.0)
            warmth_state = completed_info.get("warmth_state")
            is_cold_start = completed_info.get("is_cold_start")
            warmth_level = completed_info.get("warmth_level")
            operation = completed_info.get("operation")
            numbers = completed_info.get("numbers")
            result = completed_info.get("result")
        
        # Priority 2: Worker processing data (if scheduler data missing)
        if worker_info and (warmth_state is None or cold_start_penalty == 0.0):
            cold_start_penalty = worker_info.get("cold_start_penalty", cold_start_penalty)
            warmth_state = warmth_state or worker_info.get("warmth_state")
            is_cold_start = is_cold_start if is_cold_start is not None else worker_info.get("is_cold_start")
            warmth_level = warmth_level if warmth_level is not None else worker_info.get("warmth_level")
        
        # Priority 3: Check worker cold start events for this job
        if (warmth_state is None or cold_start_penalty == 0.0):
            for worker_id, worker_details in worker_data.get("workers", {}).items():
                for event in worker_details.get("cold_start_events", []):
                    if event.get("job_id") == job_id:
                        cold_start_penalty = cold_start_penalty or event.get("penalty_ms", 0.0)
                        warmth_state = warmth_state or event.get("warmth_state")
                        is_cold_start = is_cold_start if is_cold_start is not None else event.get("is_cold_start")
                        warmth_level = warmth_level if warmth_level is not None else event.get("warmth_level")
                        break
        
        # Get processing time from multiple sources
        processing_time = (worker_info.get("processing_time") or 
                          completed_info.get("processing_time") or 
                          0.0)
        
        row = {
            "job_id": job_id,
            "status": job_info.get("status"),
            "phase": job_info.get("phase"),
            "assigned_worker": job_info.get("assigned_worker"),
            "received_time": job_info.get("received_time"),
            "assigned_time": job_info.get("assigned_time"),
            "completed_time": job_info.get("completed_time"),
            "failed_time": job_info.get("failed_time"),
            "queue_time": None,  # Will be calculated below in milliseconds
            "processing_time": processing_time,  # Keep in seconds as per original data
            "cold_start_penalty": cold_start_penalty,  # In milliseconds
            "warmth_state": warmth_state,
            "warmth_level": warmth_level,
            "is_cold_start": is_cold_start,
            # Additional debugging fields
            "operation": operation,
            "numbers": str(numbers) if numbers else None,
            "result": str(result) if result else None,
            
            # ML algorithm columns
            "job_algorithm": job_algorithm,
            "worker_algorithm": worker_algorithm, 
            "iteration": iteration,
            "benchmark_mode": benchmark_mode,
            
            # Inter-arrival characteristics (from the mapping)
            "inter_arrival_category": arrival_info['category'],
            "batch_delay_range": arrival_info['range'],
            
            # Derived features
            "completed_successfully": job_info.get("status") == "completed",
        }
        
        # Compute queue time in milliseconds (convert from seconds)
        if job_info.get("received_time") and job_info.get("assigned_time"):
            queue_time_seconds = job_info["assigned_time"] - job_info["received_time"]
            row["queue_time"] = queue_time_seconds * 1000.0  # Convert to milliseconds
            
        # === REAL-WORLD TIME CONVERSIONS FOR ML TRAINING ===
        # Add real-world equivalent columns for 1:60 scaled metrics
        
        # Duration conversions (1:60 scale applied)
        row["processing_time_real_world_ms"] = row.get("processing_time", 0.0)  # No scaling - actual processing time
        row["queue_time_real_world_ms"] = row.get("queue_time", 0.0)  # No scaling - actual queue time
        
        # Cold start penalty conversions (NO scaling - these are response time penalties)
        row["cold_start_penalty_real_world_ms"] = row.get("cold_start_penalty", 0.0)  # No scaling - actual penalty
        
        # Warm penalty calculation (20% of cold start base)
        cold_penalty = row.get("cold_start_penalty", 0.0)
        if row.get("warmth_state") == "WARM" and cold_penalty > 0:
            # WARM penalty is 20% of the full cold start penalty
            row["warm_penalty_real_world_ms"] = cold_penalty * 0.2
        else:
            row["warm_penalty_real_world_ms"] = 0.0
            
        # Total startup penalty (cold + warm)
        row["total_startup_penalty_real_world_ms"] = (
            row.get("cold_start_penalty_real_world_ms", 0.0) + 
            row.get("warm_penalty_real_world_ms", 0.0)
        )
        
        # Container lifecycle impact classification
        row["container_lifecycle_impact"] = "none"
        if row.get("warmth_state") == "COLD":
            row["container_lifecycle_impact"] = "cold_start"
        elif row.get("warmth_state") == "WARM":
            row["container_lifecycle_impact"] = "warm_start"
        elif row.get("warmth_state") == "HOT":
            row["container_lifecycle_impact"] = "hot_start"
            
        # Real-world threshold references (for ML feature engineering)
        row["warm_threshold_real_world_min"] = 60.0  # 60 minutes real-world
        row["hot_threshold_real_world_min"] = 6.0    # 10% of 60 minutes
        
        # Performance impact category for ML
        row["user_experience_impact"] = classify_job_impact(
            row.get("cold_start_penalty_real_world_ms", 0.0),
            row.get("warm_penalty_real_world_ms", 0.0),
            row.get("processing_time_real_world_ms", 0.0)
        )
        
        # Time scaling documentation
        row["time_scaling_explanation"] = "1:60_thresholds_only_penalties_realtime"
        
        

        rows.append(row)

    df = pd.DataFrame(rows)
    out_path = log_dir / f"{run_name}_jobs.csv"
    df.to_csv(out_path, index=False)
    
    # Log summary statistics for debugging
    total_jobs = len(df)
    completed_jobs_count = len(df[df['status'] == 'completed'])
    jobs_with_cold_start_penalty = len(df[df['cold_start_penalty'] > 0])
    jobs_with_warmth_state = len(df[df['warmth_state'].notna()])
    cold_start_jobs = len(df[df['is_cold_start'] == True])
    
    logger.info(f"Exported job-level data to: {out_path}")
    logger.info(f"Job export summary:")
    logger.info(f"  Total jobs: {total_jobs}")
    logger.info(f"  Completed jobs: {completed_jobs_count}")
    logger.info(f"  Jobs with cold start penalty: {jobs_with_cold_start_penalty}")
    logger.info(f"  Jobs with warmth state: {jobs_with_warmth_state}")
    logger.info(f"  Jobs marked as cold start: {cold_start_jobs}")
    
    if jobs_with_cold_start_penalty > 0:
        avg_penalty = df[df['cold_start_penalty'] > 0]['cold_start_penalty'].mean()
        logger.info(f"  Average cold start penalty: {avg_penalty:.1f}ms")
    
    return out_path

def classify_job_impact(cold_penalty_ms, warm_penalty_ms, processing_time_ms):
    """
    Classify the user experience impact of a job based on penalties and processing time.
    
    Args:
        cold_penalty_ms: Cold start penalty in milliseconds
        warm_penalty_ms: Warm start penalty in milliseconds  
        processing_time_ms: Actual processing time in milliseconds
        
    Returns:
        str: Impact category for ML training
    """
    total_penalty = cold_penalty_ms + warm_penalty_ms
    
    if total_penalty == 0:
        return "no_penalty_impact"
    elif total_penalty < 100:  # Less than 100ms total penalty
        return "minimal_penalty_impact"
    elif total_penalty < 300:  # 100-300ms penalty
        return "moderate_penalty_impact"
    else:  # 300ms+ penalty
        return "significant_penalty_impact"
    
def consolidate_job_data_for_ml(results_dir: Path, run_results: List[Dict]) -> Optional[Path]:
    """
    Consolidate all job-level CSVs into a single ML-ready dataset.
    
    Args:
        results_dir: Directory where consolidated dataset should be saved
        run_results: List of run result dictionaries from benchmark
        
    Returns:
        Path to the consolidated CSV file, or None if no data found
    """
    all_job_data = []
    successful_runs = 0
    total_jobs = 0
    
    logger.info("🔄 Consolidating job-level data for ML...")
    
    for result in run_results:
        run_name = result.get("run_name")
        if not run_name:
            logger.warning("Skipping result with missing run_name")
            continue
            
        # Look for job CSV in the run's log directory
        job_csv_path = Path("logs") / run_name / f"{run_name}_jobs.csv"
        
        if job_csv_path.exists():
            try:
                df = pd.read_csv(job_csv_path)
                
                # Validate that we have the expected ML columns
                required_columns = ['job_algorithm', 'worker_algorithm', 'benchmark_mode']
                missing_columns = [col for col in required_columns if col not in df.columns]
                
                if missing_columns:
                    logger.warning(f"Skipping {run_name} - missing columns: {missing_columns}")
                    continue
                
                all_job_data.append(df)
                successful_runs += 1
                total_jobs += len(df)
                logger.debug(f"✅ Added {len(df)} jobs from {run_name}")
                
            except Exception as e:
                logger.warning(f"Could not read job data from {job_csv_path}: {e}")
        else:
            logger.warning(f"Job CSV not found: {job_csv_path}")
    
    if not all_job_data:
        logger.error("❌ No job-level data found for consolidation!")
        return None
    
    # Combine all job data
    logger.info(f"📊 Combining data from {successful_runs} successful runs...")
    combined_df = pd.concat(all_job_data, ignore_index=True)
    
    # Create meaningful filename with timestamp
    timestamp = time.strftime("%Y%m%d_%H%M%S")
    consolidated_filename = f"ml_dataset_jobs_{timestamp}.csv"
    consolidated_path = results_dir / consolidated_filename
    
    # Save consolidated dataset
    combined_df.to_csv(consolidated_path, index=False)
    
    # Generate detailed summary statistics
    algorithm_combinations = combined_df[['job_algorithm', 'worker_algorithm']].drop_duplicates()
    benchmark_modes = combined_df['benchmark_mode'].unique()
    completed_jobs = len(combined_df[combined_df['status'] == 'completed'])
    cold_start_jobs = len(combined_df[combined_df['is_cold_start'] == True])
    
    logger.info("🎉 ML Dataset Consolidation Complete!")
    logger.info(f"📁 Saved to: {consolidated_path}")
    logger.info(f"📊 Dataset Statistics:")
    logger.info(f"   • Total jobs: {len(combined_df):,}")
    logger.info(f"   • Completed jobs: {completed_jobs:,} ({completed_jobs/len(combined_df)*100:.1f}%)")
    logger.info(f"   • Cold start jobs: {cold_start_jobs:,} ({cold_start_jobs/len(combined_df)*100:.1f}%)")
    logger.info(f"   • Algorithm combinations: {len(algorithm_combinations)}")
    logger.info(f"   • Benchmark modes: {list(benchmark_modes)}")
    logger.info(f"   • Runs included: {successful_runs}")
    logger.info(f"   • Dataset shape: {combined_df.shape}")
    
    # Log algorithm distribution
    logger.info("🔧 Algorithm Distribution:")
    algo_counts = combined_df.groupby(['job_algorithm', 'worker_algorithm']).size().sort_values(ascending=False)
    for idx, count in algo_counts.head(10).items():
        if isinstance(idx, tuple) and len(idx) == 2:
            job_algo, worker_algo = idx
        else:
            job_algo, worker_algo = idx, ""
        logger.info(f"   • {job_algo} + {worker_algo}: {count:,} jobs")
    
    return consolidated_path