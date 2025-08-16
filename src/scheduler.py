"""
Meta TaskWave Scheduler Module

Central coordinator for the TaskWave distributed scheduling system.
Manages job distribution using pluggable scheduling algorithms.

Supported algorithms:
- Job-side: Round Robin, EDF, Urgency-based
- Worker-side: Random, Round Robin, Least Loaded, Fastest, Network Optimal

Key features:
- Asynchronous job queue management
- Worker registration and health monitoring
- Phase transition coordination
- Component-based algorithm selection
- Real-time metrics collection
"""

import os
import asyncio
import json
import random
import time
import uuid
import argparse
import logging
import sys
from typing import Dict, Any, List, Optional, Tuple, Callable, Awaitable

import heapq
from collections import deque

import config
from utils import setup_logger, setup_coordinated_logging, AsyncMessageHandler
from job import Job

# ---------------------------------- LOGGING SETUP ----------------------------------- #
# Coordinated logging setup: ensure logs are written to correct per-run folder.
run_name = os.getenv("TASKWAVE_RUN_NAME", time.strftime("run_%Y-%m-%d_run_fallback"))
log_dir = setup_coordinated_logging(run_name)
logger = setup_logger("Scheduler", log_level=config.LOGGING_LEVEL, console_level=logging.INFO, log_dir=log_dir)

# ---------------------------------- SCHEDULER CLASS ----------------------------------- #


class Scheduler:
    """
    Asynchronous scheduler with component-based architecture for ML-driven algorithm selection.

    Supports both legacy scheduling algorithms and new component-based approach where ML selects optimal job+worker algorithm combinations.

    Job side algorithm:
    1. RR Job - FIFO job scheduling
    2. EDF Job - Earliest Deadline First job scheduling; no deadline jobs = FIFO.
    3. Urgency Job - Based on Urgency Score (priority + deadline)

    Worker side algorithm:
    1. Random Worker - Randomly selects a worker for job assignment.
    2. RR Worker - Round Robin worker selection.
    3. Least Loaded Worker - Selects the worker with the least current load.
    4. Fastest Worker - Selects the worker with the fastest processing time
    5. Best network - Organise worker by latency and bandwidth (similar to proxy selection). Choose next nearest worker if full.
    
    Changes to implement phase tracking in the scheduler - 24 Jun 2025

    This adds:
    1. Phase tracking properties to track warm-up and measurement phases
    2. Method to transition from warm-up to measurement phase
    3. Phase information in job assignments
    4. Phase-aware job result processing
    """

    def __init__(self, job_algorithm=None, worker_algorithm=None):
        # Server configuration
        self.port: int = config.PORT
        self.server_ip: str = config.SERVER_IP
        self.address: Tuple[str, int] = (self.server_ip, self.port)
        self.server: Optional[asyncio.Server] = None
        self.running: bool = False

        # Worker management
        self.worker_connections: Dict[str, Tuple[asyncio.StreamReader,
                                                 asyncio.StreamWriter, AsyncMessageHandler]] = {} 
                                                 # -- worker_id -> {capacity, load, processing_time}
        
        self.worker_info: Dict[str, Dict[str, Any]] = {}
        self.worker_status: Dict[str, str] = {}     # worker_id -> status (active, idle, busy)
        self.worker_queue: deque = deque()          # For round robin/FIFO worker selection
        self.worker_capacity: Dict[str, int] = {}   # worker_id -> capacity

        # worker_id -> list of operations
        self.worker_capabilities: Dict[str, List[str]] = {}

        # -- Worker performance characteristics
        self.worker_processing_speed: Dict[str, float] = {} # worker_id -> processing speed multiplier
        self.worker_latency: Dict[str, float] = {}          # worker_id -> network latency
        self.worker_bandwidth: Dict[str, float] = {}        # worker_id -> bandwidth
        self.worker_network_score: Dict[str, float] = {}    # worker_id -> network score (bandwidth/latency)

        # Job management
        self.job_queue: List[Any] = []                  # List of pending jobs (heap in EDF)
        self.job_mapping: Dict[int, Job] = {}           # job_id -> Job instance
        self.job_assignments: Dict[str, List[int]] = {} # worker_id -> list of assigned job_ids
        self.completed_jobs: Dict[int, Any] = {}        # job_id -> result
        self.failed_jobs: Dict[int, str] = {}           # job_id -> error message

        # Task Management
        self.tasks: List[asyncio.Task] = []             # List of active tasks for cancellation
        
        # Statistics
        self.stats: Dict[str, Any] = {
            "jobs_received": 0,
            "jobs_completed": 0,
            "jobs_failed": 0,
            "total_processing_time": 0.0,
            "avg_queue_time": 0.0,
            "total_queue_time": 0.0
        }

        # Phase tracking
        self.warm_up_phase = True  # Start in warm-up phase
        self.warm_up_end_time = None  # When warm-up phase ended
        self.last_warmup_job_id = None  # Last job ID sent during warm-up
        self.measurement_stats = {
            "jobs_sent": 0,
            "jobs_completed": 0,
            "cold_starts": 0
        }
        
        # Component strategy mappings
        self.job_strategies: Dict[str, Callable[[], Awaitable[Optional[Job]]]] = {
            "rr_job": self.rr_job_selection,
            "edf_job": self.edf_job_selection,
            "urgency_job": self.urgency_job_selection
        }

        self.worker_strategies = {
            "random_worker": self.random_worker_selection,
            "rr_worker": self.round_robin_worker_selection,
            "least_loaded_fair": self.least_loaded_fair_selection,        
            "fastest_worker_fair": self.fastest_worker_fair_selection,    
            "network_optimal_fair": self.network_optimal_fair_selection   
        }


        # Default job algorithm if not specified
        if job_algorithm:
            self.job_algorithm = job_algorithm
        else:
            self.job_algorithm = config.JOB_SCHEDULING_ALGORITHM
            # Remove "_selection" suffix if present
            if self.job_algorithm.endswith("_selection"):
                self.job_algorithm = self.job_algorithm[:-10]

        # Default worker algorithm if not specified
        if worker_algorithm:
            self.worker_algorithm = worker_algorithm
        else:
            self.worker_algorithm = config.WORKER_SCHEDULING_ALGORITHM
            # Remove "_selection" suffix if present
            if self.worker_algorithm.endswith("_selection"):
                self.worker_algorithm = self.worker_algorithm[:-10]

        # Validate selected algorithms
        if self.job_algorithm not in self.job_strategies:
            logger.warning(
                f"Unknown job algorithm: {self.job_algorithm}, falling back to rr_job")
            self.job_algorithm = "rr_job"

        if self.worker_algorithm not in self.worker_strategies:
            logger.warning(
                f"Unknown worker algorithm: {self.worker_algorithm}, falling back to random_worker")
            self.worker_algorithm = "random_worker"

        # All possible algorithm combinations (3 job × 5 worker = 15)
        self.all_combinations: List[Tuple[str, str]] = [
            (j, w) for j in self.job_strategies.keys()
            for w in self.worker_strategies.keys()
        ]

        logger.info(
            f"Scheduler initialised with job algorithm: {self.job_algorithm}, worker algorithm: {self.worker_algorithm}")
        logger.info(
            f"Component architecture initialised with {len(self.all_combinations)} algorithm combinations")


    # ------------------------------- MEASUREMENT PHASE HANDLING -------------------------------- #
    async def transition_to_measurement_phase(self, transition_timestamp=None):
        """
        Signal the transition from warm-up to measurement phase.
        
        Args:
            transition_timestamp: Optional timestamp from benchmark indicating exact transition time
        
        This method:
        1. Updates the scheduler's phase state
        2. Records the last warm-up job ID
        3. Notifies all workers to enter measurement phase
        """
        
        self.warm_up_phase = False
        
        # Use provided timestamp if available, otherwise use current time
        self.warm_up_end_time = transition_timestamp or time.time()
        
         # Find the highest job ID that has been assigned to a worker during warm-up
        assigned_job_ids = []
        for worker_id in self.worker_connections:
            if worker_id in self.job_assignments:
                assigned_job_ids.extend(self.job_assignments[worker_id])
                
        # Also check completed and failed jobs
        assigned_job_ids.extend(self.completed_jobs.keys())
        assigned_job_ids.extend(self.failed_jobs.keys())
        
        # Set the last warm-up job ID to the highest assigned job ID
        self.last_warmup_job_id = max(assigned_job_ids) if assigned_job_ids else 0
        
        logger.info(f"★★★ Scheduler transitioning to measurement phase ★★★")
        logger.info(f"Last warm-up job ID: {self.last_warmup_job_id} (highest assigned/completed job during warm-up)")
    
        # Notify all workers
        for worker_id, (_, _, message_handler) in self.worker_connections.items():
            try:
                transition_message = {
                    "type": "ENTER_MEASUREMENT_PHASE",
                    "timestamp": time.time()
                }
                await message_handler.send_message(transition_message)
                logger.info(f"Sent measurement phase transition to worker {worker_id}")
            except Exception as e:
                logger.error(f"Failed to send phase transition to worker {worker_id}: {e}")

    # ---------------------------------- CONNECTION HANDLING ----------------------------------- #

    async def start_server(self):
        """Start the server and listen for incoming connections."""
        self.running = True

        try:
            self.server = await asyncio.start_server(self.handle_new_connection, self.server_ip, self.port)

            addr = self.server.sockets[0].getsockname()
            logger.info(f'Server running on {addr[0]}:{addr[1]}')

            # Start dispatched for scheduling algorithms
            scheduling_task = asyncio.create_task(
                self.scheduling_algorithm_dispatcher())
            self.tasks.append(scheduling_task)

            # Start broadcast to workers for monitoring
            monitoring_task = asyncio.create_task(
                self.outbound_worker_heartbeat_loop())
            self.tasks.append(monitoring_task)

            async with self.server:
                await self.server.serve_forever()
        except asyncio.CancelledError:
            logger.warning("Server task cancelled.")
        except Exception as e:
            logger.error(f"Server error: {e}")
        finally:
            await self.stop_server()

    async def stop_server(self):
        """Gracefully stop the server."""
        self.running = False

        # Close all worker connections
        for worker_id, (_, writer, _) in list(self.worker_connections.items()):
            try:
                writer.close()
                await writer.wait_closed()
            except Exception as e:
                logger.error(f"Error closing connection for {worker_id}: {e}")

        # Cancel all tasks
        for task in self.tasks:
            if not task.done():
                task.cancel()

        try:
            await asyncio.gather(*self.tasks, return_exceptions=True)
        except Exception as e:
            logger.error(f"Error cancelling tasks: {e}")

        # Close server
        if self.server:
            self.server.close()
            await self.server.wait_closed()

        logger.info("Server stopped.")

    async def handle_new_connection(self, reader, writer):
        """
        Handle a new client connection.

        Determines which client is trying to connect to the server.
        Imitates a three way handshake using SYN_ as the initialiser.
        - SYN_WORKER
        - SYN_JOB_GENERATOR
        """
        addr = writer.get_extra_info('peername')
        logger.info(f"New connection from {addr}")

        # Each connection creates its own message handler
        message_handler = AsyncMessageHandler(reader, writer, addr)

        try:
            initial_message = await message_handler.receive_message()

            if not initial_message or not isinstance(initial_message, dict):
                logger.warning(f"Invalid handshake message from {addr}")
                writer.close()
                await writer.wait_closed()
                return

            message_type = initial_message.get("type", "")

            if message_type == "SYN_JOB_GENERATOR":
                job_gen_id = initial_message.get(
                    "client_id", f"jobgen_{str(uuid.uuid4())[:8]}")

                ack_message = {"status": "ACK_JOB_GENERATOR"}
                await message_handler.send_message(ack_message)

                # Start job generator handler
                job_gen_task = asyncio.create_task(
                    self.inbound_job_generator_messages(reader, writer, message_handler, job_gen_id))
                self.tasks.append(job_gen_task)

                logger.info(
                    f"Job Generator {job_gen_id} connected from {addr}")

            elif message_type == "SYN_WORKER" and "worker_id" in initial_message:
                worker_id = initial_message["worker_id"]

                ack_message = {"status": "ACK_WORKER"}
                await message_handler.send_message(ack_message)

                # Register worker
                self.register_worker(
                    worker_id, reader, writer, message_handler, initial_message)

                # Start worker handler
                worker_task = asyncio.create_task(
                    self.inbound_worker_messages(worker_id, reader, writer, message_handler))
                self.tasks.append(worker_task)

                logger.info(f"Worker {worker_id} connected from {addr}")

            elif message_type == "START_MEASUREMENT_PHASE":
                logger.info("[MEASUREMENT PHASE] Received command to start measurement phase from Benchmarker.")
                
                # Get the timestamp from the message
                transition_timestamp = initial_message.get("timestamp")
                run_name = initial_message.get("run_name", "unknown")
                
                logger.info(f"Starting measurement phase for run '{run_name}' with timestamp {transition_timestamp}")
                
                # Pass the timestamp to the transition method
                await self.transition_to_measurement_phase(transition_timestamp)
                
                # Send acknowledgment
                ack_message = {
                    "status": "MEASUREMENT_PHASE_STARTED",
                    "timestamp": time.time()
                }
                await message_handler.send_message(ack_message)
                
                writer.close()
                await writer.wait_closed()
            
            else:
                logger.warning(
                    f"Unknown client type from {addr}: {message_type}")
                writer.close()
                await writer.wait_closed()

        except Exception as e:
            logger.error(f"Error handling new connection from {addr}: {e}")
            writer.close()
            await writer.wait_closed()

    # ---------------------------------- JOB GENERATOR MANAGEMENT ----------------------------------- #
    # This part handles the job generator's connection and adds it to the job queue. The job queue is then processed by the job scheduling loop

    async def inbound_job_generator_messages(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter, message_handler: AsyncMessageHandler, job_gen_id: str):
        """Handle incoming job generator connections and messages."""
        addr = writer.get_extra_info('peername')

        try:
            while self.running:
                job_data = await message_handler.receive_message()

                if not job_data:
                    await asyncio.sleep(0.1)
                    continue

                if isinstance(job_data, dict) and job_data.get("type") == "DISCONNECT":
                    logger.info(
                        f"Job Generator {job_gen_id} is disconnecting.")
                    break

                if not isinstance(job_data, dict) or not all(key in job_data for key in ["job_id", "operation", "numbers", "job_load"]):
                    logger.warning(
                        f"Invalid job from {job_gen_id}: {job_data}")
                    continue

                job = Job(
                    job_id=job_data["job_id"],
                    operation=job_data["operation"],
                    numbers=job_data["numbers"],
                    job_load=job_data["job_load"],
                    priority=job_data.get("priority", 0),
                    deadline=job_data.get("deadline", 0.0)
                )

                await self.add_job(job)

                self.stats["jobs_received"] += 1

                logger.info(
                    f"Job {job.job_id} added to queue from generator {job_gen_id}")

        except asyncio.CancelledError:
            logger.info(f"Job generator handler for {job_gen_id} cancelled")
        except Exception as e:
            logger.error(f"Error handling job generator {job_gen_id}: {e}")
        finally:
            writer.close()
            await writer.wait_closed()
            logger.info(f"Job Generator {job_gen_id} disconnected")

    async def add_job(self, job: Job):
        """INTERNAL. Add a new job to the job queue."""
        # Store job in job mapping
        self.job_mapping[job.job_id] = job

        # Enhanced logging with job details
        logger.info(
            f"Job {job.job_id} added to queue: "
            f"operation={job.operation}, load={job.job_load}, "
            f"priority={job.priority}, deadline={job.deadline:.1f}, "
            f"urgency_score={job.urgency_score:.2f}"
        )
    
        if self.job_algorithm in ["edf_job"]:
            if job.deadline > 0:
                heapq.heappush(self.job_queue, (job.deadline, job.job_id))
            else:
                max_wait_time = 60  # seconds
                virtual_deadline = job.creation_time + max_wait_time
                heapq.heappush(self.job_queue, (virtual_deadline, job.job_id))
        elif self.job_algorithm in ["urgency_job"]:
            # For Urgency-First, use urgency score (higher urgency = lower number for min-heap)
            heapq.heappush(self.job_queue, (-job.urgency_score, job.job_id))
        else:
            # Default/RR: just append job_id for FIFO processing
            self.job_queue.append(job.job_id)

        logger.debug(
            f"Job {job.job_id} added to queue with algorithm {self.job_algorithm}")

    # ---------------------------------- WORKER MANAGEMENT ----------------------------------- #
    # This part handles the worker connections and adds them to the worker queue. The worker queue is then processed by the job scheduling loop

    def register_worker(self, worker_id: str, reader: asyncio.StreamReader, writer: asyncio.StreamWriter, message_handler: AsyncMessageHandler, worker_info: Dict[str, Any]):
        """Register a new worker connection with the scheduler."""
        self.worker_connections[worker_id] = (reader, writer, message_handler)
        self.worker_info[worker_id] = worker_info

        self.worker_status[worker_id] = worker_info.get(
            "status", "active")  # Default to active if not specified
        self.worker_capacity[worker_id] = worker_info.get(
            "max_capacity", 1000)  # Default to min capacity
        self.worker_capabilities[worker_id] = worker_info.get(
            "capabilities", [])

        self.worker_processing_speed[worker_id] = worker_info.get(
            "processing_speed", 1.0)
        self.worker_latency[worker_id] = worker_info.get(
            "latency", 0.125)  # Default to median latency (0.05 to 0.2)
        self.worker_bandwidth[worker_id] = worker_info.get(
            "bandwidth", 125)  # Default to median bandwidth (50 to 200 Mbps)
        self.worker_network_score[worker_id] = worker_info.get(
            "network_score", 10.0)  # Min network score of 10.0

        # Add worker to queue for round robin selection
        self.worker_queue.append(worker_id)
        # Initialise job assignments for this worker
        self.job_assignments[worker_id] = []

        logger.info(f"Worker {worker_id} registered with capacity {self.worker_capacity[worker_id]}, "
                    f"processing speed {self.worker_processing_speed[worker_id]}, "
                    f"network score {self.worker_network_score[worker_id]}")

    async def unregister_worker(self, worker_id: str):
        "Removes worker from the scheduler's worker list and queues."""
        logger.info(f"Unregistering worker {worker_id}")

        # If worker has any job assigned, remove them from the job_assignments loop and requeue them
        if worker_id in self.job_assignments:
            for job_id in list(self.job_assignments[worker_id]):
                if job_id in self.job_mapping:
                    job = self.job_mapping[job_id]
                    job.status = "pending"
                    job.worker_id = None

                    await self.add_job(job)   # Requeue job
                    logger.info(
                        f"Requeued job {job_id} from worker {worker_id}")

            # Clear job assignments for this worker
            del self.job_assignments[worker_id]

        if worker_id in self.worker_connections:
            del self.worker_connections[worker_id]
        if worker_id in self.worker_info:
            del self.worker_info[worker_id]
        if worker_id in self.worker_status:
            del self.worker_status[worker_id]
        if worker_id in self.worker_capacity:
            del self.worker_capacity[worker_id]
        if worker_id in self.worker_capabilities:
            del self.worker_capabilities[worker_id]
        if worker_id in self.worker_processing_speed:
            del self.worker_processing_speed[worker_id]
        if worker_id in self.worker_latency:
            del self.worker_latency[worker_id]
        if worker_id in self.worker_bandwidth:
            del self.worker_bandwidth[worker_id]
        if worker_id in self.worker_network_score:
            del self.worker_network_score[worker_id]

        if worker_id in self.worker_queue:
            self.worker_queue.remove(worker_id)

        logger.info(f"Worker {worker_id} unregistered successfully")

    async def inbound_worker_messages(self, worker_id: str, reader: asyncio.StreamReader, writer: asyncio.StreamWriter, message_handler: AsyncMessageHandler):
        """
        Handle incoming worker connections.
        This async method waits for messages from the worker and checks if the worker wants to:
        1. Receive status update (worker statistics)
        2. Receive job result (Uses inbound_process_job_result method below to hand result)
        3. Receive a pong (heartbeat response to check for liveness)
        4. Receive disconnect notice and break worker connection
        """
        try:
            while self.running:
                if not message_handler:
                    logger.warning(
                        f"No message handler for worker {worker_id}. Cannot receive messages.")
                    break

                message = await message_handler.receive_message()

                if not message:
                    await asyncio.sleep(0.1)
                    continue

                # Check the message content
                if isinstance(message, dict):
                    # note: apart from ACK messages, all messages should have a type field
                    message_type = message.get("type", "")

                    if message_type == "DISCONNECT":
                        logger.info(
                            f"Worker {worker_id} requested disconnection.")
                        await self.unregister_worker(worker_id)
                        break
                    elif message_type == "PONG":
                        logger.debug(f"Received PONG from worker {worker_id}.")
                    elif message_type == "PHASE_TRANSITION_ACK":
                        success = message.get("success", False)
                        if success:
                            logger.info(f"Worker {worker_id} successfully transitioned to measurement phase")
                        else:
                            logger.warning(f"Worker {worker_id} failed to transition to measurement phase")
                    elif message_type == "STATUS_UPDATE":
                        # Receive the latest Job info
                        self.worker_info[worker_id] = message
                        if "status" in message:
                            self.worker_status[worker_id] = message["status"]
                            logger.debug(
                                f"Updated status for worker {worker_id}: {message.get('status', 'unknown')}")
                            logger.debug(f"Worker {worker_id} load: {message.get('capacity', {}).get('current_load', 'unknown')}/" +
                                         f"{message.get('capacity', {}).get('max_capacity', 'unknown')}, " +
                                         f"(load percentage at {message.get('capacity', {}).get('load_percentage', 'unknown')}%)")
                            logger.debug(f"Worker {worker_id} performance: {message.get('performance', {}).get('processing_speed', 'unknown')} Mbps processing speed, " +
                                         f"{message.get('performance', {}).get('latency', 'unknown')} latency, " +
                                         f"{message.get('performance', {}).get('bandwidth', 'unknown')} bandwidth, " +
                                         f"{message.get('performance', {}).get('network_score', 'unknown')} network score")
                            logger.debug(f"Worker {worker_id} job_stats: {message.get('job_stats', {}).get('jobs_completed', 'unknown')} jobs completed, " +
                                         f"{message.get('job_stats', {}).get('jobs_failed', 'unknown')} jobs failed, " +
                                         f"{message.get('job_stats', {}).get('active_jobs', 'unknown')} active jobs, " +
                                         f"{message.get('job_stats', {}).get('avg_processing_time', 'unknown')} average processing time")
                    elif message_type == "JOB_RESULT":
                        await self.inbound_process_job_result(message)
                    else:
                        logger.warning(
                            f"Unknown message type from worker {worker_id}: {message_type}")
                else:
                    logger.warning(
                        f"Received non-dictionary message from worker {worker_id}")
        except asyncio.CancelledError:
            logger.info(f"Worker handler for {worker_id} cancelled")
        except Exception as e:
            logger.error(f"Error handling worker {worker_id}: {e}")
        finally:
            await self.unregister_worker(worker_id)
            if writer and not writer.is_closing():
                writer.close()
                await writer.wait_closed()
            logger.info(f"Worker {worker_id} disconnected")

    async def inbound_process_job_result(self, result: Dict[str, Any]):
        """
        Enhanced job result processing that preserves cold start and warmth data.
        """
        if not result or not isinstance(result, dict):
            logger.error("Invalid job result received")
            return False

        job_id = result.get("job_id")
        worker_id = result.get("worker_id")
        status = result.get("status")
        job_phase = result.get("phase", "unknown")

        # Ensure job_id is not None and is an integer
        if job_id is None:
            logger.error("Missing job_id in job result")
            return False
        
        # Convert job_id to int if it's a string
        try:
            job_id = int(job_id)
        except (ValueError, TypeError):
            logger.error(f"Invalid job_id format: {job_id}")
            return False

        if not all([worker_id, status]):
            logger.error(f"Missing required fields in job result: {result}")
            return False

        if status == "failed":
            # Extract cold start data even for failed jobs (for analysis)
            failed_cold_start_penalty = result.get("cold_start_penalty", 0.0)
            failed_processing_time = result.get("processing_time", 0.0)
            failed_warmth_state = result.get("warmth_state", "unknown")
            
            logger.warning(
                f"Job {job_id} failed with error: {result.get('result', 'Unknown error')} "
                f"Processing time to failure: {failed_processing_time:.3f}s, "
                f"Cold start penalty: {failed_cold_start_penalty:.1f}ms, "
                f"Warmth state: {failed_warmth_state}"
            )

            if job_id in self.job_mapping:
                job = self.job_mapping[job_id]
                job.status = status
                job.result = result.get('result', 'Unknown error')
                self.job_mapping[job_id] = job

            # Record failure
            self.failed_jobs[job_id] = result.get("result", "Unknown error")
            self.stats["jobs_failed"] += 1

            # Remove the job_id from the worker's assignments
            if worker_id in self.job_assignments and job_id in self.job_assignments[worker_id]:
                self.job_assignments[worker_id].remove(job_id)

            # Requeue by calling add_job
            logger.info(f"Requeuing job {job_id} for reassignment")
            if job_id in self.job_mapping:
                job = self.job_mapping[job_id]
                job.status = "pending"
                job.worker_id = None
                await self.add_job(job)

            return False

        elif status == "completed":
            # Extract all cold start and warmth data from worker result
            cold_start_penalty = result.get("cold_start_penalty", 0.0)
            warmth_state = result.get("warmth_state", "unknown")
            warmth_level = result.get("warmth_level", None)
            is_cold_start = result.get("is_cold_start", False)
            processing_time = result.get("processing_time", 0.0)
            operation = result.get("operation", "unknown")
            numbers = result.get("numbers", [])
            job_result = result.get("result", "No result")

            logger.info(
                f"Job {job_id} completed successfully (phase={job_phase}). "
                f"Operation: {operation} on {numbers}. "
                f"Result: {job_result}. "
                f"Processing time: {processing_time:.3f}s, "
                f"Cold start penalty: {cold_start_penalty:.1f}ms, "
                f"Warmth state: {warmth_state}, "
                f"Warmth level: {warmth_level}, "
                f"Is cold start: {is_cold_start}"
            )

            # Determine if this is a measurement phase job
            is_measurement_job = job_phase == "measurement"

            # If not determined by phase, check job ID (but only if we're in measurement phase)
            if not is_measurement_job and not self.warm_up_phase:
                if self.last_warmup_job_id is not None:
                    is_measurement_job = job_id > self.last_warmup_job_id

            # Track measurement phase stats
            if is_measurement_job:
                self.measurement_stats["jobs_completed"] += 1
                
                # Track cold starts in measurement phase
                if is_cold_start and cold_start_penalty > 0:
                    self.measurement_stats["cold_starts"] += 1
                    logger.info(
                        f"MEASUREMENT_PHASE_COLD_START job_id={job_id} "
                        f"penalty={cold_start_penalty:.1f}ms worker={worker_id} "
                        f"warmth_state={warmth_state}"
                    )

            # Update job status in mapping
            if job_id in self.job_mapping:
                job = self.job_mapping[job_id]
                job.status = "completed"
                job.result = job_result
                self.job_mapping[job_id] = job
                
            # Record completion with comprehensive cold start data
            self.completed_jobs[job_id] = {
                "result": job_result,
                "operation": operation,
                "numbers": numbers,
                "processing_time": processing_time,
                "cold_start_penalty": cold_start_penalty,
                "warmth_state": warmth_state,
                "warmth_level": warmth_level,
                "is_cold_start": is_cold_start,
                "phase": job_phase,
                "timestamp": result.get("timestamp", time.time()),
                "worker_id": worker_id
            }
            
            # Debug log to verify cold start data is being stored
            logger.debug(f"Stored completion data for job {job_id}: "
                        f"cold_start_penalty={cold_start_penalty}, "
                        f"warmth_state={warmth_state}, "
                        f"is_cold_start={is_cold_start}")

            self.stats["jobs_completed"] += 1
            self.stats["total_processing_time"] += processing_time

            # Remove job from worker's assignments
            if worker_id in self.job_assignments and job_id in self.job_assignments[worker_id]:
                self.job_assignments[worker_id].remove(job_id)

            return True

        elif status == "rejected":
            # Extract cold start data even for rejected jobs (for debugging)
            rejected_cold_start_penalty = result.get("cold_start_penalty", 0.0)
            rejected_warmth_state = result.get("warmth_state", "unknown")
            
            logger.warning(
                f"Job {job_id} was rejected by worker {worker_id}: "
                f"{result.get('result', 'No reason provided')} "
                f"(cold_start_penalty: {rejected_cold_start_penalty:.1f}ms, "
                f"warmth_state: {rejected_warmth_state})"
            )

            # Update job status and requeue
            if job_id in self.job_mapping:
                job = self.job_mapping[job_id]
                job.status = "pending"
                job.worker_id = None
                await self.add_job(job)

            return False

        else:
            logger.warning(f"Unknown job result status: {status}")
            return False
    
    
    async def outbound_worker_heartbeat_loop(self):
        """
        BROADCASTS heartbeat messages (PING) to all workers at regular intervals.
        Ensures workers are alive and responsive.
        """
        try:
            while self.running:
                await asyncio.sleep(config.HEARTBEAT_INTERVAL)

                for worker_id in list(self.worker_connections.keys()):
                    try:
                        connection_tuple = self.worker_connections.get(
                            worker_id)

                        if not connection_tuple or len(connection_tuple) != 3:
                            logger.warning(
                                f"Invalid connection data for worker {worker_id}")
                            await self.unregister_worker(worker_id)
                            continue

                        reader, writer, message_handler = connection_tuple

                        if not writer or writer.is_closing():
                            logger.warning(
                                f"Worker {worker_id} connection closed unexpectedly")
                            await self.unregister_worker(worker_id)
                            continue

                        ping_message = {
                            "type": "PING",
                            "timestamp": time.time()
                        }

                        await message_handler.send_message(ping_message)
                        logger.debug(f"Sent ping to worker {worker_id}")

                    except Exception as e:
                        logger.error(
                            f"Error monitoring worker {worker_id}: {e}")
                        await self.unregister_worker(worker_id)

                # Log phase metrics periodically
                current_time = time.time()
                if not self.warm_up_phase and self.warm_up_end_time and (current_time - self.warm_up_end_time) > 0:
                    
                    # Log every third heartbeat to avoid flooding logs
                    if int(current_time) % (config.HEARTBEAT_INTERVAL * 3) < 1.0:
                        cold_start_rate = 0
                        if self.measurement_stats["jobs_completed"] > 0:
                            cold_start_rate = (self.measurement_stats["cold_starts"] / self.measurement_stats["jobs_completed"]) * 100
                        logger.info(f"Measurement phase metrics: "
                                    f"jobs_sent={self.measurement_stats['jobs_sent']}, "
                                    f"jobs_completed={self.measurement_stats['jobs_completed']}, "
                                    f"cold_starts={self.measurement_stats['cold_starts']} "
                                    f"({cold_start_rate:.2f}% cold start rate)")

        except asyncio.CancelledError:
            logger.info("Worker heartbeat loop cancelled")

    # ---------------------------------- JOB ASSIGNMENT TO WORKER ----------------------------------- #

    async def outbound_assign_job_to_worker(self, job: Job, worker_id: str):
        """
        Assigns a job to worker and updates the job status to assigned if successful.
        Includes signalling for warm-up and measurement phase to worker.
        
        Args:
        - job: Job to assign
        - worker_id: worker ID to assign the job to

        Returns:
        - bool: True if assignment was successful, False otherwise.
        """
        if job.job_id not in self.job_mapping:  # Checks if job exists wiht all the job data
            logger.error(f"Job {job.job_id} not found in job mapping")
            return False

        if worker_id not in self.worker_connections:  # Checks if worker exists in worker connections
            logger.error(f"Worker {worker_id} not found in worker connections")
            return False

        if worker_id not in self.available_workers:  # If connected, this checks if worker is offline or overloaded
            logger.warning(
                f"Worker {worker_id} is not available for job assignment")
            return False

        # Prepare assignment message
        job.status = "assigned"
        job.worker_id = worker_id
        self.job_mapping[job.job_id] = job  # Update job in mapping

        # Determine current phase
        current_phase = "measurement" if not self.warm_up_phase else "warm_up"
        
        if current_phase == "measurement":
            # If in measurement phase, count jobs sent
            self.measurement_stats["jobs_sent"] += 1
        
        assignment_message = {
            "type": "JOB_ASSIGNMENT",
            "job": job.to_dict(),
            "phase": current_phase,
            "timestamp": time.time()
        }

        # After sending, update the assignment in job assignments
        self.job_assignments[worker_id].append(job.job_id)

        try:
            _, _, message_handler = self.worker_connections[worker_id]
            await message_handler.send_message(assignment_message)
            logger.info(f"Assigned job {job.job_id} to worker {worker_id} (phase: {current_phase})")
            return True
        except Exception as e:
            logger.error(
                f"Failed to send job {job.job_id} to worker {worker_id}: {e}")
            job.status = "pending"
            job.worker_id = None
            self.job_mapping[job.job_id] = job  # Update job in mapping
            self.job_assignments[worker_id].remove(job.job_id)
            return False

    # ---------------------------------- JOB SCHEDULING LOOP DISPATCHER ----------------------------------- #

    async def log_scheduling_decision(self, job, worker_id, algorithm_combo):
        """Log detailed scheduling decision for analysis."""
        decision_log = {
            "timestamp": time.time(),
            "job_id": job.job_id,
            "job_algorithm": self.job_algorithm,
            "worker_algorithm": self.worker_algorithm,
            "selected_worker": worker_id,
            "job_properties": {
                "priority": job.priority,
                "urgency_score": job.urgency_score,
                "deadline": job.deadline,
                "job_load": job.job_load,
                "age": job.age
            },
            "worker_properties": {
                "load_percentage": self.worker_info.get(worker_id, {}).get("capacity", {}).get("load_percentage", 0),
                "network_score": self.worker_network_score.get(worker_id, 0),
                "processing_speed": self.worker_processing_speed.get(worker_id, 1.0),
                "available_capacity": self.worker_capacity.get(worker_id, 0) - self.worker_info.get(worker_id, {}).get("capacity", {}).get("current_load", 0)
            },
            "system_state": {
                "total_jobs_queued": len(self.job_queue),
                "total_workers_available": len(self.available_workers),
                "avg_worker_load": sum(self.worker_info.get(w, {}).get("capacity", {}).get("load_percentage", 0) for w in self.available_workers) / max(1, len(self.available_workers))
            }
        }

        # Log to file for later analysis
        logger.info(f"SCHEDULING_DECISION: {json.dumps(decision_log)}")

        return decision_log

    async def scheduling_algorithm_dispatcher(self):
        """
        Main scheduling loop that selects jobs and assigns them to workers.
        """
        try:
            while self.running:
                # Skip if no jobs or workers
                if (not self.job_queue or len(self.job_queue) == 0) or not self.available_workers:
                    await asyncio.sleep(0.1)  # Prevent busy waiting
                    continue

                # Select job based on algorithm
                job_selection_strategy = self.job_strategies[self.job_algorithm]
                job = await job_selection_strategy()

                if not job:
                    await asyncio.sleep(0.1)
                    continue
                
                elif job:
                    # Determine current phase
                    current_phase = "measurement" if not self.warm_up_phase else "warm_up"
                    logger.debug(f"Selected job {job.job_id} in {current_phase} phase")
                        
                # Select worker based on algorithm
                worker_selection_strategy = self.worker_strategies[self.worker_algorithm]
                worker_id = await worker_selection_strategy()

                if not worker_id:
                    # No available worker, requeue
                    await self.add_job(job)
                    logger.warning(
                        f"No available worker for job {job.job_id}. Requeuing.")
                    await asyncio.sleep(0.1)
                    continue

                # Log the scheduling decision
                await self.log_scheduling_decision(job, worker_id, f"{self.job_algorithm}+{self.worker_algorithm}")

                # Assign job to worker
                success = await self.outbound_assign_job_to_worker(job, worker_id)

                if not success:
                    # Assignment failed, requeue job
                    await self.add_job(job)
                    logger.warning(
                        f"Failed to assign job {job.job_id} to worker {worker_id}. Requeuing.")

                # Small delay between scheduling cycles
                await asyncio.sleep(0.05)

        except asyncio.CancelledError:
            logger.info("Scheduling dispatcher loop cancelled")
        except Exception as e:
            logger.error(f"Error in scheduling dispatcher: {e}")

    # ---------------------------------- JOB SCHEDULING ALGORITHMS ----------------------------------- #

    async def rr_job_selection(self):
        """Round Robin job selection algorithm.

        1. Check if job_queue has jobs
        2. Pop first job (pop left)
        3. Return job object
        4. Handle both priority queue and regular queue formats"""
        if not self.job_queue:
            return None
        if isinstance(self.job_queue, deque):
            job_id = self.job_queue.popleft()
        elif isinstance(self.job_queue, list):  # If list
            job_id = self.job_queue.pop(0)
        else:
            logger.error(f"Unexpected job queue type: {type(self.job_queue)}")
            return None

        return self.job_mapping.get(job_id)

    async def edf_job_selection(self):
        """Earliest Deadline First job selection algorithm."""
        if not self.job_queue:
            return None
        try:
            # Current add_job setup already checks if deadline
            # If no deadline, add 60 seconds from creation.time    (deadline is 10-30 secs if deadline)
            deadline, job_id = heapq.heappop(self.job_queue)
            job = self.job_mapping.get(job_id)
            if not job:
                logger.warning(
                    f"Job {job_id} selected by EDF but not found in job mapping")
                return await self.edf_job_selection()  # Recursively try next job
            return job
        except (TypeError, ValueError) as e:
            logger.error(f"Error in EDF job selection: {e}")
            return None

    async def urgency_job_selection(self):
        """Urgency Score-based job selection algorithm."""
        if not self.job_queue:
            return None
        try:
            # add_job pushes the highest urgency in the heap
            urgency_score, job_id = heapq.heappop(self.job_queue)
            job = self.job_mapping.get(job_id)
            if not job:
                logger.warning(f"Job {job_id} not found in mapping")
                return await self.urgency_job_selection()  # Try next
            logger.debug(f"Selected job {job_id} with urgency {-urgency_score}")
            return job
        except Exception as e:
            logger.error(f"Error in urgency selection: {e}")
            return None

    # ---------------------------------- WORKER SCHEDULING ALGORITHMS ----------------------------------- #

    @property
    def available_workers(self):
        """List of worker IDs that are available for job assignment."""
        if not self.worker_connections:
            return []
        return [worker_id for worker_id in self.worker_connections.keys()
                if self.worker_status.get(worker_id) not in ["overloaded", "offline"]]

    async def random_worker_selection(self):
        """Randomly selects a worker for job assignment."""
        if not self.available_workers:
            return None
        return random.choice(self.available_workers)

    async def round_robin_worker_selection(self):
        """Round Robin worker selection algorithm."""
        if not self.worker_queue:
            return None
        try:
            for _ in range(len(self.worker_queue)):
                worker_id = self.worker_queue.popleft()
                # Return the worker to end of queue
                self.worker_queue.append(worker_id)
                if (worker_id in self.worker_connections and self.worker_status.get(worker_id) not in ["overloaded", "offline"]):
                    return worker_id
            logger.error(
                f"Reached the end of the worker_queue with no available worker")
            return None
        except Exception as e:
            logger.error(f"Failed to select worker using Round Robin: {e}")


    async def least_loaded_worker_selection(self):
        """DEPRECATED - Selects the worker with the least current load."""
        if not self.available_workers:
            return None
        try:

            # Log all workers' load percentages for debugging
            worker_loads = {}
            for worker_id in self.available_workers:
                load_pct = self.worker_info.get(worker_id, {}).get(
                    "capacity", {}).get("load_percentage", 100.0)
                worker_loads[worker_id] = load_pct
            logger.info(f"Worker load percentages: {worker_loads}")

            least_loaded_worker = min(
                self.available_workers,
                key=lambda w_id: self.worker_info.get(w_id, {})
                .get("capacity", {})
                .get("load_percentage", 100.0)
            )

            least_load = self.worker_info.get(least_loaded_worker, {}).get(
                "capacity", {}).get("load_percentage", 100.0)

            logger.info(
                f"Selected least loaded worker: {least_loaded_worker} with {least_load}% load")

            return least_loaded_worker

        except Exception as e:
            logger.error(f"Error selecting least loaded worker: {e}")
            return None


    async def least_loaded_fair_selection(self):
        """
        Selects the least loaded worker while preventing starvation.
        
        Strategy:
        1. If the least loaded worker is below fairness threshold (50%), use it
        2. If the least loaded worker is still quite busy (>50%), use round-robin instead
        3. This prevents one worker from being continuously overloaded while others are idle
        """
        if not self.available_workers:
            return None

        try:
            # Get worker load information
            worker_loads = {}
            for worker_id in self.available_workers:
                load_pct = self.worker_info.get(worker_id, {}).get(
                    "capacity", {}).get("load_percentage", 0.0)
                worker_loads[worker_id] = load_pct
            
            logger.debug(f"Worker loads for fair least loaded selection: {worker_loads}")
            
            # Find least loaded worker
            least_loaded_worker = min(
                self.available_workers,
                key=lambda w_id: worker_loads.get(w_id, 100.0)
            )
            
            least_load = worker_loads.get(least_loaded_worker, 100.0)
            fairness_threshold = 50.0
            
            # If least loaded worker is under threshold, use it
            if least_load < fairness_threshold:
                logger.debug(f"Selected least loaded worker: {least_loaded_worker} with {least_load}% load")
                return least_loaded_worker
            else:
                # All workers are busy, fall back to round-robin for fairness
                logger.debug(f"Least loaded worker ({least_loaded_worker}) has {least_load}% load, using round-robin fallback")
                return await self.round_robin_worker_selection()

        except Exception as e:
            logger.error(f"Error in least loaded fair selection: {e}")
            return await self.random_worker_selection()


    async def fastest_worker_selection(self):
        """DERECATED. Selects the worker with the fastest processing time."""
        if not self.available_workers:
            return None
        try:
            # Select worker with highest processing speed (faster workers have higher values)
            fastest_worker = max(
                self.available_workers,
                key=lambda w_id: self.worker_processing_speed.get(w_id, 1.0)
            )
            logger.debug(
                f"Selected fastest worker: {fastest_worker} with speed: {self.worker_processing_speed.get(fastest_worker, 1.0)}")
            return fastest_worker
        except Exception as e:
            logger.error(f"Error selecting fastest worker: {e}")
        return None



    async def fastest_worker_fair_selection(self):
        """
        Selects the fastest worker while ensuring fair distribution.

        Strategy:
        1. Filter out overloaded workers (>80% capacity)
        2. Among remaining workers, select the fastest
        3. If all workers are overloaded, fall back to least loaded

        This prevents monopolisation while still prioritising performance.
        """
        if not self.available_workers:
            return None

        try:
            # Get worker load information
            worker_loads = {}
            for worker_id in self.available_workers:
                load_pct = self.worker_info.get(worker_id, {}).get(
                    "capacity", {}).get("load_percentage", 0.0)
                worker_loads[worker_id] = load_pct
            
            logger.debug(f"Worker loads for fair selection: {worker_loads}")
            
            # Filter workers under fairness threshold (set at 50%)
            fairness_threshold = 50.0
            fair_workers = [w_id for w_id, load in worker_loads.items() 
                        if load < fairness_threshold]
            
            if fair_workers:
                fastest_fair_worker = max(
                    fair_workers,
                    key=lambda w_id: self.worker_processing_speed.get(w_id, 1.0)
                )
                logger.debug(f"Selected fastest fair worker: {fastest_fair_worker}")
                return fastest_fair_worker
            else:
                # Fallback to least loaded
                least_loaded_worker = min(
                    self.available_workers,
                    key=lambda w_id: worker_loads.get(w_id, 100.0)
                )
                logger.debug(f"All workers over threshold, using least loaded: {least_loaded_worker}")
                return least_loaded_worker

        except Exception as e:
            logger.error(f"Error in fastest worker fair selection: {e}")
            return await self.random_worker_selection()  # Fallback


    async def network_optimal_worker_selection(self):
        """
        DEPRECATED. 
        Selects the worker with the best normalised network score (bandwidth/latency).

        Additions:
        07.06.25 - Added logging of all workers' network scores for debugging.
        """
        if not self.available_workers:
            return None

        try:
            # Log all workers' network scores for debugging
            worker_scores = {}
            for worker_id in self.available_workers:
                score = self.worker_network_score.get(worker_id, 0.625)
                worker_scores[worker_id] = score
            logger.info(f"Network scores for selection: {worker_scores}")

            # Select worker with the highest network score
            best_network_worker = max(
                self.available_workers,
                key=lambda w_id: self.worker_network_score.get(w_id, 0.625)
            )

            best_score = self.worker_network_score.get(
                best_network_worker, 0.625)
            logger.info(
                f"Selected worker {best_network_worker} with network score: {best_score}")

            return best_network_worker
        except Exception as e:
            logger.error(
                f"Error selecting worker with best network score: {e}")
            return None


    async def network_optimal_fair_selection(self):
        """
        Selects the worker with the best network score while ensuring fair distribution.
        
        Strategy:
        1. Filter out overloaded workers (>50% capacity)
        2. Among remaining workers, select the one with best network score
        3. If all workers are overloaded, fall back to least loaded
        """
        if not self.available_workers:
            return None

        try:
            # Get worker load information
            worker_loads = {}
            worker_scores = {}
            for worker_id in self.available_workers:
                load_pct = self.worker_info.get(worker_id, {}).get(
                    "capacity", {}).get("load_percentage", 0.0)
                score = self.worker_network_score.get(worker_id, 0.625)
                worker_loads[worker_id] = load_pct
                worker_scores[worker_id] = score
            
            logger.debug(f"Worker loads for network fair selection: {worker_loads}")
            logger.debug(f"Worker network scores: {worker_scores}")
            
            # Filter workers under fairness threshold
            fairness_threshold = 50.0
            fair_workers = [w_id for w_id, load in worker_loads.items() 
                        if load < fairness_threshold]
            
            if fair_workers:
                # Select worker with best network score among fair workers
                best_network_worker = max(
                    fair_workers,
                    key=lambda w_id: worker_scores.get(w_id, 0.625)
                )
                best_score = worker_scores.get(best_network_worker, 0.625)
                logger.debug(f"Selected network optimal fair worker: {best_network_worker} with score: {best_score}")
                return best_network_worker
            else:
                # All workers over threshold, fall back to least loaded
                least_loaded_worker = min(
                    self.available_workers,
                    key=lambda w_id: worker_loads.get(w_id, 100.0)
                )
                logger.debug(f"All workers over threshold, using least loaded: {least_loaded_worker}")
                return least_loaded_worker

        except Exception as e:
            logger.error(f"Error in network optimal fair selection: {e}")
            return await self.random_worker_selection()  # Fallback

# -------------------------------------- MAIN FUNCTION -------------------------------------- #

"""
TaskWave Scheduler Launcher

This script starts the TaskWave scheduler with configurable job and worker selection algorithms.

Usage:
    python main.py [options]

Options:
    --job-algorithm ALG         Job selection algorithm to use (rr_job, edf_job, urgency_job)
    --worker-algorithm ALG      Worker selection algorithm to use (random_worker, rr_worker, 
                                least_loaded_worker, fastest_worker, network_optimal_worker)
    --log-level LEVEL           Logging level (INFO, DEBUG, etc.)
    --host HOST                 IP address to bind to
    --port PORT                 Port to listen on
"""

# Logger setup
logger = setup_logger("Scheduler", log_level=config.LOGGING_LEVEL)

# Algorithm selection
JOB_ALGORITHMS = [
    "rr_job",
    "edf_job",
    "urgency_job"
]

WORKER_ALGORITHMS = [
    "random_worker",
    "rr_worker",
    "least_loaded_fair",        
    "fastest_worker_fair",
    "network_optimal_fair"
]



async def main():
    """Main function to start the taskwave scheduler."""
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="TaskWave Scheduler")

    parser.add_argument(
        "--job-algorithm",
        type=str,
        choices=JOB_ALGORITHMS,
        default=config.JOB_SCHEDULING_ALGORITHM,
        help="Job selection algorithm to use"
    )

    parser.add_argument(
        "--worker-algorithm",
        type=str,
        choices=WORKER_ALGORITHMS,
        default=config.WORKER_SCHEDULING_ALGORITHM,
        help="Worker selection algorithm to use"
    )

    parser.add_argument(
        "--log-level",
        type=str,
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        default=logging.getLevelName(config.LOGGING_LEVEL),  # Convert numeric level to name
        help="Logging level"
    )

    parser.add_argument(
        "--host",
        type=str,
        default=config.SERVER_IP,
        help="IP address to bind to"
    )

    parser.add_argument(
        "--port",
        type=int,
        default=config.PORT,
        help="Port to listen on"
    )
    
    # Add cold start test argument
    parser.add_argument(
        "--cold-start-test",
        action="store_true",
        help="Enable cold start test mode with exaggerated parameters"
    )

    args = parser.parse_args()
    
    # Handle cold start test mode
    if args.cold_start_test:
        os.environ["TASKWAVE_COLD_START_TEST"] = "true"
        logger.info("Cold start test mode enabled via command line")

    # Apply command-line overrides to config
    if args.log_level:
        try:
            # Convert string log level to numeric value
            log_level = getattr(logging, args.log_level)
            logger.setLevel(log_level)
            config.LOGGING_LEVEL = log_level  # This is fine, it's intended to be modified

            # Also update the root logger to ensure all modules use this level
            logging.getLogger().setLevel(log_level)

            logger.info(f"Log level set to {args.log_level}")
        except (AttributeError, TypeError):
            logger.warning(f"Invalid log level: {args.log_level}, using default")

    if args.host:
        config.SERVER_IP = args.host

    if args.port:
        config.PORT = args.port

    # Update config with command-line args
    if args.job_algorithm:
        config.JOB_SCHEDULING_ALGORITHM = args.job_algorithm

    if args.worker_algorithm:
        config.WORKER_SCHEDULING_ALGORITHM = args.worker_algorithm

    # Create scheduler instance
    scheduler = Scheduler(
        job_algorithm=args.job_algorithm,
        worker_algorithm=args.worker_algorithm
    )

    try:
        logger.info("Scheduler starting...")
        await scheduler.start_server()
    except KeyboardInterrupt:
        logger.info("Scheduler interrupted by user. Shutting down...")
    except Exception as e:
        logger.error(f"Scheduler failed: {e}")
    finally:
        if scheduler.running:
            await scheduler.stop_server()
        logger.info("Scheduler shutdown complete.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Shutting down TaskWave scheduler...")
        sys.exit(0)
