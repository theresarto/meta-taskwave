"""
Meta TaskWave Worker Module

Represents compute nodes that process jobs in the TaskWave system.
Simulates FaaS containers with capacity constraints and cold start behaviour.

Key features:
- Job processing with configurable capacity
- Cold start simulation (HOT/WARM/COLD states)
- Network performance variability
- Phase tracking (warm-up vs measurement)
- Asynchronous job execution and result reporting
"""

import asyncio
import random
import time
import uuid
import argparse
import os
import logging
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple

import config
from utils import setup_worker_logger, AsyncMessageHandler
from job import Job

# ------------------------------- WORKER CLASS ----------------------------------- #

class Worker:
    """
    Represents a Worker that:
    - connects to scheduler
    - receives jobs from scheduler
    - processes jobs
    - sends back result asynchronously
    
    Each worker has its own capacity limit and processing capability.
    Status is communicated to scheduler for optimal job distribution.
    
    Note:
    - Simulated latency -> total communication overhead including network RTT, connection establishment, message serialisation, incl. ping etc.
        - Factors:
            - Network conditions (RTT, packet loss) aka PING 1ms to 100ms usually
            - Connection establishment time (TCP handshake, SSL, auth, 10 to 50ms)
            - Worker processing time (serialisation, deserialisation, etc. 1ms to 10ms)
            - Processing speed (context switching, result prep, job queue 1 to 5ms)
            - System load delays (network congestion, CPU load, 0 to 100ms)
            - Cold start delays (if worker is idle, and needs to be warmed up if past config threshold). - feature added 19.06.25
            
    - Regarding cold start:
        - Base cold start penalty: 300ms (0.3 seconds)
        - After 60 seconds idle: Full 300ms penalty applies
    This relates to the typical colds start penalities found in commercial FaaS platforms:
        - AWS Lambda: 100-400ms for simple functions
        - Google Cloud Functions: 200-500ms
        - Azure Functions: 300-700ms
    """
    
    def __init__(self, worker_id: str, server_ip: str, port: int):
        # Worker properties
        self.worker_id: str = worker_id if worker_id else f"worker_{str(uuid.uuid4())[:8]}"
        self.server_ip: str = server_ip or config.SERVER_IP
        self.port: int = port or config.PORT
        self.address: Tuple[str, int] = (self.server_ip, self.port)
        
        # Worker settings
        self.capabilities: List[str] = config.AVAILABLE_OPERATIONS
        self.max_capacity: int = random.randint(config.MIN_WORKER_CAPACITY, 
                                                config.MAX_WORKER_CAPACITY)  # 1000 to 4000
        self.current_load: int = 0

        # Performance settings: Represents real world constraints, randomness representing uncertainty
        self.processing_speed: float = random.uniform(0.8, 1.2)  # Speed factor for processing time (1.0 = normal speed)
        
        # BASE network characteristics - these will be modified dynamically
        self.base_latency: float = random.uniform(0.05, 0.2)  # Base latency
        self.base_bandwidth: float = random.uniform(50, 200)  # Base bandwidth in MB/s
        
        # CURRENT network characteristics (these change dynamically)
        self.simulated_latency: float = self.base_latency
        self.bandwidth: float = self.base_bandwidth
        
        # Network variability settings
        self.last_network_update: float = time.time()
        self.network_update_interval: float = random.uniform(5.0, 15.0)  # Update every 5-15 seconds
        self.network_variance_factor: float = 0.3  # How much network can vary (30%)
        
        # Enhanced cold start tracking
        self.phase = "warm_up"  # Start in warm-up phase
        self.phase_change_timestamp = time.time()  # Track when phase started
        
        # Counters for warm-up phase
        self.warm_up_jobs_received = 0
        self.warm_up_jobs_completed = 0
        self.warm_up_cold_starts = 0
        
        # Counters for measurement phase
        self.measurement_jobs_received = 0
        self.measurement_jobs_completed = 0
        self.measurement_cold_starts = 0
        self.measurement_cold_start_penalty_sum = 0.0
        
        # Cold start parameters
        self.last_activity_time = time.time()
        self.cold_start_penalty_base = config.COLD_START_PENALTY_BASE
        self.warm_time_threshold = config.WARM_TIME_THRESHOLD if hasattr(config, 'WARM_TIME_THRESHOLD') else 60.0
        self.cold_start_events = []  # Track cold start events
        self.cold_time_threshold = config.COLD_TIME_THRESHOLD
        
        # Check if we should have cold start on first run
        self.cold_start_on_first_run = getattr(config, 'COLD_START_ON_FIRST_RUN', True)
                
        # Connection properties
        self.reader: Optional[asyncio.StreamReader] = None
        self.writer: Optional[asyncio.StreamWriter] = None
        self.message_handler: Optional[AsyncMessageHandler] = None
        self.running: bool = False
        self.connection_attempts: int = 0
        
        # Job tracking        
        self.active_jobs: Dict[int, Job] = {}
        self.completed_jobs: int = 0
        self.failed_jobs: int = 0
        self.total_processing_time = 0.0
        self.uptime_start = time.time()

        # Get the run name from environment variables for coordinated logging
        run_name = os.getenv("TASKWAVE_RUN_NAME", time.strftime("run_%Y-%m-%d_run_fallback"))
        
        # Use coordinated logging to get the right log directory
        log_dir = None
        if run_name:
            log_dir = Path("logs") / run_name
            if not log_dir.exists():
                log_dir.mkdir(parents=True, exist_ok=True)
        
        # Setup logger for this worker
        self.logger = setup_worker_logger(self.worker_id, log_dir)
        self.logger.info(f"Worker {self.worker_id} created (capacity: {self.max_capacity}, cold start base: {self.cold_start_penalty_base}ms)")
        if self.cold_start_on_first_run:
            self.logger.info(f"Cold start will be applied to first job (COLD_START_ON_FIRST_RUN=true)")
        else:
            self.logger.info(f"Cold start will only be applied after inactivity (COLD_START_ON_FIRST_RUN=false)")
   
    def update_network_performance(self):
        """
        Update network performance characteristics to simulate real-world network variability.
        This simulates:
        - Network congestion
        - Distance from servers
        - Infrastructure quality changes
        - Time-of-day effects
        """
        current_time = time.time()
        
        # Check if it's time to update network performance
        if current_time - self.last_network_update < self.network_update_interval:
            return
            
        # Update latency with some variability
        latency_multiplier = random.uniform(1 - self.network_variance_factor, 
                                          1 + self.network_variance_factor)
        self.simulated_latency = max(0.01, self.base_latency * latency_multiplier)
        
        # Update bandwidth with some variability (inverse relationship sometimes)
        bandwidth_multiplier = random.uniform(1 - self.network_variance_factor, 
                                            1 + self.network_variance_factor)
        self.bandwidth = max(10.0, self.base_bandwidth * bandwidth_multiplier)
        
        # Occasionally simulate network issues (5% chance)
        if random.random() < 0.05:
            self.simulated_latency *= random.uniform(2.0, 5.0)  # Significant latency spike
            self.bandwidth *= random.uniform(0.2, 0.5)  # Bandwidth degradation
            self.logger.debug(f"Network issue simulated for {self.worker_id}: "
                            f"latency={self.simulated_latency:.3f}s, bandwidth={self.bandwidth:.1f}MB/s")
        
        # Reset update timer with some randomness
        self.last_network_update = current_time
        self.network_update_interval = random.uniform(5.0, 15.0)
        
        self.logger.debug(f"Network updated for {self.worker_id}: "
                         f"latency={self.simulated_latency:.3f}s, bandwidth={self.bandwidth:.1f}MB/s, "
                         f"network_score={self.network_score:.2f}") 

    @property
    def status(self) -> str:
        """Get the current status of the worker."""
        if not self.running:
            return "offline"

        load_percentage = (self.current_load / self.max_capacity) * 100
        
        if load_percentage >= 90:
            return "overloaded"
        elif load_percentage >= 70:
            return "busy"
        elif load_percentage > 0:
            return "active"
        else:
            return "idle"
        
    @property
    def load_percentage(self) -> float:
        """Get the current load percentage of the worker."""
        return (self.current_load / self.max_capacity) * 100 if self.max_capacity > 0 else 0.0
    
    @property
    def uptime(self) -> float:
        """Get the uptime of the worker in seconds."""
        return time.time() - self.uptime_start if self.running else 0.0
    
    @property
    def avg_processing_time(self) -> float:
        """Calculate the average processing time per job."""
        if self.completed_jobs == 0:
            return 0.0
        return self.total_processing_time / max(1, self.completed_jobs)

    @property
    def available_capacity(self):
        """Return the available capacity of the worker."""
        return max(0, self.max_capacity - self.current_load)
    
    @property
    def network_score(self) -> float:
        """
        Calculate network performance score.
        Higher score = better network performance.
        
        Formula: (bandwidth / latency) normalised to 0-10 scale
        """ 
        # Raw score: bandwidth (MB/s) divided by latency (seconds)
        raw_score = self.bandwidth / self.simulated_latency
        
        # Normalise to 0-10 scale (assuming max reasonable score is bandwidth=200, latency=0.05)
        # Max theoretical: 200/0.05 = 4000, Min theoretical: 50/0.2 = 250
        max_theoretical_score = 4000
        normalised_score = min(10.0, (raw_score / max_theoretical_score) * 10)
        
        return round(normalised_score, 2)

    @property
    def worker_stats(self) -> Dict[str, Any]:
        """
        Get comprehensive worker statistics.
        
        Returns:
            Dictionary with worker statistics
        """
        # Calculate current warmth level on-demand
        current_time = time.time()
        time_since_last_activity = current_time - self.last_activity_time
        warmth_decay = time_since_last_activity / self.warm_time_threshold
        current_warmth_level = max(0.0, 1.0 - warmth_decay)
        
        # Enhanced cold start statistics
        avg_cold_start_penalty = 0.0
        if self.measurement_cold_starts > 0:
            avg_cold_start_penalty = self.measurement_cold_start_penalty_sum / self.measurement_cold_starts
        
        # Calculate cold start rate based on completed jobs in measurement phase
        cold_start_rate = 0.0
        if self.measurement_jobs_completed > 0:
            cold_start_rate = (self.measurement_cold_starts / self.measurement_jobs_completed) * 100
        
        
        return {
            "worker_id": self.worker_id,
            "status": self.status,
            "uptime": self.uptime,
            "capacity": {
                "max_capacity": self.max_capacity,
                "current_load": self.current_load,
                "available_capacity": self.available_capacity,
                "load_percentage": self.load_percentage
            },
            "performance": {
                "processing_speed": self.processing_speed,
                "latency": self.simulated_latency,
                "bandwidth": self.bandwidth,
                "network_score": self.network_score
            },
            "job_stats": {
                "jobs_completed": self.completed_jobs,
                "jobs_failed": self.failed_jobs,
                "active_jobs": len(self.active_jobs),
                "avg_processing_time": self.avg_processing_time
            },
            "cold_start": {
                "phase": self.phase,
                "phase_change_timestamp": self.phase_change_timestamp,
                "warm_up_jobs_received": self.warm_up_jobs_received,
                "warm_up_jobs_completed": self.warm_up_jobs_completed,
                "warm_up_cold_starts": self.warm_up_cold_starts,
                "measurement_jobs_received": self.measurement_jobs_received,
                "measurement_jobs_completed": self.measurement_jobs_completed,
                "measurement_cold_starts": self.measurement_cold_starts,
                "avg_cold_start_penalty_ms": avg_cold_start_penalty,
                "cold_start_rate_percent": cold_start_rate,
                "last_activity_time": self.last_activity_time,
                "warmth_level": current_warmth_level
            },
            "capabilities": self.capabilities
        }
    
    # ------------------------------- CONNECTION HANDLING ----------------------------------- #
    
    async def connect_to_scheduler(self, retries: int = config.MAX_RETRIES, 
                                   base_delay: float = config.BASE_DELAY,
                                   max_delay: float = config.MAX_DELAY):
        """
        Establishes connection to the scheduler with retries and exponential backoff.
        Similar to job-generator.
        Each worker = one connection to scheduler.
        - type: syn message
        - response: ack message
        """
        
        attempt = 0
        while attempt < retries:
            try:
                self.logger.info(f"Connecting to scheduler at {self.server_ip}:{self.port}")
                self.reader, self.writer = await asyncio.open_connection(
                    self.server_ip, self.port)
                
                self.message_handler = AsyncMessageHandler(self.reader, self.writer, self.address)
                
                # Prepare handshake with worker details
                handshake_message = {
                    "type": "SYN_WORKER",
                    "worker_id": self.worker_id,
                    "capabilities": self.capabilities,
                    "max_capacity": self.max_capacity,
                    "status": self.status,
                    "processing_speed": self.processing_speed,
                    "latency": self.simulated_latency,
                    "bandwidth": self.bandwidth,
                    "network_score": self.network_score
                }
                await self.message_handler.send_message(handshake_message)
                
                # Wait for acknowledgement from the scheduler
                response = await self.message_handler.receive_message()
                if response and response.get("status") == "ACK_WORKER":
                    self.logger.info(f"Worker {self.worker_id} connected and registered with Scheduler at {self.server_ip}:{self.port}")
                    self.running = True
                    return True
                else:
                    self.logger.error(f"Scheduler did not acknowledge Worker. Response: {response}")
                    raise ConnectionError("Handshake failed with Scheduler.")
                
            except Exception as e:
                self.logger.error(f"Connection attempt {attempt + 1} failed: {e}")
                attempt += 1  # Fixed syntax error
                
                # Exponential backoff
                delay = min(max_delay, base_delay * (2 ** attempt))
                self.logger.info(f"Retrying ({attempt}/{retries}) in {delay:.2f} seconds...")
                await asyncio.sleep(delay)
                
                if self.writer:
                    self.writer.close()
                    await self.writer.wait_closed()
                    self.writer = None
                    self.reader = None
        
        self.logger.error("Failed to connect to scheduler after maximum attempts.")
        return False
        
    async def send_status_update(self):
        """Send status update to scheduler."""
        if not self.message_handler:
            self.logger.error("Cannot send status update: not connected to scheduler")
            return
        

        status_message = {
            "type": "STATUS_UPDATE",
            **self.worker_stats
        }
                
        try:
            await self.message_handler.send_message(status_message)
            self.logger.debug(f"Sent status update: {self.status} ({self.load_percentage:.1f}% load)")
        except Exception as e:
            self.logger.error(f"Failed to send status update: {e}")
            
    
    # ------------------------- WARM UP AND COLD START HANDLING ------------------------- #
    
    def enter_measurement_phase(self):
        """
        Transition to measurement phase.
        This is where we start tracking cold starts and performance metrics.
        """
        if self.phase == "warm_up":  # Only transition if currently in warm-up phase
            self.phase = "measurement"
            self.phase_change_timestamp = time.time()
            self.last_activity_time = time.time()  # Reset activity timer
            
            self.logger.info(f"★★★ Worker {self.worker_id} entering measurement phase ★★★")
            self.logger.info(f"Warm-up phase stats:")
            self.logger.info(f"  Jobs received: {self.warm_up_jobs_received}")
            self.logger.info(f"  Jobs completed: {self.warm_up_jobs_completed}")
            self.logger.info(f"  Cold starts: {self.warm_up_cold_starts}")
            
            return True
        return False
        
    
    # ------------------------------- JOB PROCESSING ----------------------------------- #
    
    async def can_accept_job(self, job_data) -> bool:
        """
        Check if worker can accept job based on capacity and load
        """
        if isinstance(job_data, dict):
            job_load = job_data.get("job_load", 0)
            operation = job_data.get("operation", "")
            job_id = job_data.get("job_id", "unknown")
        else:
            job_load = getattr(job_data, "job_load", 0)
            operation = getattr(job_data, "operation", "")
            job_id = getattr(job_data, "job_id", "unknown")
        
        if self.current_load + job_load > self.max_capacity:
            self.logger.warning(f"Worker {self.worker_id} cannot accept job {job_id}: exceeds capacity. "
                                f"{self.current_load + job_load} > {self.max_capacity}")
            return False
        
        if operation not in self.capabilities:
            self.logger.warning(f"Worker {self.worker_id} cannot accept job {job_id}: operation '{operation}' not supported.")
            return False
        
        return True

    
    async def process_job(self, job_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process a job and RETURN the result.
        Implements a tiered (step-function) cold start model with HOT/WARM/COLD states and enhanced phase-aware cold start tracking.
        Returns:
            Dict with job result, including is_cold_start and warmth_state.
        """
        job = None
        processing_start_time = time.time()
        cold_start_penalty = 0.0
        is_cold_start = False
        warmth_state = "HOT"
        warmth_level = 1.0
        try:
            job = Job.from_dict(job_data) if isinstance(job_data, dict) else job_data

            # Phase-aware job count
            if self.phase == "warm_up":
                self.warm_up_jobs_received += 1
            else:
                self.measurement_jobs_received += 1

            # Determine cold start penalty and warmth state using step-function model
            current_time = time.time()
            time_since_last_activity = current_time - self.last_activity_time

            # In warm-up phase, always apply cold start to first job if enabled
            if self.phase == "warm_up" and self.warm_up_jobs_received == 1 and self.cold_start_on_first_run:
                cold_start_penalty, warmth_state, warmth_level = self.calculate_cold_start_penalty(
                    self.warm_time_threshold,
                    self.cold_time_threshold,  
                    self.cold_start_penalty_base,
                    9999 # Force cold start
                )
                is_cold_start = True
                self.logger.info(f"Worker {self.worker_id} applying first-run cold start penalty in warm-up phase")
            else:
                cold_start_penalty, warmth_state, warmth_level = self.calculate_cold_start_penalty(
                    self.warm_time_threshold,
                    self.cold_time_threshold,  
                    self.cold_start_penalty_base,
                    time_since_last_activity
                )
                is_cold_start = (warmth_state == "COLD")

            # Ensure cold_start_penalty is always a float in milliseconds
            cold_start_penalty = float(cold_start_penalty)

            if cold_start_penalty > 0:
                self.logger.info(
                    f"Worker {self.worker_id} cold start: {warmth_state} "
                    f"(idle: {time_since_last_activity:.1f}s, "
                    f"penalty: {cold_start_penalty:.1f}ms, "
                    f"phase: {self.phase}, "
                    f"is_cold_start: {is_cold_start})"
                )
                
                # Apply the cold start delay
                await asyncio.sleep(cold_start_penalty / 1000.0)
            else:
                self.logger.debug(f"Worker {self.worker_id} is {warmth_state} - no cold start penalty.")

            # Update last activity time
            self.last_activity_time = current_time

            # Simulate network latency
            await asyncio.sleep(self.simulated_latency)

            if not await self.can_accept_job(job):
                return {
                    "type": "JOB_RESULT",
                    "worker_id": self.worker_id,
                    "status": "rejected",
                    "job_id": job.job_id,
                    "result": "Job rejected - insufficient capacity or unsupported operation",
                    "processing_time": 0.0,
                    "timestamp": time.time(),
                    "is_cold_start": is_cold_start,
                    "warmth_state": warmth_state
                }
            job.status = "processing"
            job.worker_id = self.worker_id
            self.active_jobs[job.job_id] = job
            self.current_load += job.job_load
            self.logger.info(f"Processing job {job.job_id} on Worker {self.worker_id} with load {job.job_load}. "
                            f"Current load: {self.current_load}/{self.max_capacity}")

            await self.send_status_update()

            result = await self.execute_operation(job)
            self.logger.debug(f"Operation result for job {job.job_id}: {result}")

            # Simulate processing time
            base_time_per_unit = config.BASE_PROCESSING_TIME / 1000.0
            job_processing_time = base_time_per_unit * job.job_load * self.processing_speed
            await asyncio.sleep(job_processing_time)

            total_processing_time = time.time() - processing_start_time
            job.status = "completed"
            job.result = result

            if self.phase == "warm_up":
                self.warm_up_jobs_completed += 1
            else:
                self.measurement_jobs_completed += 1
            self.completed_jobs += 1
            self.total_processing_time += total_processing_time
            self.current_load -= job.job_load
            del self.active_jobs[job.job_id]

            if self.completed_jobs % 10 == 0:
                self.logger.info(f"Worker {self.worker_id} milestone - completed {self.completed_jobs} jobs")
                self.logger.info(f"Performance stats: {self.worker_stats}")

            self.logger.info(f"Job {job.job_id} completed on Worker {self.worker_id}.")

            result_message = {
                "type": "JOB_RESULT",
                "worker_id": self.worker_id,
                "status": "completed",
                "job_id": job.job_id,
                "operation": job.operation,  # ADD: for debugging and scheduler logging
                "numbers": job.numbers,      # ADD: for debugging and scheduler logging
                "result": result,
                "processing_time": total_processing_time,
                "queue_time": processing_start_time - job.creation_time,
                "cold_start_penalty": float(cold_start_penalty),  # ENSURE: always float
                "warmth_state": warmth_state,
                "warmth_level": warmth_level,
                "is_cold_start": is_cold_start,
                "phase": self.phase,
                "timestamp": time.time()
            }
            self.logger.debug(f"Sending result to scheduler - Job {job.job_id}: "
                 f"cold_start_penalty={cold_start_penalty}, "
                 f"warmth_state={warmth_state}, "
                 f"is_cold_start={is_cold_start}")
            
            self.logger.info(
                f"Job {job.job_id} completed successfully: {job.numbers} {job.operation} = {result} "
                f"(processed in {total_processing_time:.3f}s, "
                f"cold_start_penalty: {cold_start_penalty:.1f}ms, "
                f"warmth_state: {warmth_state}, "
                f"is_cold_start: {is_cold_start}, "
                f"phase: {self.phase})"
            )
            return result_message
        except Exception as e:
            self.logger.error(f"Failed with error processing job {job.job_id if job else 'unknown'}: {e}")
            self.failed_jobs += 1
            error_result = {
                "type": "JOB_RESULT",
                "worker_id": self.worker_id,
                "job_id": job.job_id if job else "unknown",
                "result": f"Processing error: {str(e)}",
                "status": "failed",
                "processing_time": time.time() - processing_start_time,
                "cold_start_penalty": float(cold_start_penalty),
                "warmth_state": warmth_state,
                "warmth_level": warmth_level,
                "is_cold_start": is_cold_start,
                "phase": self.phase,
                "timestamp": time.time()
            }
            return error_result
        finally:
            if job:
                self.current_load = max(0, self.current_load - job.job_load)
                if job.job_id in self.active_jobs:
                    del self.active_jobs[job.job_id]
                await self.send_status_update()

    def calculate_cold_start_penalty(self, warm_time_threshold: float, cold_time_threshold: float, 
                                penalty_base: float, idle_time: float) -> tuple:
        """
        Implements a step-function (tiered) cold start penalty model.
        Returns (penalty_ms, warmth_state, warmth_level)
        - HOT: idle <= warm_time_threshold: 0ms
        - WARM: warm_time_threshold < idle <= cold_time_threshold: 1/6 penalty
        - COLD: idle > cold_time_threshold: full penalty
        """
        # Defaults
        if idle_time <= warm_time_threshold:
            penalty = 0.0
            warmth_state = "HOT"
            warmth_level = 1.0
        elif warm_time_threshold < idle_time <= cold_time_threshold:
            penalty = penalty_base / 6.0  # Keep as 1/6 to match benchmark data
            warmth_state = "WARM"
            warmth_level = 0.5
        else:  # idle_time > cold_time_threshold
            penalty = penalty_base
            warmth_state = "COLD"
            warmth_level = 0.0
        return penalty, warmth_state, warmth_level
     
    async def execute_operation(self, job: Job) -> Any:
        """
        Executes operation defined in job
        
        Args:
        - job: Job instance containing operation and numbers
        
        Returns:
        - result of operation"""
        
        operation = job.operation
        numbers = job.numbers
        
        if operation == "add":
            return sum(numbers)
        elif operation == "subtract":
            return numbers[0] - numbers[1]
        elif operation == "multiply":
            return numbers[0] * numbers[1]
        elif operation == "divide":
            return numbers[0] / numbers[1]
        elif operation == "power":
            return numbers[0] ** numbers[1]
        else:
            return f"Unsupported operation: {operation}"
        
    # ------------------------------- MESSAGE HANDLING ----------------------------------- #
    
    async def receive_jobs(self):
        """
        Continuously receive jobs. (NOTE: This is only for INBOUND jobs)
        Jobs received from the scheduler call process_job(self) which then sends the result back to the scheduler.
        
        Note:
        - Process_jobs(self) contains the logic to process job and update status
        - Receive_jobs(self) is the overarching function that listens for jobs and sends back results from process_jobs
        """
        
        try:
            while self.running:
                if not self.message_handler:
                    self.logger.error("No message handler available. Cannot receive jobs.")
                    await asyncio.sleep(1)
                    continue
            
                message = await self.message_handler.receive_message()
            
                if not message:
                    await asyncio.sleep(0.1)  # Avoid busy waiting if no message received
                    continue
                
                if isinstance(message, dict):
                    message_type = message.get("type", "")
                    
                    if message_type == "DISCONNECT":  # Fixed logic error
                        self.logger.info("Received disconnect request from scheduler. Shutting down shortly.")
                        break
                    elif message_type == "PING":
                        await self.handle_ping(message)
                        continue
                    elif message_type == "STATUS_REQUEST":
                        await self.send_status_update()
                        continue
                    elif message_type == "ENTER_MEASUREMENT_PHASE":
                        self.logger.info(f"Received phase transition command from scheduler")
                        success = self.enter_measurement_phase()
                        
                        # Send acknowledgment
                        try:
                            ack_message = {
                                "type": "PHASE_TRANSITION_ACK",
                                "worker_id": self.worker_id,
                                "success": success,
                                "timestamp": time.time()
                            }
                            await self.message_handler.send_message(ack_message)
                            self.logger.info(f"Sent phase transition acknowledgment to scheduler")
                        except Exception as e:
                            self.logger.error(f"Failed to send phase transition acknowledgment: {e}")
                        continue    
                        
                    elif message_type == "JOB_ASSIGNMENT":  # process job begins here! We first check for job data
                        job_data = message.get("job", message)
                        if not job_data:
                            self.logger.error("Received JOB_ASSIGNMENT without job data.")
                            continue
                    else:
                        job_data = message
                else:
                    job_data = message
                
                # Clean logging - only show essential job info for assignment
                if isinstance(job_data, dict):
                    essential_info = {
                        "job_id": job_data.get("job_id"),
                        "operation": job_data.get("operation"),
                        "numbers": job_data.get("numbers"),
                        "job_load": job_data.get("job_load"),
                        "priority": job_data.get("priority"),
                        "deadline": job_data.get("deadline"),
                        "status": job_data.get("status"),
                        "worker_id": job_data.get("worker_id")
                    }
                    self.logger.info(f"Received job assignment: {essential_info}")
                else:
                    self.logger.info(f"Received job assignment: {job_data}")
                    
                result = await self.process_job(job_data)   # Calls process_job(self) here
                
                # NOTE: This sends the job result back to the scheduler
                try:
                    await self.message_handler.send_message(result)
                    self.logger.debug(f"Sent result for job {result.get('job_id', 'unknown')} from {self.worker_id}")
                except Exception as e:
                    self.logger.error(f"{self.worker_id} failed to send job result: {e}")    
                
                await asyncio.sleep(0.05)
                
        except asyncio.CancelledError:
            self.logger.info("Job receiving task cancelled")
        except Exception as e:
            self.logger.error(f"Error in job receiving loop: {e}")
        finally:
            await self.disconnect()
        
        
    async def handle_ping(self, message: Dict[str, Any]):
        """Heartbeat check from scheduler. Respond with PONG."""
        try:
            if not self.message_handler:
                self.logger.error("No message handler available. Cannot send PONG.")
                return
                
            pong_message = {
                "type": "PONG",
                "worker_id": self.worker_id,
                "status": self.status,
                "current_load": self.current_load,
                "active_jobs": len(self.active_jobs),
                "ping_timestamp": message.get("timestamp", time.time()),
                "pong_timestamp": time.time()
            }
            
            await self.message_handler.send_message(pong_message)
            self.logger.debug(f"Sent PONG response to Scheduler")
            
        except Exception as e:
            self.logger.error(f"{self.worker_id} failed to respond to PING: {e}")
        
    # ------------------------------- LIFECYCLE MANAGEMENT ----------------------------------- #

    async def disconnect(self):
        """
        Gracefully disconnect from the Scheduler.
        """
        self.running = False
        
        try:
            if self.message_handler:
                disconnect_message = {
                    "type": "DISCONNECT",
                    "worker_id": self.worker_id,
                    "reason": "Worker shutdown",
                    "final_stats": self.worker_stats,
                    "timestamp": time.time()
                }
                await self.message_handler.send_message(disconnect_message)
                self.logger.info("Sent disconnect message to Scheduler")
        except Exception as e:
            self.logger.error(f"Failed to send disconnect message: {e}")
        finally:
            if self.writer:
                self.writer.close()
                try:
                    await self.writer.wait_closed()
                except Exception:
                    pass
            self.logger.info(f"Worker {self.worker_id} disconnected")
            
# ------------------------------- HEARTBEAT TASK ----------------------------------- #
    
async def heartbeat_task(worker: Worker, interval: float = config.HEARTBEAT_INTERVAL):
    """
    Periodically send heartbeat messages to scheduler to ensure liveness.
    
    Args:
        worker: Worker instance to send heartbeat from
        interval: Interval in seconds to send heartbeat via CONFIG
    """
    
    try:
        while worker.running:
            worker.update_network_performance()
            await worker.send_status_update()
            await asyncio.sleep(interval)
    except asyncio.CancelledError:
        worker.logger.debug("Heartbeat task cancelled")
    except Exception as e:
        worker.logger.error(f"Error in heartbeat task: {e}")
        
# ------------------------------- MAIN FUNCTION ----------------------------------- #

async def main():
    """Main entry point for the worker."""
    
    parser = argparse.ArgumentParser(description="TaskWave Worker")
    parser.add_argument(
        "--worker_id",
        type=str,
        help="Specific worker ID (default: auto-generated if not provided)"
    )
    parser.add_argument(
        "--server_ip",
        type=str,
        default=config.SERVER_IP,
        help="IP address of the scheduler (default: from config)"
    )
    parser.add_argument(
        "--port",
        type=int,
        default=config.PORT,
        help="Port of the scheduler (default: from config)"
    )
    parser.add_argument(
        "--run_name",
        type=str,
        help="Run name for coordinated logging (default: from TASKWAVE_RUN_NAME env var)"
    )
    
    args = parser.parse_args()
    
    # If run_name is provided via command line, set it in the environment
    if args.run_name:
        os.environ["TASKWAVE_RUN_NAME"] = args.run_name
    
    # Create worker instance
    worker = Worker(
        worker_id=args.worker_id,
        server_ip=args.server_ip,
        port=args.port,
    )
    
    # Implementation
    try:
        if await worker.connect_to_scheduler():
            heartbeat_task_cor = asyncio.create_task(heartbeat_task(worker))
            
            await worker.receive_jobs()
            
            heartbeat_task_cor.cancel()
            
            try:
                await heartbeat_task_cor # Wait for heartbeat task to finish
                
            except asyncio.CancelledError:
                pass
            
        else:
            worker.logger.error("Failed to connect to scheduler, exiting...")
            
    except KeyboardInterrupt:
        worker.logger.warning("Worker interrupted by user. Shutting down...")
    except Exception as e:
        worker.logger.error(f"Worker failed to run: {e}")
    finally:
        if worker.running:
            await worker.disconnect()
        worker.logger.info("Worker shutdown complete.")
        
if __name__ == "__main__": 
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Worker interrupted by user. Shutting down...")
    except Exception as e:
        print(f"Worker encountered an error: {e}")
        