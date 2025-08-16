"""
Meta TaskWave Job Generator Module

Simulates realistic FaaS workload patterns by generating job batches.
Models bursty serverless invocations with configurable arrival patterns.

Key features:
- Batch-based job generation (1-15 jobs per batch)
- Configurable inter-arrival times
- Random job properties (operations, loads, priorities)
- Phase-aware generation (warm-up vs measurement)
- Continuous job submission to scheduler
"""

import asyncio
import random
import time
import uuid
import logging
import os
from typing import Optional, Tuple

import config
from job import Job
from utils import setup_logger, setup_coordinated_logging, AsyncMessageHandler

# ------------------------------- LOGGING SETUP ----------------------------------- #
if not os.path.exists("../logs"):
    os.makedirs("../logs")

run_name = os.getenv("TASKWAVE_RUN_NAME", time.strftime("run_%Y-%m-%d_run_fallback"))
log_dir = setup_coordinated_logging(run_name)
logger = setup_logger("Job_Generator", log_level=logging.INFO, console_level=logging.INFO, log_dir=log_dir)

# ------------------------------- JOB GENERATOR ----------------------------------- #

class JobGenerator:
    """Generates jobs and sends them to the scheduler via asynchronous socket connection."""
    
    def __init__(self, server_ip: str, port: int, job_gen_id: Optional[str] = None):
        # Generator property
        self.job_gen_id = job_gen_id or f"jobgen_{str(uuid.uuid4())[:8]}"
        
        # Job properties
        self.job_id_counter: int = 0
        self.server_ip: str = server_ip
        self.port: int = port
        self.address: Tuple[str, int] = (self.server_ip, self.port)
        self.client_id: str = f"jobgen_{str(uuid.uuid4())[:8]}"
        
        # Connection properties
        self.message_handler: Optional[AsyncMessageHandler] = None
        self.reader: Optional[asyncio.StreamReader] = None
        self.writer: Optional[asyncio.StreamWriter] = None
        self.running: bool = False
        
# ------------------------------- CONNECTION HANDLING ----------------------------------- #

    async def connect_to_scheduler(self, retries: int = config.MAX_RETRIES, 
                                base_delay: float = config.BASE_DELAY, 
                                max_delay: float = config.MAX_DELAY):
        """
        Establishes connection to the scheduler with retries and exponential backoff.
        - type: syn message
        - response: ack message
        """
        
        attempt = 0
        while attempt < retries:
            try:
                logger.info(f"Connecting to scheduler at {self.server_ip}:{self.port}")
                self.reader, self.writer = await asyncio.open_connection(
                    self.server_ip, self.port)
                
                self.message_handler = AsyncMessageHandler(
                    self.reader, self.writer, self.address)
                
                # Send handshake message to identify as Job Generator
                handshake_message = {
                    "type": "SYN_JOB_GENERATOR",
                    "client_id": self.client_id
                }
                await self.message_handler.send_message(handshake_message)
                
                # Wait for acknowledgment from the scheduler
                response = await self.message_handler.receive_message()
                if response and response.get("status") == "ACK_JOB_GENERATOR":
                    logger.info(f"Connected to scheduler at {self.server_ip}:{self.port}.")
                    self.running = True
                    return True
                else:
                    logger.error("Failed to receive acknowledgment from scheduler.")
                    return False
            
            except Exception as e:
                logger.error(f"Connection attempt {attempt + 1} failed: {e}")
                attempt += 1
                
                # Exponential backoff
                delay = min(max_delay, base_delay * (2 ** attempt))
                logger.info(f"Retrying ({attempt}/{retries}) in {delay:.2f} seconds...")
                await asyncio.sleep(delay)
                
                if self.writer:
                    self.writer.close()
                    await self.writer.wait_closed()
                    self.writer = None
                    self.reader = None
        
        logger.error("Failed to connect to scheduler after maximum attempts.")
        return False

    async def generate_jobs(self):
        """Generate jobs using configuration settings."""
        if not self.message_handler:
            logger.error("Cannot generate jobs: not connected to scheduler")
            return
        
        try:
            # Check if cold start test mode is enabled
            cold_start_test = os.getenv("TASKWAVE_COLD_START_TEST", "false").lower() == "true"
            
            # Set batch delay based on mode
            min_batch_delay = config.MIN_BATCH_DELAY
            max_batch_delay = config.MAX_BATCH_DELAY
            
            # Use longer delays for cold start testing
            if cold_start_test:
                min_batch_delay = 5.0  # Longer delays to trigger cold starts
                max_batch_delay = 15.0
                logger.info(f"Cold start test mode: Using longer batch delays ({min_batch_delay}-{max_batch_delay}s)")
            
            while self.running:
                # Use config values for batch control
                num_jobs = random.randint(1, config.MAX_JOBS_PER_BATCH)
                delay = random.uniform(min_batch_delay, max_batch_delay)
                
                for _ in range(num_jobs):
                    self.job_id_counter += 1
                    job = Job(
                        job_id=self.job_id_counter,
                        operation=random.choice(config.AVAILABLE_OPERATIONS),
                        numbers=[random.randint(1, 10) for _ in range(2)],
                        job_load=random.randint(config.MIN_JOB_LOAD, config.MAX_JOB_LOAD),
                        priority=random.choices([0, 1], weights=[70, 30], k=1)[0],
                        deadline=time.time() + random.randint(10, 30)
                    )
                    
                    # Send job to scheduler
                    await self.message_handler.send_message(job.to_dict())
                    logger.info(f"Generated and sent job: {job}")
                
                logger.info(f"Generated {num_jobs} jobs. Next batch in {delay:.1f} seconds.")
                if cold_start_test:
                    logger.info("Cold start test mode: Long delay to trigger cold starts")
                
                await asyncio.sleep(delay)  # Wait before next batch of jobs
                
        except asyncio.CancelledError:
            logger.warning("Job generation task cancelled")
        except Exception as e:
            logger.error(f"Error during job generation: {e}")
        finally:
            await self.disconnect()
   
            
    async def disconnect(self):
        """Gracefully closes the connection to the scheduler."""
        self.running = False
        try:
            if self.message_handler:
                disconnect_msg = {"type": "DISCONNECT", "client_id": self.client_id}
                await self.message_handler.send_message(disconnect_msg)
                logger.info("Sent disconnect message to scheduler.")
        except Exception as e:
            logger.error(f"Error during disconnection: {e}")
        finally:
            if self.writer:
                self.writer.close()
                await self.writer.wait_closed()
            logger.info("Disconnected from scheduler.")
            
# ------------------------------- MAIN EXECUTION ----------------------------------- #

async def main():
    """Main function to run the Job Generator."""
    job_generator = JobGenerator(server_ip=config.SERVER_IP, port=config.PORT)
    
    try:
        if await job_generator.connect_to_scheduler():
            await job_generator.generate_jobs()
    except KeyboardInterrupt:
        logger.warning("Job Generator interrupted. Shutting down...")
    except Exception as e: 
        logger.error(f"Job Generator encountered an error: {e}")
    finally:
        if job_generator.running:
            await job_generator.disconnect()
            
            
if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.error(f"Shutting down Job Generator due to keyboard interrupt.")
