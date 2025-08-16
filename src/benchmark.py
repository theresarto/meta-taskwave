"""
Meta TaskWave Benchmark Orchestrator

This file acts as the main orchestrator for running benchmarks on the TaskWave system.

The benchmark is designed to evaluate the performance of TaskWave by running a series of predefined tasks across multiple configurations.
It will run each algorithm once, saving the data into a CSV file for later analysis; which then gets called in run_all_combinations() to iterate through all combinations of algorithms and configurations.

The core flow is as follows:
    run_all_combinations() 
    └── calls run_single_benchmark() 15×10 times
        ├── start_system_components()
        ├── monitor_benchmark_progress() 
        ├── stop_system_components()
        └── collect_run_metrics()
"""

import asyncio
import time
from datetime import datetime
import logging
import argparse
import sys
import os
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple
import pandas as pd
import config

import benchmark_utils
from utils import setup_coordinated_logging, update_run_summary_async, AsyncMessageHandler


# ------------------------------- LOGGING SETUP ----------------------------------- #

# batch_folder = setup_coordinated_logging()
logger = logging.getLogger("Benchmark_Orchestrator")


# ------------------------------- BENCHMARK ORCHESTRATOR CLASS ----------------------------------- #

# Global variable to store the current run name
run_name = ""

class BenchmarkOrchestrator:
    """
    Orchestrates benchmark runs across all algorithm combinations.
    Manages process lifecycle, monitors progress, and collects metrics.
    """

    def __init__(self, config_overrides: Optional[Dict[str, Any]] = None):
        """
        Initialise the benchmark orchestrator.

        Args:
            config_overrides: Optional configuration overrides
        """
        # Configuration
        self.config_overrides = config_overrides or {}

        # Results storage
        self.results: List[Dict[str, Any]] = []

        # Process tracking
        self.scheduler_process = None
        self.worker_processes: List = []
        self.job_generator_process = None

        # Benchmark parameters
        self.benchmark_timeout = config.BENCHMARK_TIMEOUT
        self.stabilisation_wait = config.STABILISATION_WAIT
        self.cleanup_wait = config.CLEANUP_WAIT
        # Algorithm combinations (Call helper function)
        # Helper function
        self.algorithm_combinations: List[Tuple[str, str]
                                          ] = self._get_all_algorithm_combinations()

        # Immediate log on initialisation
        logger.info(
            f"Benchmark orchestrator initialised with {len(self.algorithm_combinations)} algorithm combinations")

    # ------------------------------- HELPER FUNCITONS ------------------------------- #

    def _get_all_algorithm_combinations(self) -> List[Tuple[str, str]]:
        """
        Return list of all job + worker algorithm combinations.

        Returns:
            List of (job_algorithm, worker_algorithm) tuples
        """

        # Algorithm selection
        job_algorithms = [
            "rr_job",
            "edf_job",
            "urgency_job"
        ]

        worker_algorithms = [
            "random_worker",              # Random baseline
            "rr_worker",                  # Round-robin baseline  
            "least_loaded_fair",          # Fair version of least loaded
            "fastest_worker_fair",        # Fair version of fastest
            "network_optimal_fair"        # Fair version of network optimal
        ]

        combinations = []
        for job_algo in job_algorithms:
            for worker_algo in worker_algorithms:
                combinations.append((job_algo, worker_algo))

        # Log the algorithm configuration
        logger.info(f"Configured benchmark with {len(job_algorithms)} job algorithms and {len(worker_algorithms)} worker algorithms")
        logger.info(f"Total combinations: {len(combinations)}")
        logger.info(f"Job algorithms: {job_algorithms}")
        logger.info(f"Worker algorithms: {worker_algorithms}")

        return combinations

    def _validate_benchmark_config(self, duration: int, job_limit: int, worker_count: int) -> None:
        """
        Checks if benchmark parameters are reasonable.
        This sets the minimum and maximum amount to enter for benchmarking to avoid inconsistency or invalid runs.

        Args:
            duration: Benchmark duration in seconds
            job_limit: Maximum number of jobs to process
            worker_count: Number of worker processes

        Raises:
            ValueError: If parameters are invalid
        """

        # Duration
        if duration < 30 or duration > 600:
            raise ValueError(
                "Duration must be between 30 seconds (0.5 mins) to 600 seconds (10 mins)")

        # Job Limit
        if job_limit < 50 or job_limit > 10000:
            raise ValueError(
                "Number of jobs to test must be between 50 to 1000")

        # Worker Count
        if worker_count < 2:
            raise ValueError(
                "Worker count must be at least 2 to test multiple node distributed system")

        if worker_count > 10:
            raise ValueError(
                "Worker count must be less than or equal to 10 to avoid excessive resource usage")

        logger.debug(
            f"Benchmark config validated: {duration=}s, {job_limit=}, {worker_count=}")

    # ------------------------------- CORE BENCHMARK RUN FUNCTIONS ------------------------------- #

    def export_individual_result(self, result: Dict[str, Any], run_name: str) -> str:
        """
        Export an individual benchmark result to a CSV file with standardized time units.
        
        Args:
            result: Benchmark result dictionary
            run_name: Name of the run
            
        Returns:
            Path to the exported file
        """
        try:
            # Convert single result to DataFrame
            result_df = pd.DataFrame([result])
            
            # Create path to logs directory for this run
            log_dir = Path("logs") / run_name
            if not log_dir.exists():
                log_dir.mkdir(parents=True, exist_ok=True)
            
            # Create CSV file path
            csv_filename = f"{run_name}_result.csv"
            csv_path = log_dir / csv_filename
            
            # Export to CSV
            result_df.to_csv(csv_path, index=False)
            
            # Create a summary CSV with key metrics - streamlined to avoid duplication
            summary_data = {
                "job_algorithm": [result.get("job_algorithm")],
                "worker_algorithm": [result.get("worker_algorithm")],
                "jobs_completed": [result.get("jobs_completed", 0)],
                "throughput_jobs_per_second": [result.get("throughput_jobs_per_second", 0.0)],
                "avg_worker_utilisation_percent": [result.get("avg_worker_utilisation_percent", 0.0)],
                "avg_queue_time_ms": [result.get("avg_queue_time_ms", 0.0)],
                "avg_processing_time_ms": [result.get("avg_processing_time_ms", 0.0)],
                "job_success_rate": [result.get("job_success_rate", 0.0)],
                
                # Cold start metrics
                "cold_start_rate_percent": [result.get("cold_start_rate_percent", 0.0)],
                "avg_cold_start_penalty_ms": [result.get("avg_cold_start_penalty_ms", 0.0)],
                "total_cold_starts": [result.get("total_cold_starts", 0)],
                
                # Phase information
                "warm_up_jobs_completed": [result.get("warm_up_jobs_completed", 0)],
                "measurement_jobs_completed": [result.get("measurement_jobs_completed", 0)],
                "measurement_cold_starts": [result.get("measurement_cold_starts", 0)],
                
                "benchmark_success": [result.get("benchmark_success", False)]
            }
            
            summary_df = pd.DataFrame(summary_data)
            summary_path = log_dir / f"{run_name}_summary.csv"
            summary_df.to_csv(summary_path, index=False)
            
            logger.info(f"Individual result exported to: {csv_path}")
            logger.info(f"Individual summary exported to: {summary_path}")
            
            return str(csv_path)
            
        except Exception as e:
            logger.error(f"Error exporting individual result for {run_name}: {e}")
            return ""
    
    
    async def run_single_benchmark(self, job_algo: str, worker_algo: str, benchmark_run_name: str, 
                              duration: int, job_limit: int, worker_count: int) -> Dict[str, Any]:
    
        """
        Run a single benchmark with the specified algorithms and parameters.

        Args:
            job_algo: Job scheduling algorithm
            worker_algo: Worker scheduling algorithm
            benchmark_run_name: Unique name for this benchmark run
            duration: Duration of the benchmark in seconds
            job_limit: Maximum number of jobs to process
            worker_count: Number of worker processes

        Returns:
            Dictionary containing run metrics
        """
        
        # Set global run_name for other components to access
        global run_name
        run_name = benchmark_run_name  # Assign to global variable
        
        # Setup logging in the new folder
        batch_folder = setup_coordinated_logging(run_name)
        self.logger = logging.getLogger("Benchmark_Orchestrator")
        
        logger.info(f"Starting benchmark run: {run_name}")
        logger.info(f"Algorithm combination: {job_algo} + {worker_algo}")
        logger.info(
            f"Parameters: {duration}s duration, {job_limit} jobs max, {worker_count} workers")

        system_start_time = time.time()

        try:
            # Setup logging for this run
            await self.setup_logging_for_run(run_name, batch_folder)

            # Start system components
            await self.start_system_components(job_algo, worker_algo, worker_count, run_name)

            # Monitor benchmark progress (includes warm-up and measurement phases)
            actual_duration, jobs_completed = await self.monitor_benchmark_progress(job_limit, duration)

            # Stop system components with cool-down period
            await self.stop_system_components()

            # Extract iteration number from run_name if you want it dynamic
            iteration_num = 1
            try:
                parts = benchmark_run_name.split('_')
                if len(parts) >= 6 and 'iter' in parts[5]:
                    iteration_num = int(parts[5].replace('iter', ''))
            except:
                iteration_num = 1
    
            # Collect metrics from logs
            metrics = await self.collect_run_metrics(
                run_name=benchmark_run_name,
                job_algorithm=job_algo,           
                worker_algorithm=worker_algo,     
                iteration=iteration_num,                      
                benchmark_mode=os.getenv("TASKWAVE_BENCHMARK_MODE", "default")    
            )

            # Extract phase-specific metrics
            warm_up_duration = metrics.get("warm_up_duration", 0)
            measurement_duration = metrics.get("measurement_duration", 0)
            
            # Create result record with enhanced phase information
            result = {
                "run_name": run_name,
                "job_algorithm": job_algo,
                "worker_algorithm": worker_algo,
                "total_duration_seconds": actual_duration,
                "total_jobs_completed": jobs_completed,
                "worker_count": worker_count,
                "benchmark_success": True,
                "error_message": None,
                
                # Phase-specific metrics
                "warm_up_phase": {
                    "duration_seconds": warm_up_duration,
                    "jobs_completed": metrics.get("warm_up_jobs_completed", 0)
                },
                "measurement_phase": {
                    "duration_seconds": measurement_duration,
                    "jobs_completed": metrics.get("measurement_jobs_completed", 0),
                    "cold_starts": metrics.get("measurement_cold_starts", 0),
                    "cold_start_rate_percent": metrics.get("cold_start_rate_percent", 0.0)
                },
                
                # Include all other metrics
                **metrics
            }

            elapsed_time = time.time() - system_start_time
            logger.info(
                f"Benchmark run completed successfully: {run_name} (took {elapsed_time:.1f}s)")
            logger.info(f"Phase summary: {warm_up_duration:.1f}s warm-up, {measurement_duration:.1f}s measurement")
            logger.info(f"Job completion: {result['warm_up_phase']['jobs_completed']} in warm-up, "
                        f"{result['measurement_phase']['jobs_completed']} in measurement")
            
            if result['measurement_phase']['jobs_completed'] > 0:
                logger.info(f"Cold starts: {result['measurement_phase']['cold_starts']} "
                            f"({result['measurement_phase']['cold_start_rate_percent']:.2f}% of measurement jobs)")
            
            # Export individual result to CSV
            self.export_individual_result(result, run_name)

            return result

        except Exception as e:
            elapsed_time = time.time() - system_start_time
            logger.error(f"Benchmark run failed: {run_name} - {e}")

            # Ensure cleanup even on failure
            try:
                await self.stop_system_components()
            except Exception as cleanup_error:
                logger.error(f"Error during cleanup: {cleanup_error}")

            # Return error result
            error_result = {
                "run_name": run_name,
                "job_algorithm": job_algo,
                "worker_algorithm": worker_algo,
                "duration_seconds": elapsed_time,
                "jobs_completed": 0,
                "worker_count": worker_count,
                "benchmark_success": False,
                "error_message": str(e),
                # Set error values for metrics
                "jobs_completed": 0,
                "throughput_jobs_per_second": 0.0,
                "avg_worker_utilisation_percent": 0.0,
                "avg_queue_time_ms": 0.0,
                "avg_processing_time_ms": 0.0,
                "job_success_rate": 0.0,
                "avg_recovery_time_ms": 0.0
            }
            
            # Export failed result as well
            self.export_individual_result(error_result, run_name)
            
            return error_result

    async def run_all_combinations(self, iterations: int = 10, duration: int = 180,
                                job_limit: int = 200, worker_count: int = 4,
                                start_iteration: int = 1) -> pd.DataFrame:
        """
        Run benchmarks for all algorithm combinations with multiple iterations.

        Args:
            iterations: Number of iterations per algorithm combination
            duration: Maximum duration per run in seconds
            job_limit: Maximum jobs per run
            worker_count: Number of worker processes

        Returns:
            DataFrame containing all benchmark results
        """
        global run_name
        
        logger.info(
            "[Important] Running comprehensive benchmark of all algorithm combinations")
        logger.info(
            f"Configuration: {iterations} iterations, {duration}s max duration, {job_limit} job limit, {worker_count} workers")

        # Validate benchmark configuration
        self._validate_benchmark_config(duration, job_limit, worker_count)

        # Calculate total runs
        total_runs = len(self.algorithm_combinations) * iterations
        current_run = 0
        successful_runs = 0
        failed_runs = 0

        logger.info(f"Total benchmark runs to execute: {total_runs}")

        overall_system_start_time = time.time()
        date_str = time.strftime("%Y-%m-%d_%H-%M")
        
        # Create a progress tracking CSV file
        progress_dir = Path("results") / f"benchmark_{date_str}"
        progress_dir.mkdir(parents=True, exist_ok=True)
        progress_path = progress_dir / "benchmark_progress.csv"
        
        progress_data = {
            "run_number": [],
            "job_algorithm": [],
            "worker_algorithm": [],
            "iteration": [],
            "status": [],
            "completion_time": [],
            "run_name": []
        }
        progress_df = pd.DataFrame(progress_data)
        progress_df.to_csv(progress_path, index=False)
        
        logger.info(f"Progress tracking file created at: {progress_path}")

        start_iter = start_iteration
        # Main execution loop for all combinations
        for job_algo, worker_algo in self.algorithm_combinations:
            for iteration in range(iterations):
                current_run += 1
                iteration_num = iteration + start_iter
                
                # Set the global run_name for this iteration
                benchmark_run_name = f"run_{date_str}_{job_algo}_{worker_algo}_iter{iteration_num:02d}"
                
                # Update progress tracking file - Starting run
                new_row = {
                    "run_number": current_run,
                    "job_algorithm": job_algo,
                    "worker_algorithm": worker_algo,
                    "iteration": iteration_num,
                    "status": "starting",
                    "completion_time": "",
                    "run_name": benchmark_run_name
                }
                progress_df = pd.concat([progress_df, pd.DataFrame([new_row])], ignore_index=True)
                progress_df.to_csv(progress_path, index=False)

                logger.info("=" * 80)
                logger.info(
                    f"Progress: Run {current_run}/{total_runs} --> {benchmark_run_name} ({job_algo} + {worker_algo})")
                logger.info("=" * 80)

                run_start_time = time.time()
                try:
                    result = await self.run_single_benchmark(
                        job_algo,
                        worker_algo,
                        benchmark_run_name,  # Use the local variable
                        duration,
                        job_limit,
                        worker_count
                    )

                    self.results.append(result)
                    
                    run_end_time = time.time()
                    run_duration = run_end_time - run_start_time

                    if result["benchmark_success"]:
                        successful_runs += 1
                        status = "completed"
                        logger.info(f"✅ Run {current_run}/{total_runs} completed successfully in {run_duration:.1f}s")
                    else:
                        failed_runs += 1
                        status = "failed"
                        logger.info(f"❌ Run {current_run}/{total_runs} failed in {run_duration:.1f}s: {result.get('error_message', 'Unknown error')}")

                except Exception as e:
                    run_end_time = time.time()
                    run_duration = run_end_time - run_start_time
                    
                    logger.error(
                        f"❌ Unexpected error during run {benchmark_run_name}: {e}")
                    failed_runs += 1
                    status = "error"

                    # Create error result
                    error_result = {
                        "run_name": benchmark_run_name,
                        "job_algorithm": job_algo,
                        "worker_algorithm": worker_algo,
                        "benchmark_success": False,
                        "error_message": f"Unexpected error: {str(e)}",
                    }
                    self.results.append(error_result)
                    
                    # Export error result
                    self.export_individual_result(error_result, benchmark_run_name)
                    
                # Update progress tracking file - Completed run
                for i, row in progress_df.iterrows():
                    if row["run_name"] == benchmark_run_name:
                        progress_df.at[i, "status"] = status
                        progress_df.at[i, "completion_time"] = time.strftime("%Y-%m-%d %H:%M:%S")
                        break
                        
                progress_df.to_csv(progress_path, index=False)
                
                # Calculate and display estimated time remaining
                elapsed_time = time.time() - overall_system_start_time
                avg_run_time = elapsed_time / current_run
                remaining_runs = total_runs - current_run
                estimated_time_remaining = avg_run_time * remaining_runs
                
                if remaining_runs > 0:
                    logger.info(f"⏱️ Progress: {current_run}/{total_runs} runs completed ({(current_run/total_runs)*100:.1f}%)")
                    logger.info(f"⏱️ Estimated time remaining: {estimated_time_remaining/60:.1f} minutes")
                    logger.info(f"⏱️ Estimated completion time: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time() + estimated_time_remaining))}")
                
                # Slow down between runs
                await asyncio.sleep(2)

        # Calculate total time
        total_elapsed_time = time.time() - overall_system_start_time

        # Log completion statistics
        logger.info("=" * 80)
        logger.info("BENCHMARK COMPLETION SUMMARY")
        logger.info("=" * 80)
        logger.info(f"Total runs executed: {total_runs}")
        logger.info(f"Successful runs: {successful_runs}")
        logger.info(f"Failed runs: {failed_runs}")
        logger.info(f"Success rate: {(successful_runs/total_runs)*100:.1f}%")
        logger.info(f"Total time elapsed: {total_elapsed_time/60:.1f} minutes")
        logger.info(
            f"Average time per run: {total_elapsed_time/total_runs:.1f} seconds")
        logger.info("=" * 80)

        # Export results
        results_df = pd.DataFrame(self.results)
        export_path = self.export_results(results_df)
        logger.info(f"Results exported to: {export_path}")

        # Consolidate job-level data for ML
        logger.info("=" * 80)
        logger.info("ML DATASET CONSOLIDATION")
        logger.info("=" * 80)
        
        try:
            results_dir = Path(export_path).parent  # Same folder as benchmark results
            ml_dataset_path = benchmark_utils.consolidate_job_data_for_ml(results_dir, self.results)
            
            if ml_dataset_path:
                logger.info(f"🤖 ML-ready dataset created: {ml_dataset_path}")
                logger.info(f"🎯 Ready for machine learning model training!")
            else:
                logger.warning("⚠️ Could not create ML dataset - no job-level data found")
                
        except Exception as e:
            logger.error(f"❌ Error during ML dataset consolidation: {e}")
        
        logger.info("=" * 80)
        
        return results_df

    # ------------------------------- PROCESS MANAGEMENT ----------------------------------- #

    async def start_system_components(self, job_algo: str, worker_algo: str, worker_count: int, run_name: str) -> None:
        """
        Starts the scheduler, worker, and job generator using the job_algo x worker_algo combination.

        Args:
            job_algo: Job scheduling algorithm
            worker_algo: Worker scheduling algorithm
            worker_count: Number of worker processes to start
            run_name: Unique name for this benchmark run

        Returns:
            None
        """

        logger.debug(
            f"Starting system components: {job_algo=} x {worker_algo=} with {worker_count} workers")

        # Prepare environment variables
        env_vars = {
            "TASKWAVE_JOB_ALGORITHM": job_algo,
            "TASKWAVE_WORKER_ALGORITHM": worker_algo,
            "TASKWAVE_ENV": "benchmark",
            "TASKWAVE_SERVER_IP": config.SERVER_IP,
            "TASKWAVE_PORT": str(config.PORT),
            "TASKWAVE_RUN_NAME": run_name, # Make sure the run name is passed
        }
        
        # Add cold start test mode if enabled via environment
        if os.getenv("TASKWAVE_COLD_START_TEST", "false").lower() == "true":
            env_vars["TASKWAVE_COLD_START_TEST"] = "true"
            logger.info("Cold start test mode is enabled")

        try:
            # Start the scheduler
            scheduler_cmd = [
                "python", "scheduler.py",
                "--job-algorithm", job_algo,
                "--worker-algorithm", worker_algo,
                "--log-level", "INFO"
            ]

            self.scheduler_process = benchmark_utils.start_process(
                scheduler_cmd, env_vars)
            if not self.scheduler_process:
                raise RuntimeError("Failed to start scheduler process")

            await asyncio.sleep(3)  # Allow time for scheduler to initialise
            logger.info(
                f"Scheduler started with PID: {self.scheduler_process.pid}")

            # Check for remote or local worker setup
            remote_workers = os.getenv(
                "TASKWAVE_REMOTE_WORKERS", "false").lower() == "true"

            if remote_workers:
                # Remote workers are expected. Do not start local workers.
                logger.info(
                    f"Remote workers enabled. Expecting {worker_count} workers to connect from remote machines.")
                logger.info("Please start workers on remote machine now.")

                # Countdown timer for remote worker connection
                connection_wait_time = self.stabilisation_wait * 2  # Double the wait time
                logger.info(
                    f"Waiting {connection_wait_time} seconds for remote workers to connect...")

                for remaining in range(connection_wait_time, 0, -1):
                    if remaining % 5 == 0 or remaining <= 10:  # Show every 5 seconds, then every second for last 10
                        logger.info(
                            f"{remaining} seconds remaining for worker connections...")
                    if remaining < 5:
                        print(f"{remaining}... ", end="", flush=True)
                    await asyncio.sleep(1)

                print()
                logger.info("Remote worker connection window completed")

            else:
                # Local workers mode - start workers on this machine
                for i in range(worker_count):
                    worker_id = f"WORKER_{i+1}"
                    worker_cmd = [
                        "python", "worker.py",
                        "--worker_id", worker_id,
                        "--run_name", run_name  # Pass the run name to the worker
                    ]
                    worker_process = benchmark_utils.start_process(
                        worker_cmd, env_vars)
                    if not worker_process:
                        raise RuntimeError(
                            f"Failed to start worker {worker_id}")
                    self.worker_processes.append(worker_process)

                # Wait for workers to connect
                await asyncio.sleep(self.stabilisation_wait)

            # Start job generator
            job_gen_cmd = ["python", "job-generator.py"]
            self.job_generator_process = benchmark_utils.start_process(
                job_gen_cmd, env_vars)
            if not self.job_generator_process:
                raise RuntimeError("Failed to start job generator process")

            # Wait for job generation to begin
            await asyncio.sleep(2)

            logger.info(f"All system components started successfully")

        except Exception as e:
            logger.error(f"Error starting system components: {e}")
            # Clean up any started processes
            await self.stop_system_components()
            raise

    async def stop_system_components(self) -> None:
        """
        Stop all system components gracefully.
        """
        logger.debug("Stopping system components")

        try:
            # Stop job generator first to prevent new jobs
            if self.job_generator_process:
                benchmark_utils.stop_process(self.job_generator_process)
                self.job_generator_process = None
                logger.debug("Job generator stopped")

            # Wait for pending jobs to complete
            await asyncio.sleep(5)

            # Stop workers
            for worker_process in self.worker_processes:
                if worker_process:
                    benchmark_utils.stop_process(worker_process)
            self.worker_processes.clear()
            logger.debug("All workers stopped")

            # Stop scheduler last
            if self.scheduler_process:
                benchmark_utils.stop_process(self.scheduler_process)
                self.scheduler_process = None
                logger.debug("Scheduler stopped")

            # Additional wait for cleanup
            await asyncio.sleep(2)

        except Exception as e:
            logger.error(f"Error stopping system components: {e}")

    async def monitor_benchmark_progress(self, job_limit: int = config.BENCHMARK_JOB_LIMIT, safety_timeout: float = config.SAFETY_TIMEOUT) -> Tuple[float, int]:
        """
        Monitor benchmark progress and stop when criteria is met.
        Includes a warm-up period before starting measurement.

        Args:
            job_limit: Target number of jobs to complete (primary criterion)
            safety_timeout: Maximum duration before timing out (safety net)

        Returns:
            Tuple containing elapsed time and number of jobs processed
        """

        system_start_time = time.time()
        warm_up_period = getattr(config, 'WARM_UP_PERIOD', 30)  # seconds
        warm_up_end_time = system_start_time + warm_up_period

        logger.info(
            f"Phase 1: Starting benchmark with {warm_up_period}s warm-up period")

        # Warm up phase
        logger.info("Warm up phase started... allowing system to stabilise")

        while time.time() < warm_up_end_time:
            await asyncio.sleep(1)
            elapsed_time = time.time() - system_start_time
            if elapsed_time % 5 == 0:
                logger.debug(
                    f"Warm up progress: {elapsed_time:.1f}s elapsed / {warm_up_period}s total")

        logger.info(
            "Phase 1: Warm up phase completed. Signaling scheduler to start measurement phase...\n")\
        
        # Record exact transition timestamp
        transition_timestamp = time.time()
        
        # Signal to scheduler to start measurement phase
        transition_success = False
        max_attempts = 3
        for attempt in range(1, max_attempts + 1):
            try:
                # Create a temporary connection to scheduler
                reader, writer = await asyncio.open_connection(config.SERVER_IP, config.PORT)
                message_handler = AsyncMessageHandler(reader, writer)
                
                transition_message = {
                    "type": "START_MEASUREMENT_PHASE",
                    "timestamp": transition_timestamp,  # Send the exact transition timestamp
                    "run_name": run_name  # Include run name for logging correlation
                }
                
                await message_handler.send_message(transition_message)
                
                # Wait for acknowledgment with timeout
                try:
                    response = await asyncio.wait_for(message_handler.receive_message(), timeout=5.0)
                    if response and response.get("status") == "MEASUREMENT_PHASE_STARTED":
                        logger.info("✅ Scheduler acknowledged measurement phase transition")
                        transition_success = True
                        break
                    else:
                        logger.warning(f"⚠️ Unexpected response from scheduler: {response}")
                except asyncio.TimeoutError:
                    logger.warning("⚠️ Timeout waiting for scheduler acknowledgment")
                
                # Close the temporary connection
                writer.close()
                await writer.wait_closed()
                
                if not transition_success and attempt < max_attempts:
                    logger.info(f"Retrying measurement phase transition (attempt {attempt+1}/{max_attempts})...")
                    await asyncio.sleep(1)
                    
            except Exception as e:
                logger.error(f"Error signaling measurement phase transition (attempt {attempt}/{max_attempts}): {e}")
                if attempt < max_attempts:
                    await asyncio.sleep(1)
                
        if not transition_success:
            logger.warning("⚠️ Could not signal measurement phase transition to scheduler after multiple attempts")
            logger.warning("⚠️ Continuing with benchmark, but phase metrics may be inaccurate")

        # Allow a short delay for scheduler to notify workers
        await asyncio.sleep(2)
        
        # Record the measurement phase start time (for calculating metrics later)
        measurement_start_time = transition_timestamp    
        logger.info(f"Phase 2: Starting measurement phase at {datetime.fromtimestamp(measurement_start_time).strftime('%H:%M:%S.%f')[:-3]}...")

        # Use measurement phase job count logic
        log_dir = Path("logs") / run_name
        measurement_job_count_start = benchmark_utils.get_measurement_phase_job_count(log_dir)
        stall_timeout = config.STALL_TIMEOUT
        last_progress_time = measurement_start_time

        logger.debug(
            f"Phase 2 :Starting measurement with initial measurement phase job count: {measurement_job_count_start}")
        logger.debug(
            f"Monitoring benchmark progress: max {safety_timeout}s or {job_limit} jobs\n")

        while True:
            await asyncio.sleep(1)

            current_time = time.time()
            elapsed_measurement_time = current_time - measurement_start_time
            total_elapsed_time = current_time - system_start_time

            # Get current measurement phase job completion count
            measurement_job_count_current = benchmark_utils.get_measurement_phase_job_count(log_dir)
            measured_job_count = measurement_job_count_current - measurement_job_count_start

            # Ensure job generator is stopped once limit is reached
            if measured_job_count >= job_limit:
                logger.warning(
                    f"Job limit reached: {measured_job_count} jobs in {elapsed_measurement_time:.1f}s (measurement phase)")
                break

            # Safety criterion: Don't run forever
            if total_elapsed_time >= safety_timeout:
                logger.warning(
                    f"Safety timeout of {safety_timeout}s reached with only {measured_job_count} jobs completed during measurement")
                logger.warning(
                    "This algorithm combination may be too slow for practical use")
                break

            # Progress tracking
            if measurement_job_count_current > measurement_job_count_start:
                measurement_job_count_start = measurement_job_count_current
                last_progress_time = current_time
                logger.debug(
                    f"Progress: {measured_job_count} jobs completed in {elapsed_measurement_time:.1f}s (measurement phase)")
            else:
                # Check for stalls
                if current_time - last_progress_time > stall_timeout:
                    logger.warning(
                        f"No progress for {stall_timeout}s - system may be stalled")
                    # Continue monitoring rather than failing

            # Basic process health check
            if self.scheduler_process and self.scheduler_process.poll() is not None:
                raise RuntimeError("Scheduler process terminated unexpectedly")

            if self.job_generator_process and self.job_generator_process.poll() is not None:
                logger.warning("Job generator process terminated")
                # This might be normal if it finished generating jobs

        # Return measurement phase duration and job count
        final_measurement_duration = time.time() - measurement_start_time
        final_job_count = benchmark_utils.get_measurement_phase_job_count(log_dir) - measurement_job_count_start

        logger.warning(
            f"Phase 2:\nBenchmark measurement phase completed: {final_measurement_duration:.1f}s, {final_job_count} jobs\n")

        return final_measurement_duration, final_job_count

    # ------------------------------- DATA COLLECTION ----------------------------------- #

    # Update collect_run_metrics in benchmark.py to include these metrics

    async def collect_run_metrics(self, run_name: str, job_algorithm: Optional[str] = None, 
                                worker_algorithm: Optional[str] = None, iteration: Optional[int] = None,
                                benchmark_mode: Optional[str] = None) -> Dict[str, Any]:
        """Enhanced metrics collection with algorithm info for job export."""
        
        """
        Enhanced metrics collection with standardized time units and proper cold start metrics.
        All time-based metrics are consistently represented in milliseconds in the final dictionary.

        Args:
            run_name: Name of the run for logging

        Returns:
            Dictionary containing calculated metrics with standardized units
        """
        scheduler_data = {}
        worker_data = {}
        try:
            # Find latest log directory
            log_dir = benchmark_utils.find_latest_log_dir()
            if not log_dir:
                raise RuntimeError("No log directory found")

            # Parse logs
            scheduler_log_path = log_dir / "scheduler.log"
            try:
                scheduler_data = benchmark_utils.parse_scheduler_logs(scheduler_log_path)
            except Exception as e:
                logger.error(f"Error parsing scheduler logs: {e}")

            worker_log_paths = benchmark_utils.get_all_worker_logs(log_dir)
            try:
                worker_data = benchmark_utils.parse_worker_logs(worker_log_paths)
            except Exception as e:
                logger.error(f"Error parsing worker logs: {e}")

            # Export raw job-level data for this run
            benchmark_utils.export_job_level_data(
                scheduler_data, worker_data, run_name,
                job_algorithm=job_algorithm,
                worker_algorithm=worker_algorithm,
                iteration=iteration, 
                benchmark_mode=benchmark_mode
            )

            # Calculate individual metric categories
            throughput_metrics = benchmark_utils.calculate_throughput_metrics(scheduler_data, worker_data)
            latency_metrics = benchmark_utils.calculate_latency_metrics(scheduler_data, worker_data)
            reliability_metrics = benchmark_utils.calculate_reliability_metrics(scheduler_data, worker_data)
            load_balancing_metrics = benchmark_utils.calculate_load_balancing_metrics(scheduler_data, worker_data)
            
            # Use the enhanced cold start metrics calculation
            cold_start_metrics = benchmark_utils.calculate_enhanced_cold_start_metrics(scheduler_data, worker_data)

            # ========== CONSOLIDATE METRICS - REMOVE DUPLICATES ==========
            
            # Use throughput_metrics as the source of truth for job counts
            jobs_completed = throughput_metrics.get("jobs_completed", 0)
            
            # Consolidated metrics dictionary (no duplicates)
            # IMPORTANT: All time values standardized to milliseconds
            metrics = {
                # === CORE JOB METRICS (single source of truth) ===
                "jobs_completed": jobs_completed,
                "throughput_jobs_per_second": throughput_metrics.get("throughput_jobs_per_second", 0.0),
                "avg_worker_utilisation_percent": throughput_metrics.get("avg_worker_utilisation_percent", 0.0),
                
                # === LATENCY METRICS (milliseconds) ===
                "avg_queue_time_ms": latency_metrics.get("avg_queue_time_ms", 0.0),
                "avg_processing_time_ms": latency_metrics.get("avg_processing_time_ms", 0.0),
                
                # === RELIABILITY METRICS ===
                "total_received_jobs": reliability_metrics.get("total_received_jobs", 0),
                "failed_jobs": reliability_metrics.get("failed_jobs", 0),
                "pending_jobs": reliability_metrics.get("pending_jobs", 0),
                "in_progress_jobs": reliability_metrics.get("in_progress_jobs", 0),
                "job_success_rate": reliability_metrics.get("job_success_rate", 0.0),
                "reallocated_jobs": reliability_metrics.get("reallocated_jobs", 0),
                "avg_recovery_time_ms": reliability_metrics.get("avg_recovery_time_ms", 0.0),
                
                # === LOAD BALANCING METRICS ===
                "fairness_index": load_balancing_metrics.get("fairness_index", 0.0),
                "load_distribution_std": load_balancing_metrics.get("load_distribution_std", 0.0),
                "active_workers": load_balancing_metrics.get("active_workers", 0),
                "total_workers": load_balancing_metrics.get("total_workers", 0),
                "worker_participation_rate": load_balancing_metrics.get("worker_participation_rate", 0.0),
                
                # === COLD START METRICS (using enhanced data) ===
                "cold_start_rate_percent": cold_start_metrics.get("cold_start_rate_percent", 0.0),
                "avg_cold_start_penalty_ms": cold_start_metrics.get("avg_cold_start_penalty_ms", 0.0),
                "total_cold_starts": cold_start_metrics.get("total_cold_starts", 0),
                "avg_warm_penalty_ms": cold_start_metrics.get("avg_warm_penalty_ms", 0.0),
                "cold_penalty_percent_of_execution": cold_start_metrics.get("cold_penalty_percent_of_execution", 0.0),
                "warm_penalty_percent_of_execution": cold_start_metrics.get("warm_penalty_percent_of_execution", 0.0),
                "total_penalty_percent_of_execution": cold_start_metrics.get("total_penalty_percent_of_execution", 0.0),
                "cold_state_percent": cold_start_metrics.get("cold_state_percent", 0.0),
                "warm_state_percent": cold_start_metrics.get("warm_state_percent", 0.0),
                "hot_state_percent": cold_start_metrics.get("hot_state_percent", 0.0),
            }

            # Calculate phase-aware metrics
            try:
                phase_metrics = benchmark_utils.calculate_phase_metrics(scheduler_data, worker_data)
                
                # Add phase-specific metrics
                metrics.update({
                    "warm_up_duration": phase_metrics.get("warm_up_phase", {}).get("duration", 0.0),
                    "warm_up_jobs_completed": phase_metrics.get("warm_up_phase", {}).get("jobs_completed", 0),
                    "measurement_duration": phase_metrics.get("measurement_phase", {}).get("duration", 0.0),
                    "measurement_jobs_completed": phase_metrics.get("measurement_phase", {}).get("jobs_completed", 0),
                    "measurement_cold_starts": phase_metrics.get("measurement_phase", {}).get("cold_start_count", 0),
                })
            except Exception as e:
                logger.warning(f"Error calculating phase metrics: {e}")
                # Default values if phase metrics fail
                metrics.update({
                    "warm_up_duration": 0.0,
                    "warm_up_jobs_completed": 0,
                    "measurement_duration": 0.0,
                    "measurement_jobs_completed": 0,
                    "measurement_cold_starts": 0,
                })

            # Log key metrics for verification
            logger.debug(f"Metrics collected successfully for {run_name}")
            logger.info(f"Cold start metrics: {metrics['cold_start_rate_percent']}% rate, {metrics['total_cold_starts']} total")
            
            return metrics

        except Exception as e:
            logger.error(f"Error collecting metrics for {run_name}: {e}")
            return {
                # Error fallback values
                "jobs_completed": 0,
                "throughput_jobs_per_second": 0.0,
                "avg_worker_utilisation_percent": 0.0,
                "avg_queue_time_ms": 0.0,
                "avg_processing_time_ms": 0.0,
                "job_success_rate": 0.0,
                "avg_recovery_time_ms": 0.0,
                "fairness_index": 0.0,
                "active_workers": 0,
                "worker_participation_rate": 0.0,
                "total_cold_starts": 0,
                "cold_start_rate_percent": 0.0,
                "avg_cold_start_penalty_ms": 0.0,
            }

    def export_results(self, results_df: pd.DataFrame, filename: Optional[str] = None) -> str:
        """
        Export benchmark results to CSV file in timestamped folder.

        Args:
            results_df: DataFrame containing benchmark results
            filename: Optional custom filename

        Returns:
            Path to exported file
        """
        # Create timestamped folder for this benchmark run
        timestamp = time.strftime("%Y%m%d_%H%M%S")

        if filename is None:
            filename = f"benchmark_results_{timestamp}.csv"

        # Create timestamped results directory
        results_dir = Path("results") / f"benchmark_{timestamp}"
        results_dir.mkdir(parents=True, exist_ok=True)

        # Export path
        export_path = results_dir / filename

        try:
            # Export to CSV
            results_df.to_csv(export_path, index=False)

            # Create summary statistics
            summary_path = results_dir / f"summary_{filename}"
            summary_stats = self._create_summary_statistics(results_df)
            summary_stats.to_csv(summary_path, index=False)

            # Also create a "latest" symlink/copy for easy access
            latest_dir = Path("results") / "latest"
            try:
                if latest_dir.exists():
                    # Alternative approach not using shutil directly
                    for item in latest_dir.iterdir():
                        if item.is_file():
                            item.unlink()  # Delete file
                        elif item.is_dir():
                            for subitem in item.iterdir():
                                subitem.unlink()  # Delete files in subdirectory
                            item.rmdir()  # Delete empty directory
                    latest_dir.rmdir()  # Delete the now-empty directory
                
                # Create the directory
                latest_dir.mkdir(exist_ok=True)
                
                # Copy files using Path methods
                with open(export_path, 'rb') as src, open(latest_dir / "benchmark_results.csv", 'wb') as dst:
                    dst.write(src.read())
                
                with open(summary_path, 'rb') as src, open(latest_dir / "summary.csv", 'wb') as dst:
                    dst.write(src.read())
            except Exception as e:
                logger.error(f"Error creating 'latest' directory: {e}")
                
            logger.info(f"Results exported to: {export_path}")
            logger.info(f"Summary exported to: {summary_path}")
            logger.info(f"Latest results also available in: {latest_dir}")

            return str(export_path)

        except Exception as e:
            logger.error(f"Error exporting results: {e}")
            raise

    def _create_summary_statistics(self, results_df: pd.DataFrame) -> pd.DataFrame:
        """
        Create summary statistics from benchmark results with improved cold start metrics.

        Args:
            results_df: Full benchmark results

        Returns:
            DataFrame with summary statistics by algorithm combination
        """
        if results_df.empty:
            return pd.DataFrame()

        # Group by algorithm combination
        grouped = results_df.groupby(['job_algorithm', 'worker_algorithm'])

        # Calculate summary statistics
        summary_stats = grouped.agg({
            'benchmark_success': ['count', 'sum'],
            'jobs_completed': ['mean', 'std'],
            'throughput_jobs_per_second': ['mean', 'std'],
            'avg_worker_utilisation_percent': ['mean', 'std'],
            'avg_queue_time_ms': ['mean', 'std'],
            'avg_processing_time_ms': ['mean', 'std'],
            'job_success_rate': ['mean', 'std'],
            
            # Cold start metrics (true cold starts only)
            'cold_start_rate_percent': ['mean', 'std'],
            'avg_cold_start_penalty_ms': ['mean', 'std'],
            'total_cold_starts': ['mean', 'sum'],
            
            # Execution time impact metrics
            'cold_penalty_percent_of_execution': ['mean', 'std'],
            'warm_penalty_percent_of_execution': ['mean', 'std'],
            'total_penalty_percent_of_execution': ['mean', 'std'],
            
            # Warmth state distribution
            'cold_state_percent': ['mean', 'std'],
            'warm_state_percent': ['mean', 'std'],
            'hot_state_percent': ['mean', 'std'],
        }).round(3)

        # Flatten column names
        summary_stats.columns = ['_'.join(col).strip()
                                for col in summary_stats.columns]

        # Reset index to make algorithm columns regular columns
        summary_stats = summary_stats.reset_index()
        
        # Add cold start test mode indicator
        cold_start_test_mode = os.getenv("TASKWAVE_COLD_START_TEST", "false").lower() == "true"
        summary_stats['cold_start_test_mode'] = cold_start_test_mode

        return summary_stats

    # ------------------------------- UTILITIES ----------------------------------- #

    def create_run_name(self, job_algo: str, worker_algo: str, iteration: int) -> str:
        """
        Create a unique run name for the given parameters.

        Args:
            job_algo: Job scheduling algorithm
            worker_algo: Worker selection algorithm
            iteration: Iteration number

        Returns:
            Unique run name
        """
        date_str = time.strftime("%Y-%m-%d_%H-%M")
        return f"run_{date_str}_{job_algo}_{worker_algo}_iter{iteration:02d}"

    async def setup_logging_for_run(self, run_name: str, batch_folder: Path) -> None:
        """
        Setup logging configuration for a specific run.

        Args:
            run_name: Name of the run
            batch_folder: Folder where logs and summary should be stored
        """
        # Update run summary with current run info
        await update_run_summary_async(
            batch_folder,
            "Benchmark_Orchestrator",
            {"current_run": run_name}
        )

# ------------------------------- MAIN EXECUTION ----------------------------------- #


async def main():
    """Main entry point for benchmark orchestrator."""
    parser = argparse.ArgumentParser(
        description="TaskWave Benchmark Orchestrator")

    parser.add_argument(
        "--single-run",
        type=str,
        nargs=2,
        metavar=("JOB_ALGO", "WORKER_ALGO"),
        help="Run a single algorithm combination (e.g., --single-run rr_job least_loaded_worker)"
    )

    parser.add_argument(
        "--iterations",
        type=int,
        default=10,
        help="Number of iterations per algorithm combination (default: 10)"
    )

    parser.add_argument(
        "--duration",
        type=int,
        default=180,
        help="Maximum duration per run in seconds (default: 180)"
    )

    parser.add_argument(
        "--job-limit",
        type=int,
        default=200,
        help="Maximum number of jobs per run (default: 200)"
    )

    parser.add_argument(
        "--workers",
        type=int,
        default=4,
        help="Number of worker processes (default: 4)"
    )

    parser.add_argument(
        "--remote-workers",
        action="store_true",
        help="Use remote workers instead of starting local worker processes"
    )
    
    parser.add_argument(
    "--benchmark-mode",
    type=str,
    choices=["default", "light", "moderate", "heavy", "test", "cold_start_test", "validation_light", "validation_mod", "validation_heavy"],
    default="default",
    help="Benchmark mode to use (sets multiple parameters for the workload pattern)"
)

    parser.add_argument(
    "--cold-start-test",
    action="store_true",
    help="Enable cold start test mode with exaggerated parameters"
)

    parser.add_argument(
        "--start-iteration",
        type=int,
        default=1,
        help="Starting iteration number (default: 1)"
    )
    
    args = parser.parse_args()

    # Apply benchmark mode by setting environment variable
    if args.benchmark_mode and args.benchmark_mode != "default":
        os.environ["TASKWAVE_BENCHMARK_MODE"] = args.benchmark_mode
        logger.info(f"Set benchmark mode: {args.benchmark_mode}")
        
        # Force config reload by re-importing or applying mode settings directly
        # Since config.py has already been imported, we need to apply mode settings manually
        from config import BENCHMARK_MODES
        
        if args.benchmark_mode in BENCHMARK_MODES:
            mode_settings = BENCHMARK_MODES[args.benchmark_mode]
            logger.info(f"Applying benchmark mode settings: {mode_settings}")
            
            # Set environment variables for the mode
            for setting, value in mode_settings.items():
                env_var = f"TASKWAVE_{setting}"
                os.environ[env_var] = str(value)
                logger.info(f"  Set {env_var} = {value}")

    # Setup logging
    logger.info("TaskWave Benchmark Orchestrator starting")

    try:
        # Set remote workers environment variable
        if args.remote_workers:
            os.environ["TASKWAVE_REMOTE_WORKERS"] = "true"
            logger.info("Remote workers mode enabled")
            
        # Set cold start test mode if enabled
        if args.cold_start_test:
            os.environ["TASKWAVE_COLD_START_TEST"] = "true"
            logger.info("Cold start test mode enabled (exaggerated cold start penalties will be applied)")

        # Create orchestrator
        orchestrator = BenchmarkOrchestrator()

        if args.single_run:
            # Single run mode
            job_algo, worker_algo = args.single_run
            benchmark_run_name = orchestrator.create_run_name(job_algo, worker_algo, 1)

            logger.info(
                f"Running single benchmark: {job_algo} + {worker_algo}")

            result = await orchestrator.run_single_benchmark(
                job_algo, worker_algo, benchmark_run_name,
                args.duration, args.job_limit, args.workers
            )

            # Export single result
            results_df = pd.DataFrame([result])
            export_path = orchestrator.export_results(
                results_df, f"single_run_{benchmark_run_name}.csv")

            logger.info(f"Single run completed. Results: {export_path}")

        else:
            # Full benchmark mode
            logger.info(
                "Running comprehensive benchmark of all algorithm combinations")

            results_df = await orchestrator.run_all_combinations(
                iterations=args.iterations,
                duration=args.duration,
                job_limit=args.job_limit,
                worker_count=args.workers,
                start_iteration=args.start_iteration
            )

            logger.info(
                f"Comprehensive benchmark completed. {len(results_df)} runs executed.")

    except KeyboardInterrupt:
        logger.warning("Benchmark interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Benchmark failed: {e}")
        sys.exit(1)

    logger.info("Benchmark orchestrator finished")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Benchmark orchestrator interrupted by user")
        sys.exit(1)
