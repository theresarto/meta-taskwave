"""
Meta TaskWave Utils Module

Common utilities for the TaskWave system including logging, messaging, and helpers.

Key components:
- Coordinated logging setup for benchmark runs
- AsyncMessageHandler for JSON-based socket communication
- Log analysis utilities for finding recent runs
- Run summary tracking for experiments
- Worker-specific logging configuration
"""

import json
import logging
import os
import time
import asyncio
from pathlib import Path
from typing import Optional

# ------------------------------- LOGGING SETUP ----------------------------------- #

# Global variable to store the current batch folder for the run
_current_batch_folder = None
_run_counter = 0

def create_timestamped_batch_folder():
    """Create a timestamped batch folder for this run (down to minute precision)."""
    global _run_counter
    
    # Increment run counter
    _run_counter += 1
    
    # Create timestamp for basic folder identification (date only, not time)
    timestamp = time.strftime("%Y-%m-%d")
    
    # Create folder with sequential run number
    folder_name = f"run_{timestamp}_run{_run_counter:03d}"
    batch_folder = Path("logs") / folder_name
    batch_folder.mkdir(parents=True, exist_ok=True)
    
    print(f"Created sequential batch folder: {batch_folder}")
    return batch_folder

def reset_batch_folder():
    """Reset the global batch folder to force creation of a new one."""
    global _current_batch_folder
    _current_batch_folder = None

def get_or_create_batch_folder():
    """Get the current batch folder, or create one if none exists."""
    global _current_batch_folder
    if _current_batch_folder is None:
        _current_batch_folder = create_timestamped_batch_folder()
    return _current_batch_folder

def setup_logger(name, log_level=logging.INFO, console_level=logging.WARNING, log_dir=Path("logs")):
    """Set up a logger with timestamped file and console handlers."""
    
    if isinstance(log_dir, str):
        log_dir = Path(log_dir)
    
    # Create logger
    logger = logging.getLogger(name)
    logger.setLevel(log_level)
    
    # Prevent duplicate handlers
    if logger.handlers:
        return logger
    
    # Create formatters
    formatter = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    
    # File handler - now in timestamped folder
    try:
        file_path = log_dir / f"{name.lower().replace(' ', '_')}.log"
        file_handler = logging.FileHandler(file_path, mode="w", encoding='utf-8')
        file_handler.setFormatter(formatter)
        file_handler.setLevel(log_level)
        
        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        console_handler.setLevel(console_level)
        
        # Add handlers
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)
    except Exception as e:
        print(f"Error setting up logger: {e}")
    
    return logger

def setup_worker_logger(worker_id, batch_folder=None):
    """
    Set up logging for a specific worker with batched log files.
    
    Args:
        worker_id: Unique identifier for the worker
        batch_folder: Path to the folder for all logs in this run
        
    Returns:
        Logger instance configured for this worker
    """
    
    # Get run name from environment if batch_folder not provided
    if batch_folder is None:
        run_name = os.getenv("TASKWAVE_RUN_NAME")
        if run_name:
            batch_folder = Path("logs") / run_name
            if not batch_folder.exists():
                batch_folder.mkdir(parents=True, exist_ok=True)
        else:
            # Fallback to creating a batch folder
            batch_folder = get_or_create_batch_folder()
    
    # Ensure batch_folder is a Path object
    elif isinstance(batch_folder, str):
        batch_folder = Path(batch_folder)
    
    logger = logging.getLogger(f"Worker_{worker_id}")
    logger.setLevel(logging.INFO)
    
    # Prevent duplicate handlers
    if logger.handlers:
        for handler in logger.handlers:
            logger.removeHandler(handler)
    
    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
    
    try:
        # File handler
        log_file_path = batch_folder / f"worker_{worker_id}.log"
        file_handler = logging.FileHandler(log_file_path, mode="w", encoding='utf-8')
        file_handler.setFormatter(formatter)
        
        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)
        
        logger.info(f"Worker {worker_id} logger initialized. Logs will be saved to: {log_file_path}")
    except Exception as e:
        print(f"Error setting up worker logger: {e}")
    
    return logger

def setup_coordinated_logging(run_name: Optional[str] = None):
    """
    Set up coordinated logging for all components in the same timestamped folder.
    
    Returns the batch folder path so all components can use the same folder.
    """
    if not run_name:
        raise ValueError("run_name must be provided to setup_coordinated_logging()")

    batch_folder = Path("logs") / run_name
    batch_folder.mkdir(parents=True, exist_ok=True)
    
    # Create a run summary file
    summary_file = batch_folder / "run_summary.json"
    
    run_info = {
        "run_id": batch_folder.name,
        "start_time": time.time(),
        "start_time_human": time.strftime("%Y-%m-%d %H:%M:%S"),
        "components": [],
        "algorithms": {},
        "status": "running"
    }
    
    try:
        with open(summary_file, 'w') as f:
            json.dump(run_info, f, indent=2)
        
        print(f"📁 Created new run folder: {batch_folder}")
        print(f"📊 All logs for this run will be saved in: {batch_folder}")
    except Exception as e:
        print(f"Error creating run summary: {e}")
    
    return batch_folder

def log_run_completion(batch_folder, stats=None):
    """Update the run summary with completion information."""
    # Ensure batch_folder is a Path object
    if isinstance(batch_folder, str):
        batch_folder = Path(batch_folder)
        
    summary_file = batch_folder / "run_summary.json"
    
    if summary_file.exists():
        try:
            with open(summary_file, 'r') as f:
                run_info = json.load(f)
            
            run_info.update({
                "end_time": time.time(),
                "end_time_human": time.strftime("%Y-%m-%d %H:%M:%S"),
                "duration_seconds": time.time() - run_info["start_time"],
                "status": "completed"
            })
            
            if stats:
                run_info["final_stats"] = stats
            
            with open(summary_file, 'w') as f:
                json.dump(run_info, f, indent=2)
            
            print(f"✅ Run completed. Summary saved to: {summary_file}")
        except Exception as e:
            print(f"Error updating run summary: {e}")

def update_run_summary(batch_folder, component_name, algorithm_info=None):
    """Update the run summary with component and algorithm information."""
    # Ensure batch_folder is a Path object
    if isinstance(batch_folder, str):
        batch_folder = Path(batch_folder)
        
    summary_file = batch_folder / "run_summary.json"
    
    if summary_file.exists():
        try:
            with open(summary_file, 'r') as f:
                run_info = json.load(f)
            
            if component_name not in run_info["components"]:
                run_info["components"].append(component_name)
            
            if algorithm_info:
                run_info["algorithms"].update(algorithm_info)
            
            with open(summary_file, 'w') as f:
                json.dump(run_info, f, indent=2)
        except Exception as e:
            print(f"Error updating run summary: {e}")

# ------------------------------- ENHANCED MESSAGE HANDLER ----------------------------------- #

class AsyncMessageHandler:
    """Asynchronous message handler for socket communication using JSON."""
    
    def __init__(self, reader, writer, addr=None, format="utf-8"):
        self.reader = reader
        self.writer = writer
        self.addr = addr  # keep track of who's sending and receiving messages
        self.format = format
        self.logger = logging.getLogger("AsyncMessageHandler")
        
    async def send_message(self, message):
        try:
            if isinstance(message, dict):
                message_str = json.dumps(message)
            else:
                message_str = message
                
            message_data = message_str.encode(self.format)
            msg_length = len(message_data)
            
            header = str(msg_length).encode(self.format).ljust(64, b' ')
            
            self.writer.write(header)
            self.writer.write(message_data)
            await self.writer.drain()  # Key feature of asyncio (compared to threading) -- no malformed json
            
            self.logger.debug(f"Sent message to {self.addr}: {message}")
            return True
        
        except Exception as e:
            self.logger.error(f"Error sending message to {self.addr}: {e}")
            return False

    async def receive_message(self):
        try:
            raw_length = await self.reader.readexactly(64)
            if not raw_length:
                return None
            
            msg_length = raw_length.decode(self.format).strip()
            if not msg_length:
                return None
            
            msg_length = int(msg_length)
            
            message_data = await self.reader.readexactly(msg_length)
            if not message_data: 
                return None
            
            return json.loads(message_data.decode(self.format))
            
        except json.JSONDecodeError:
            self.logger.error("Error decoding JSON message")
            return None
        except Exception as e:
            self.logger.error(f"Error receiving message: {e}")
            return None

# ------------------------------- ASYNC VERSIONS OF UTILITY FUNCTIONS ----------------------------------- #

async def update_run_summary_async(batch_folder, component_name, algorithm_info=None):
    """Async version of update_run_summary."""
    # Ensure batch_folder is a Path object
    if isinstance(batch_folder, str):
        batch_folder = Path(batch_folder)
        
    summary_file = batch_folder / "run_summary.json"
    
    if summary_file.exists():
        try:
            # Use a thread pool executor for file operations to avoid blocking the event loop
            loop = asyncio.get_event_loop()
            
            # Read the file
            with open(summary_file, 'r') as f:
                content = await loop.run_in_executor(None, f.read)
                run_info = json.loads(content)
            
            # Update the data
            if component_name not in run_info["components"]:
                run_info["components"].append(component_name)
            
            if algorithm_info:
                run_info["algorithms"].update(algorithm_info)
            
            # Write the file
            json_content = json.dumps(run_info, indent=2)
            with open(summary_file, 'w') as f:
                await loop.run_in_executor(None, lambda: f.write(json_content))
                
        except Exception as e:
            print(f"Error updating run summary asynchronously: {e}")

async def log_run_completion_async(batch_folder, stats=None):
    """Async version of log_run_completion."""
    # Ensure batch_folder is a Path object
    if isinstance(batch_folder, str):
        batch_folder = Path(batch_folder)
        
    summary_file = batch_folder / "run_summary.json"
    
    if summary_file.exists():
        try:
            # Use a thread pool executor for file operations to avoid blocking the event loop
            loop = asyncio.get_event_loop()
            
            # Read the file
            with open(summary_file, 'r') as f:
                content = await loop.run_in_executor(None, f.read)
                run_info = json.loads(content)
            
            # Update the data
            run_info.update({
                "end_time": time.time(),
                "end_time_human": time.strftime("%Y-%m-%d %H:%M:%S"),
                "duration_seconds": time.time() - run_info["start_time"],
                "status": "completed"
            })
            
            if stats:
                run_info["final_stats"] = stats
            
            # Write the file
            json_content = json.dumps(run_info, indent=2)
            with open(summary_file, 'w') as f:
                await loop.run_in_executor(None, lambda: f.write(json_content))
            
            print(f"✅ Run completed. Summary saved to: {summary_file}")
        except Exception as e:
            print(f"Error updating run summary asynchronously: {e}")

# ------------------------------- LOG ANALYSIS UTILITIES ----------------------------------- #

def find_latest_run_folder():
    """Find the most recent run folder."""
    logs_dir = Path("logs")
    if not logs_dir.exists():
        return None
    
    run_folders = [f for f in logs_dir.iterdir() if f.is_dir() and f.name.startswith("run_")]
    if not run_folders:
        return None
    
    # Sort by creation time and return the latest
    latest_folder = max(run_folders, key=lambda x: x.stat().st_mtime)
    return latest_folder

def list_all_runs():
    """List all available run folders with their summaries."""
    logs_dir = Path("logs")
    if not logs_dir.exists():
        print("No logs directory found.")
        return
    
    run_folders = [f for f in logs_dir.iterdir() if f.is_dir() and f.name.startswith("run_")]
    if not run_folders:
        print("No run folders found.")
        return
    
    print("📋 Available runs:")
    print("-" * 80)
    
    for run_folder in sorted(run_folders, key=lambda x: x.stat().st_mtime, reverse=True):
        summary_file = run_folder / "run_summary.json"
        
        if summary_file.exists():
            try:
                with open(summary_file, 'r') as f:
                    run_info = json.load(f)
                
                status_emoji = "✅" if run_info.get("status") == "completed" else "🔄"
                duration = run_info.get("duration_seconds", 0)
                
                print(f"{status_emoji} {run_folder.name}")
                print(f"    📅 {run_info.get('start_time_human', 'Unknown')}")
                print(f"    ⏱️  Duration: {duration:.1f}s")
                print(f"    🔧 Components: {', '.join(run_info.get('components', []))}")
                
                algorithms = run_info.get('algorithms', {})
                if algorithms:
                    print(f"    🤖 Algorithms: {algorithms}")
                print()
            except Exception as e:
                print(f"Error reading summary from {run_folder.name}: {e}")
        else:
            print(f"📁 {run_folder.name} (no summary)")