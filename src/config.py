"""
Meta TaskWave Configuration Module

Central configuration for the distributed FaaS scheduling system.
Handles environment detection, network discovery, and parameter settings.
"""

import socket
import os
import logging
import json
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Setup default logger for config
from utils import setup_logger  
logger = setup_logger('Config', log_dir=Path("../logs"))

# ------------------------------- ENVIRONMENT CONFIGURATION ----------------------------------- #

# Environment detection
ENV = os.getenv("TASKWAVE_ENV", "development").lower()
logger.info(f"Running in {ENV} environment")

# ------------------------------- NETWORK DISCOVERY ----------------------------------- #

def get_local_ip():
    """Dynamically fetch the current local IP address."""
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        # Connect to a public address to get your local IP
        s.connect(("8.8.8.8", 80))
        return s.getsockname()[0]
    finally:
        s.close()
        
def get_server_ip():
    """
    Determines where to connect to.
    
    Gets server IP with multiple fallback methods:
    1. Environment variable (production/docker)
    2. Config file (LAN setup)  
    3. Localhost fallback (for local testing)  # Changed from auto-detection
    """
    
    # Method 1: Environment variable (highest priority)
    env_ip = os.getenv("TASKWAVE_SERVER_IP")
    if env_ip:
        logger.info(f"Using server IP from environment variable: {env_ip}")
        return env_ip
    
    # Method 2: Config file
    config_file = Path('network_config.json')
    if config_file.exists():
        try:
            with open(config_file, 'r') as f:
                network_config = json.load(f)
                if 'server_ip' in network_config:
                    logger.info(f"Using server IP from config file: {network_config['server_ip']}")
                    return network_config['server_ip']
        except (json.JSONDecodeError, KeyError, FileNotFoundError):
            logger.warning("Failed to read network config file, falling back to localhost")
            pass
        
    # Method 3: Localhost fallback (safe for local testing)
    logger.info("Using localhost fallback for local testing")
    return "127.0.0.1"  # Changed from get_local_ip()

def save_network_config(server_ip, port):
    """
    Save the server IP and port to a JSON config file.
    
    This is useful for LAN setups where the server IP might change.
    """
    config_file = Path('network_config.json')
    network_config = {
        'server_ip': server_ip,
        'port': port
    }
    
    with open(config_file, 'w') as f:
        json.dump(network_config, f, indent=4)
    
    logger.info(f"Saved network configuration to {config_file}")


# ------------------------------- SERVER CONFIGURATION ----------------------------------- #

# Get server IP using the discovery method
SERVER_IP = get_server_ip()
PORT = int(os.getenv("TASKWAVE_PORT", "57935"))

# Validate port range
if not (1024 <= PORT <= 65535):
    logger.warning(f"Port {PORT} outside recommended range (1024-65535)")
    # Optional default:
    logger.info("Falling back to default port 57935")
    PORT = 57935

# Save config for other nodes (only if this is the scheduler)
if __name__ == "__main__" or os.getenv("TASKWAVE_ROLE") == "scheduler":
    save_network_config(SERVER_IP, PORT)
    
# ------------------------------- LOGGING CONFIGURATION ----------------------------------- #

# Logging level based on environment
if ENV == "production":
    LOGGING_LEVEL = logging.INFO
elif ENV == "development":
    LOGGING_LEVEL = logging.DEBUG
else:
    LOGGING_LEVEL = logging.DEBUG

# ------------------------------- SCHEDULING CONFIGURATION ----------------------------------- #

# Scheduling algorithm (edf = Earliest Deadline First, uf = Urgency First)
JOB_SCHEDULING_ALGORITHM = os.getenv("TASKWAVE_JOB_ALGORITHM", "rr_job").lower()
WORKER_SCHEDULING_ALGORITHM = os.getenv("TASKWAVE_WORKER_ALGORITHM", "random_worker").lower()

# ------------------------------- CONNECTION CONFIGURATION ----------------------------------- #

# Connection retry settings
MAX_RETRIES = int(os.getenv("TASKWAVE_MAX_RETRIES", "5"))
BASE_DELAY = float(os.getenv("TASKWAVE_BASE_DELAY", "1.0"))
MAX_DELAY = float(os.getenv("TASKWAVE_MAX_DELAY", "16.0"))

# ------------------------------- PARAMETER CONFIGURATION ----------------------------------- #

# ===== JOB GENERATION ===== #
# Note: This represents the bursty behaviour when FaaS is invoked.
# Fixed
MAX_JOBS_PER_BATCH = int(os.getenv("TASKWAVE_MAX_JOBS_PER_BATCH", "15"))      # Increased from 8

# Batch delay represents the inter arrival time between job batches in real FaaS invocations.
# Note: When benchmarking, these fields must be changed using arguments
MIN_BATCH_DELAY = float(os.getenv("TASKWAVE_MIN_BATCH_DELAY", "0.5"))             # Reduced from 1
MAX_BATCH_DELAY = float(os.getenv("TASKWAVE_MAX_BATCH_DELAY", "1.5"))             # Reduced from 3

# JOB LOAD - Smaller, more frequent jobs
# Note: The job loads are on the higher side of literature values, but not unrealistic
# Rationale: Lower job loads (see validation modes) create concurrency congestion at validation_heavy
MIN_JOB_LOAD = int(os.getenv("TASKWAVE_MIN_JOB_LOAD", "200"))                
MAX_JOB_LOAD = int(os.getenv("TASKWAVE_MAX_JOB_LOAD", "600"))   
BASE_PROCESSING_TIME = float(os.getenv("TASKWAVE_BASE_PROCESSING_TIME", "2.0"))


# ===== WORKER CAPACITY ===== #
# Cold start simulation parameters - added 19.06.25 to include cold start simulation affecting FaaS op cost/resource usage.
COLD_START_PENALTY_BASE = float(os.getenv("TASKWAVE_COLD_START_PENALTY", "300.0"))  # ms
WARM_TIME_THRESHOLD = float(os.getenv("TASKWAVE_WARM_TIME", "60.0"))                # seconds until fully cold
COLD_START_ON_FIRST_RUN = os.getenv("TASKWAVE_COLD_START_FIRST", "true").lower() == "true"

# WORKER CAPACITY - More realistic constraints
# Note: While on higher side, these values are based on literature and AWS Lambda's common configurations
# Rationale: Higher capacity allows for more complex jobs, but not too high to avoid congestion
MIN_WORKER_CAPACITY = int(os.getenv("TASKWAVE_MIN_WORKER_CAPACITY", "600"))   # Reduced from 800
MAX_WORKER_CAPACITY = int(os.getenv("TASKWAVE_MAX_WORKER_CAPACITY", "1200"))  # Reduced from 2000

# Operations
# Note: These operations are typical for FaaS workloads, allowing for diverse job generation
# Example operations: Resize thumbnail, process image, calculate statistics, etc.
AVAILABLE_OPERATIONS = ["add", "subtract", "multiply", "divide", "power"]

# Worker health monitoring
HEARTBEAT_INTERVAL = float(os.getenv("TASKWAVE_HEARTBEAT_INTERVAL", "15.0"))  # seconds

# ------------------------------- COLD START CONFIGURATION ----------------------------------- #

# Cold start simulation parameters - added 19.06.25 to include cold start simulation affecting FaaS op cost/resource usage.
COLD_START_PENALTY_BASE = float(os.getenv("TASKWAVE_COLD_START_PENALTY", "300.0"))  # ms
WARM_TIME_THRESHOLD = float(os.getenv("TASKWAVE_WARM_TIME", "10.0"))                # seconds until warm->cold starts
COLD_TIME_THRESHOLD = float(os.getenv("TASKWAVE_COLD_TIME", "30.0"))               # ADD THIS - seconds until fully cold
WARM_START_PENALTY_FACTOR = 0.167  
COLD_START_ON_FIRST_RUN = os.getenv("TASKWAVE_COLD_START_FIRST", "true").lower() == "true"

# Cold start testing mode
COLD_START_TEST_MODE = os.getenv("TASKWAVE_COLD_START_TEST", "false").lower() == "true"

if COLD_START_TEST_MODE:
    # More visible cold start settings for testing
    COLD_START_PENALTY_BASE = 2000  # 2 seconds (exaggerated for visibility)
    WARM_TIME_THRESHOLD = 10.0      # 10 seconds (shortened for quicker testing)
    COLD_TIME_THRESHOLD = 5.0       # ADD THIS - 5 seconds to fully cold
    MIN_BATCH_DELAY = 5.0           # Longer delays between batches
    MAX_BATCH_DELAY = 15.0          # To ensure cold starts happen
    
    # Log the test mode activation
    logger.info("=" * 50)
    logger.info("COLD START TEST MODE ACTIVATED")
    logger.info("Cold Start Penalty: 2000ms")
    logger.info("Warm Time Threshold: 10s")
    logger.info("Cold Time Threshold: 5s")  # ADD THIS
    logger.info("Batch Delay Range: 5-15s")
    logger.info("=" * 50)
    
# ------------------------------- BENCHMARK CONFIGURATION ----------------------------------- #

# Benchmarking parameters
WARM_UP_PERIOD = int(os.getenv("TASKWAVE_WARM_UP_PERIOD", "30"))              # seconds
MEASUREMENT_DURATION = int(os.getenv("TASKWAVE_MEASUREMENT_DURATION", "180"))  # seconds

# Processing limits
BENCHMARK_JOB_LIMIT = int(os.getenv("TASKWAVE_BENCHMARK_JOB_LIMIT", "500"))   # Increased from 300
BENCHMARK_TIMEOUT = int(os.getenv("TASKWAVE_BENCHMARK_TIMEOUT", "300"))  # 5 minutes increased from 300
SAFETY_TIMEOUT = float(os.getenv("TASKWAVE_SAFETY_TIMEOUT", "300.0"))  # Increased from 180
STALL_TIMEOUT = float(os.getenv("TASKWAVE_STALL_TIMEOUT", "60.0"))   

# Orchestrator settings
STABILISATION_WAIT = int(os.getenv("TASKWAVE_STABILISATION_WAIT", "10"))    # System startup wait
CLEANUP_WAIT = int(os.getenv("TASKWAVE_CLEANUP_WAIT", "30"))  

# ------------------------------- BENCHMARK MODES ----------------------------------- #

BENCHMARK_MODE = os.getenv("TASKWAVE_BENCHMARK_MODE", "default").lower()

# Benchmark mode settings - these override other settings when a mode is activated
BENCHMARK_MODES = {
    "default": {
        # Default values - no changes
    },
    "light": {
        # Light FaaS workload (extended delays, less frequent invocations)
        "MIN_BATCH_DELAY": 8.0,      # 8-15 seconds between batches (from literature)
        "MAX_BATCH_DELAY": 15.0,
        "MAX_JOBS_PER_BATCH": 4,     # Smaller batches (literature: 4-8 lower range)
        "MIN_JOB_LOAD": 200,         # Standard complexity
        "MAX_JOB_LOAD": 600,         # Literature: within range
        "MIN_WORKER_CAPACITY": 600,  # Literature: 600 units
        "MAX_WORKER_CAPACITY": 1200, # Literature: 1200 units
        "COLD_START_PENALTY_BASE": 300.0,  # Literature: 300ms
        "WARM_TIME_THRESHOLD": 60.0        # Literature: 60 seconds
    },
    "moderate": {
        # Moderate FaaS workload (typical load, moderate frequency)
        "MIN_BATCH_DELAY": 2.0,      # 2-5 seconds between batches (from literature)
        "MAX_BATCH_DELAY": 5.0,
        "MAX_JOBS_PER_BATCH": 6,     # Moderate batches (literature: middle of 4-8 range)
        "MIN_JOB_LOAD": 250,         # Moderate complexity
        "MAX_JOB_LOAD": 600,         # Literature: within range
        "MIN_WORKER_CAPACITY": 600,  # Literature: standard capacity
        "MAX_WORKER_CAPACITY": 1200, # Literature: standard range
        "COLD_START_PENALTY_BASE": 300.0,  # Literature: 300ms
        "WARM_TIME_THRESHOLD": 60.0         # Literature: 60 seconds
    },
    "heavy": {
        # Heavy FaaS workload (busy system, high frequency)
        "MIN_BATCH_DELAY": 0.5,      # 0.5-1.5 seconds between batches (from literature)
        "MAX_BATCH_DELAY": 1.5,
        "MAX_JOBS_PER_BATCH": 8,     # Larger batches (literature: upper range)
        "MIN_JOB_LOAD": 300,         # Higher complexity
        "MAX_JOB_LOAD": 600,         # Literature: within range
        "MIN_WORKER_CAPACITY": 600,  # Literature: standard capacity
        "MAX_WORKER_CAPACITY": 1200, # Literature: standard range
        "COLD_START_PENALTY_BASE": 300.0,  # Literature: 300ms
        "WARM_TIME_THRESHOLD": 60.0         # Literature: 60 seconds
    },
    "test": {
        # Test mode with smaller parameters for quick validation
        "MIN_BATCH_DELAY": 1.0,      # Fast job generation
        "MAX_BATCH_DELAY": 3.0,
        "MAX_JOBS_PER_BATCH": 3,     # Smaller batches
        "MIN_JOB_LOAD": 100,         # Simpler jobs
        "MAX_JOB_LOAD": 300,
        "BENCHMARK_JOB_LIMIT": 50,   # Lower job limit
        "SAFETY_TIMEOUT": 120.0,     # Shorter timeout
        "COLD_START_PENALTY_BASE": 100.0,
        "WARM_TIME_THRESHOLD": 10.0  # Quick cold starts for testing
    },
    "cold_start_test": {
        # Special mode for testing cold start behaviour with exaggerated timings
        "WARM_TIME_THRESHOLD": 2.0,    # Very short warm period
        "COLD_TIME_THRESHOLD": 5.0,    # Very short before cold
        "COLD_START_PENALTY_BASE": 2000.0,  # Exaggerated penalty
        "WARM_START_PENALTY_FACTOR": 0.3,   # 30% for warm starts
        "MIN_BATCH_DELAY": 8.0,        # Give time for containers to cool
        "MAX_BATCH_DELAY": 15.0,
        "MAX_JOBS_PER_BATCH": 2 
    },
    "validation_light": {
        # Validation of light workload patterns
        "MIN_JOB_LOAD": 50,          # Literature: typical small functions
        "MAX_JOB_LOAD": 256,         # Literature: Shahrad - 50% use ≤170MB, 90% use ≤400MB
        "MIN_WORKER_CAPACITY": 256,  # Literature: AWS Lambda common allocations
        "MAX_WORKER_CAPACITY": 1024, # Literature: 1769MB = 1vCPU, optimal around 1024MB
        "MIN_BATCH_DELAY": 8.0,      # 8-15 seconds between batches (from literature)
        "MAX_BATCH_DELAY": 15.0,
        "MAX_JOBS_PER_BATCH": 6,     # Literature: Shahrad batch patterns
        "COLD_START_PENALTY_BASE": 300.0,  # Literature: standard cold start penalty
        "WARM_TIME_THRESHOLD": 60.0,        # Literature: typical warm threshold
        
        "BENCHMARK_JOB_LIMIT": 1000,         # Sufficient for statistical comparison
        "SAFETY_TIMEOUT": 300.0,            # 4 minutes to allow job completion
        "MEASUREMENT_DURATION": 180         # Keep standard 3-minute measurement
    },
    "validation_mod": {
        # Validation of moderate workload patterns

        "MIN_JOB_LOAD": 50,          # Literature: typical small functions
        "MAX_JOB_LOAD": 256,         # Literature: Shahrad - 50% use ≤170MB, 90% use ≤400MB
        "MIN_WORKER_CAPACITY": 256,  # Literature: AWS Lambda common allocations
        "MAX_WORKER_CAPACITY": 1024, # Literature: 1769MB = 1vCPU, optimal around 1024MB
        "MIN_BATCH_DELAY": 2.0,      # Literature: moderate workload patterns
        "MAX_BATCH_DELAY": 5.0,
        "MAX_JOBS_PER_BATCH": 6,     # Literature: Shahrad batch patterns
        "COLD_START_PENALTY_BASE": 300.0,  # Literature: standard cold start penalty
        "WARM_TIME_THRESHOLD": 60.0,        # Literature: typical warm threshold
        
        "BENCHMARK_JOB_LIMIT": 1000,         # Sufficient for statistical comparison
        "SAFETY_TIMEOUT": 300.0,            # 4 minutes to allow job completion
        "MEASUREMENT_DURATION": 180         # Keep standard 3-minute measurement
    },
    "validation_heavy": {
        # Validation of moderate workload patterns
        # Note: Do not use this. This causes system to break down. Project needs scaling for higher concurrency.
        "MIN_JOB_LOAD": 50,          # Literature: typical small functions
        "MAX_JOB_LOAD": 256,         # Literature: Shahrad - 50% use ≤170MB, 90% use ≤400MB
        "MIN_WORKER_CAPACITY": 256,  # Literature: AWS Lambda common allocations
        "MAX_WORKER_CAPACITY": 1024, # Literature: 1769MB = 1vCPU, optimal around 1024MB
        "MIN_BATCH_DELAY": 0.5,      # Literature: moderate workload patterns
        "MAX_BATCH_DELAY": 1.5,
        "MAX_JOBS_PER_BATCH": 6,     # Literature: Shahrad batch patterns
        "COLD_START_PENALTY_BASE": 300.0,  # Literature: standard cold start penalty
        "WARM_TIME_THRESHOLD": 60.0,        # Literature: typical warm threshold
        
        "BENCHMARK_JOB_LIMIT": 1000,         # Sufficient for statistical comparison
        "SAFETY_TIMEOUT": 300.0,            # 4 minutes to allow job completion
        "MEASUREMENT_DURATION": 180         # Keep standard 3-minute measurement
    }
}

# Apply benchmark mode settings
if BENCHMARK_MODE in BENCHMARK_MODES and BENCHMARK_MODE != "default":
    mode_settings = BENCHMARK_MODES[BENCHMARK_MODE]
    logger.info(f"Applying benchmark mode: {BENCHMARK_MODE}")
    
    # Apply each setting from the mode
    for setting, value in mode_settings.items():
        globals()[setting] = value
        logger.info(f"  {setting} = {value}")

# Additional check for environment variable overrides
for setting in BENCHMARK_MODES.get(BENCHMARK_MODE, {}):
    env_var = f"TASKWAVE_{setting}"
    if env_var in os.environ:
        try:
            # Convert the environment variable to the appropriate type
            value = type(globals()[setting])(os.environ[env_var])
            globals()[setting] = value
            logger.info(f"Override from environment: {setting} = {value}")
        except (ValueError, TypeError):
            logger.warning(f"Failed to parse environment variable {env_var}")

# Legacy support for direct cold start test mode
COLD_START_TEST_MODE = os.getenv("TASKWAVE_COLD_START_TEST", "false").lower() == "true"
if COLD_START_TEST_MODE and BENCHMARK_MODE == "default":
    # If --cold-start-test flag is used but no benchmark mode, apply cold start settings
    COLD_START_PENALTY_BASE = 2000.0  
    WARM_TIME_THRESHOLD = 10.0      
    MIN_BATCH_DELAY = 5.0           
    MAX_BATCH_DELAY = 15.0          
    logger.info("Legacy cold start test mode activated")


# ------------------------------- DEPLOYMENT HELPERS ----------------------------------- #

def get_role():
    """Determine what role this instance should play."""
    return os.getenv("TASKWAVE_ROLE", "auto").lower()

def is_scheduler():
    """Check if this instance should act as scheduler."""
    role = get_role()
    return role in ["scheduler", "auto"]

def is_worker():
    """Check if this instance should act as worker."""
    role = get_role()
    return role in ["worker", "auto"]

def is_generator():
    """Check if this instance should act as job generator."""
    role = get_role()
    return role in ["generator", "auto"]

# Print configuration summary
if __name__ == "__main__":
    logger.info("=" * 50)
    logger.info("TaskWave Configuration")
    logger.info("=" * 50)
    logger.info(f"Environment: {ENV}")
    logger.info(f"Server IP: {SERVER_IP}")
    logger.info(f"Port: {PORT}")
    logger.info(f"Job Scheduling Algorithm: {JOB_SCHEDULING_ALGORITHM}")
    logger.info(f"Worker Scheduling Algorithm: {WORKER_SCHEDULING_ALGORITHM}")
    logger.info(f"Logging Level: {logging.getLevelName(LOGGING_LEVEL)}")
    logger.info("=" * 50)
