"""
Meta TaskWave Job Module

Defines the Job class representing units of work in the TaskWave system.
Jobs contain operations, data, and scheduling metadata for processing by workers.

Key components:
- Job properties (id, operation, numbers, load, priority, deadline)
- Serialisation methods for network transmission
- Status tracking throughout lifecycle
- Urgency score calculation for scheduling
"""

import time
from typing import List, Dict, Any, Optional


class Job:
    """
    Represents a task to be processed by a worker. 
    Created by the job_generator.
    """

    def __init__(self, job_id: int, operation: str, numbers: List[int], job_load: int, priority: int = 0, deadline: float = 0.0):
        self.job_id: int = job_id
        self.operation: str = operation
        self.numbers: List[int] = numbers

        self.job_load: int = job_load
        self.priority: int = priority
        self.deadline: float = deadline
        self.creation_time: float = time.time()
        self.status: str = "pending"  # pending, assigned, processing, completed, failed
        self.worker_id: Optional[str] = None  # checks which worker it's assigned to
        self.result: Optional[Any] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert job to dictionary for serialisation."""
        return {
            "job_id": self.job_id,
            "operation": self.operation,
            "numbers": self.numbers,
            "job_load": self.job_load,
            "priority": self.priority,
            "deadline": self.deadline,
            "creation_time": self.creation_time,
            "status": self.status,
            "worker_id": self.worker_id,
            "result": self.result
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Job':
        """Create a Job instance from a dictionary."""
        if "job_id" not in data:
            raise ValueError("job_id is required")

        job = cls(
            job_id=data["job_id"],  # ← Now guaranteed to exist
            operation=data.get("operation", ""),
            numbers=data.get("numbers", []),
            job_load=data.get("job_load", 0),
            priority=data.get("priority", 0),
            deadline=data.get("deadline", 0.0)
        )

        # Set optional fields if provided
        if "status" in data:
            job.status = data["status"]
        if "worker_id" in data:
            job.worker_id = data["worker_id"]
        if "result" in data:
            job.result = data["result"]
        if "creation_time" in data:
            job.creation_time = data["creation_time"]

        return job

    def __repr__(self) -> str:
        """String representation of the job."""
        return f"[JOB]({self.job_id}, {self.operation}, {self.numbers}, priority={self.priority}, deadline={self.deadline}, status={self.status})"

    @property
    def age(self) -> float:
        """Calculate age of job in seconds. Time since creation."""
        return time.time() - self.creation_time
    
    @property
    def time_remaining(self) -> float:
        """Calculate time remaining until deadline in seconds."""
        if self.deadline <= 0:
            return float('inf')
        return max(0, self.deadline - time.time())
    
    @property
    def urgency_score(self) -> float:
        """Calculate urgency score based on priority and time remaining. HIGHER = MORE URGENT."""
        if self.deadline <= 0:  # No deadline set
            return float(self.priority)
        
        time_factor = max(1, 10 / (1 + self.time_remaining))  # Higher time remaining reduces urgency score. Lowest is 1.
        return float(self.priority) * time_factor
    
    @property
    def is_overdue(self) -> bool:
        """Check if the job is overdue based on its deadline."""
        return self.deadline > 0 and time.time() > self.deadline
    
    @property
    def is_urgent(self) -> bool:
        """Check if the job is urgent based on its urgency score."""
        return self.priority > 0
    
    @property  
    def is_critical(self) -> bool:
        """Check if job is critically urgent (high urgency score)."""
        return self.urgency_score > 5.0
    
    def update_status(self, status: str, result: Any = None) -> None:
        """Update the job status and optionally set the result."""
        self.status = status
        if result is not None:
            self.result = result
        
    