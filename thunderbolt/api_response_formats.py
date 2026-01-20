import requests
from typing import List, Optional, Dict, Any
from pydantic import BaseModel
from datetime import datetime

class NodeInfo(BaseModel):
    """Information about a connected node."""
    hostname: str
    last_seen: datetime
    failed_healthchecks: int
    command_connected: bool
    health_connected: bool


class NodesListResponse(BaseModel):
    """Response from listing nodes."""
    total: int
    nodes: List[NodeInfo]


class CommandResponse(BaseModel):
    """Response from executing a single command."""
    command: str
    total_nodes: int
    responses_received: int
    failed_sends: Optional[int] = 0
    method: str
    results: Dict[str, Any]


class HealthResponse(BaseModel):
    """Response from health check endpoint."""
    status: str
    connected_slaves: int
    pending_commands: int


class InfoResponse(BaseModel):
    """Response from info endpoint."""
    message: str
    connected_slaves: int
    command_port: int
    health_check_port: int
    shared_directory: Optional[str] = None


class CommandSummary(BaseModel):
    """Summary of command execution results."""
    total_nodes: int
    successful: int
    failed: int
    timed_out: int
    failed_sends: int
    success_rate: float


class BatchedSummary(BaseModel):
    """Summary of batched command execution results."""
    total_commands: int
    total_nodes: int
    successful_commands: int
    failed_commands: int
    node_failures: int
    success_rate: float