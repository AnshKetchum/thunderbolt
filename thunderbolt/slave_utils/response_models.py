"""
Response models for Thunderbolt command execution.
"""
from typing import List, Optional
from pydantic import BaseModel


class CommandResult(BaseModel):
    """Model for command execution results."""
    command_uuid: str
    node: str
    command: str
    stdout: Optional[str] = None
    stderr: Optional[str] = None
    exit_code: Optional[int] = None
    error: Optional[str] = None
    timed_out: bool = False


class BatchedResponse(BaseModel):
    """Model for batched command response."""
    total_commands: int
    total_nodes: int
    method: str
    results: List[CommandResult]