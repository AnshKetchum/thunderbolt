#!/usr/bin/env python3
"""
API Client for Thunderbolt Master
"""
import requests
from typing import List, Optional, Dict, Any
from pydantic import BaseModel
from datetime import datetime

from .master_utils.execution.response_models import CommandResult 
from .api_response_formats import NodesListResponse, HealthResponse, InfoResponse



class ThunderboltAPI:
    """Client for interacting with the Thunderbolt Master API."""
    
    def __init__(self, host: str = "localhost", port: int = 8001, base_path: str = ""):
        """
        Initialize the Thunderbolt API client.
        
        Args:
            host: Master server hostname or IP
            port: Master server REST API port (default: 8001)
            base_path: Base path prefix for the API (e.g., "/api/v1")
        """
        self.base_path = base_path.rstrip("/")  # Remove trailing slash if present
        self.base_url = f"http://{host}:{port}{self.base_path}"
        self.session = requests.Session()
    
    def list_nodes(self) -> NodesListResponse:
        """
        List all connected slave nodes.
        
        Returns:
            NodesListResponse containing total count and list of node information
        
        Raises:
            requests.exceptions.RequestException: If the request fails
        """
        url = f"{self.base_url}/nodes"
        response = self.session.get(url)
        response.raise_for_status()
        return NodesListResponse(**response.json())
    
    def run_command(
        self,
        command: str,
        nodes: List[str],
        timeout: int = 30,
        use_sudo: bool = False,
        force_method: Optional[str] = None
    ) -> List[CommandResult]:
        """
        Execute a command on specified nodes.
        
        Args:
            command: Shell command to execute
            nodes: List of hostnames to run the command on
            timeout: Command timeout in seconds (default: 30)
            use_sudo: Whether to run with sudo privileges (default: False)
            force_method: Force execution method - "shared_dir" or "websocket" (default: None for auto)
        
        Returns:
            List of CommandResult objects containing execution results for each node
        
        Raises:
            requests.exceptions.RequestException: If the request fails
        """
        url = f"{self.base_url}/run"
        payload = {
            "command": command,
            "nodes": nodes,
            "timeout": timeout,
            "use_sudo": use_sudo
        }
        
        if force_method is not None:
            payload["force_method"] = force_method
        
        response = self.session.post(url, json=payload)
        response.raise_for_status()
        
        return [CommandResult(**result) for result in response.json()]

    def run_batched_commands(
        self,
        commands: List[Dict[str, Any]],
        force_method: Optional[str] = None
    ) -> List[CommandResult]:
        """
        Execute different commands on different nodes in a batched manner.
        Results are returned in the same order as the input commands.
        
        Args:
            commands: List of command specifications, each containing:
                - node: str (hostname)
                - command: str (shell command to execute)
                - timeout: int (optional, default 30)
                - use_sudo: bool (optional, default False)
            force_method: Force execution method - "shared_dir" or "websocket" (default: None for auto)
        
        Returns:
            List[CommandResult] containing execution results in the same order as input
        
        Example:
            commands = [
                {"node": "node1", "command": "echo hello", "timeout": 10},
                {"node": "node1", "command": "echo world", "timeout": 10},
                {"node": "node2", "command": "uptime", "use_sudo": False}
            ]
            results = api.run_batched_commands(commands, force_method="websocket")
            # results[0] corresponds to commands[0], etc.
        
        Raises:
            requests.exceptions.RequestException: If the request fails
        """
        url = f"{self.base_url}/run_batched"
        payload = {"commands": commands}
        
        if force_method is not None:
            payload["force_method"] = force_method
        
        response = self.session.post(url, json=payload)
        response.raise_for_status()
        return [CommandResult(**result) for result in response.json()]

    def run_on_all_nodes(
        self,
        command: str,
        timeout: int = 30,
        use_sudo: bool = False,
        require_fully_connected: bool = True,
        force_method: Optional[str] = None
    ) -> List[CommandResult]:
        """
        Execute a command on all connected nodes.
        
        Args:
            command: Shell command to execute
            timeout: Command timeout in seconds (default: 30)
            use_sudo: Whether to run with sudo privileges (default: False)
            require_fully_connected: Only run on nodes with both channels connected (default: True)
            force_method: Force execution method - "shared_dir" or "websocket" (default: None for auto)
        
        Returns:
            List[CommandResult] containing execution results
        
        Raises:
            ValueError: If no nodes are connected
        """
        if require_fully_connected:
            hostnames = self.get_fully_connected_nodes()
        else:
            hostnames = self.get_node_hostnames()
            
        if not hostnames:
            raise ValueError("No nodes are connected")
        
        return self.run_command(command, hostnames, timeout, use_sudo, force_method)

    def health(self) -> HealthResponse:
        """
        Check master server health.
        
        Returns:
            HealthResponse containing status and metrics
        """
        url = f"{self.base_url}/health"
        response = self.session.get(url)
        response.raise_for_status()
        return HealthResponse(**response.json())
    
    def info(self) -> InfoResponse:
        """
        Get master server information.
        
        Returns:
            InfoResponse containing server configuration details
        """
        url = f"{self.base_url}/"
        response = self.session.get(url)
        response.raise_for_status()
        return InfoResponse(**response.json())
    
    def get_node_hostnames(self) -> List[str]:
        """
        Get list of all connected node hostnames.
        
        Returns:
            List of hostname strings
        """
        nodes_data = self.list_nodes()
        return [node.hostname for node in nodes_data.nodes]
    
    def get_fully_connected_nodes(self) -> List[str]:
        """
        Get list of nodes with both command and health channels connected.
        
        Returns:
            List of hostname strings for fully connected nodes
        """
        nodes_data = self.list_nodes()
        return [
            node.hostname 
            for node in nodes_data.nodes
            if node.command_connected and node.health_connected
        ]
    
    
    def close(self):
        """Close the underlying session."""
        self.session.close()
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()