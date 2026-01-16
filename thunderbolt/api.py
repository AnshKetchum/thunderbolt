#!/usr/bin/env python3
"""
API Client for Master Command Runner
"""
import requests
from typing import List, Optional, Dict, Any


class ThunderboltAPI:
    """Client for interacting with the Master Command Runner API."""
    
    def __init__(self, host: str = "localhost", port: int = 8001, base_path: str = ""):
        """
        Initialize the Master API client.
        
        Args:
            host: Master server hostname or IP
            port: Master server REST API port (default: 8001)
            base_path: Base path prefix for the API (e.g., "/thunderbolt")
        """
        self.base_path = base_path.rstrip("/")  # Remove trailing slash if present
        self.base_url = f"http://{host}:{port}{self.base_path}"
    
    def list_nodes(self) -> Dict[str, Any]:
        """
        List all connected slave nodes.
        
        Returns:
            Dict containing:
                - total: Number of connected nodes
                - nodes: List of node information dicts
        
        Raises:
            requests.exceptions.RequestException: If the request fails
        """
        url = f"{self.base_url}/nodes"
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    
    def run_command(
        self,
        command: str,
        nodes: List[str],
        timeout: int = 30,
        use_sudo: bool = False
    ) -> Dict[str, Any]:
        """
        Execute a command on specified nodes.
        
        Args:
            command: Shell command to execute
            nodes: List of hostnames to run the command on
            timeout: Command timeout in seconds (default: 30)
            use_sudo: Whether to run with sudo privileges (default: False)
        
        Returns:
            Dict containing:
                - command: The executed command
                - total_nodes: Total number of target nodes
                - responses_received: Number of responses received
                - results: Dict mapping hostname to result dict
                    Each result contains:
                        - hostname: Node hostname
                        - stdout: Command standard output
                        - stderr: Command standard error
                        - returncode: Exit code
                        - status: "success", "error", or "timeout"
        
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
        
        response = requests.post(url, json=payload)
        response.raise_for_status()
        return response.json()
    
    def health(self) -> Dict[str, Any]:
        """
        Check master server health.
        
        Returns:
            Dict with health status and number of connected slaves
        """
        url = f"{self.base_url}/health"
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    
    def get_node_hostnames(self) -> List[str]:
        """
        Get list of all connected node hostnames.
        
        Returns:
            List of hostname strings
        """
        nodes_data = self.list_nodes()
        return [node['hostname'] for node in nodes_data['nodes']]
    
    def run_on_all_nodes(
        self,
        command: str,
        timeout: int = 30,
        use_sudo: bool = False
    ) -> Dict[str, Any]:
        """
        Execute a command on all connected nodes.
        
        Args:
            command: Shell command to execute
            timeout: Command timeout in seconds (default: 30)
            use_sudo: Whether to run with sudo privileges (default: False)
        
        Returns:
            Same format as run_command()
        
        Raises:
            ValueError: If no nodes are connected
        """
        hostnames = self.get_node_hostnames()
        if not hostnames:
            raise ValueError("No nodes are connected")
        
        return self.run_command(command, hostnames, timeout, use_sudo)
    
    def close(self):
        """Close the underlying session (no-op without session)."""
        pass
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()