#!/usr/bin/env python3
"""
API Client for Thunderbolt Master
"""
import requests
from typing import List, Optional, Dict, Any


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
    
    def list_nodes(self) -> Dict[str, Any]:
        """
        List all connected slave nodes.
        
        Returns:
            Dict containing:
                - total: Number of connected nodes
                - nodes: List of node information dicts
                    Each node dict contains:
                        - hostname: Node hostname
                        - last_seen: ISO timestamp of last communication
                        - failed_healthchecks: Number of consecutive failed health checks
                        - command_connected: Whether command channel is connected
                        - health_connected: Whether health channel is connected
        
        Raises:
            requests.exceptions.RequestException: If the request fails
        """
        url = f"{self.base_url}/nodes"
        response = self.session.get(url)
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
                - failed_sends: Number of nodes where command send failed
                - results: Dict mapping hostname to result dict
                    Each result contains:
                        - type: "command_result"
                        - command_id: UUID of the command
                        - hostname: Node hostname
                        - command: The executed command
                        - success: Boolean indicating if command succeeded
                        - exit_code: Exit code (if success=True)
                        - stdout: Command standard output (if success=True)
                        - stderr: Command standard error (if success=True)
                        - error: Error message (if success=False)
        
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
        
        response = self.session.post(url, json=payload)
        response.raise_for_status()
        return response.json()
    
    def run_batched_commands(self, commands: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Execute different commands on different nodes in a batched manner.
        Commands are grouped by node and executed sequentially per node,
        but all nodes run in parallel.
        
        Args:
            commands: List of command specifications, each containing:
                - node: str (hostname)
                - command: str (shell command to execute)
                - timeout: int (optional, default 30)
                - use_sudo: bool (optional, default False)
        
        Returns:
            Dict containing:
                - total_commands: Total number of commands
                - total_nodes: Number of unique nodes
                - results: Dict mapping hostname to result dict
                    Each result contains:
                        - commands: List of command results (if successful)
                            Each command result has:
                                - command: The executed command string
                                - result: Dict with success, exit_code, stdout, stderr, or error
                        - error: Error message (if node-level failure)
        
        Example:
            commands = [
                {"node": "node1", "command": "echo hello", "timeout": 10},
                {"node": "node1", "command": "echo world", "timeout": 10},
                {"node": "node2", "command": "uptime", "use_sudo": False}
            ]
            result = api.run_batched_commands(commands)
        
        Raises:
            requests.exceptions.RequestException: If the request fails
        """
        url = f"{self.base_url}/run_batched"
        payload = {"commands": commands}
        
        response = self.session.post(url, json=payload)
        response.raise_for_status()
        return response.json()
    
    def health(self) -> Dict[str, Any]:
        """
        Check master server health.
        
        Returns:
            Dict containing:
                - status: "healthy"
                - connected_slaves: Number of connected slave nodes
                - pending_commands: Number of commands awaiting responses
        """
        url = f"{self.base_url}/health"
        response = self.session.get(url)
        response.raise_for_status()
        return response.json()
    
    def info(self) -> Dict[str, Any]:
        """
        Get master server information.
        
        Returns:
            Dict containing:
                - message: Server description
                - connected_slaves: Number of connected slaves
                - command_port: Command WebSocket port
                - health_check_port: Health check WebSocket port
        """
        url = f"{self.base_url}/"
        response = self.session.get(url)
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
    
    def get_fully_connected_nodes(self) -> List[str]:
        """
        Get list of nodes with both command and health channels connected.
        
        Returns:
            List of hostname strings for fully connected nodes
        """
        nodes_data = self.list_nodes()
        return [
            node['hostname'] 
            for node in nodes_data['nodes']
            if node.get('command_connected') and node.get('health_connected')
        ]
    
    def run_on_all_nodes(
        self,
        command: str,
        timeout: int = 30,
        use_sudo: bool = False,
        require_fully_connected: bool = True
    ) -> Dict[str, Any]:
        """
        Execute a command on all connected nodes.
        
        Args:
            command: Shell command to execute
            timeout: Command timeout in seconds (default: 30)
            use_sudo: Whether to run with sudo privileges (default: False)
            require_fully_connected: Only run on nodes with both channels connected (default: True)
        
        Returns:
            Same format as run_command()
        
        Raises:
            ValueError: If no nodes are connected
        """
        if require_fully_connected:
            hostnames = self.get_fully_connected_nodes()
        else:
            hostnames = self.get_node_hostnames()
            
        if not hostnames:
            raise ValueError("No nodes are connected")
        
        return self.run_command(command, hostnames, timeout, use_sudo)
    
    def get_command_summary(self, result: Dict[str, Any]) -> Dict[str, Any]:
        """
        Generate a summary of command execution results.
        
        Args:
            result: Result dict from run_command() or run_on_all_nodes()
        
        Returns:
            Dict containing:
                - total_nodes: Total target nodes
                - successful: Number of successful executions
                - failed: Number of failed executions
                - timed_out: Number of nodes that didn't respond
                - failed_sends: Number of nodes where send failed
                - success_rate: Percentage of successful executions
        """
        total = result['total_nodes']
        received = result['responses_received']
        failed_sends = result.get('failed_sends', 0)
        
        successful = 0
        failed = 0
        
        for hostname, node_result in result['results'].items():
            if node_result.get('success'):
                successful += 1
            else:
                failed += 1
        
        timed_out = total - received - failed_sends
        
        return {
            'total_nodes': total,
            'successful': successful,
            'failed': failed,
            'timed_out': timed_out,
            'failed_sends': failed_sends,
            'success_rate': (successful / total * 100) if total > 0 else 0.0
        }
    
    def get_batched_summary(self, result: Dict[str, Any]) -> Dict[str, Any]:
        """
        Generate a summary of batched command execution results.
        
        Args:
            result: Result dict from run_batched_commands()
        
        Returns:
            Dict containing:
                - total_commands: Total number of commands executed
                - total_nodes: Number of nodes involved
                - successful_commands: Number of successful command executions
                - failed_commands: Number of failed command executions
                - node_failures: Number of nodes with node-level errors
                - success_rate: Percentage of successful command executions
        """
        total_commands = result['total_commands']
        total_nodes = result['total_nodes']
        
        successful_commands = 0
        failed_commands = 0
        node_failures = 0
        
        for hostname, node_result in result['results'].items():
            if 'error' in node_result:
                # Node-level failure
                node_failures += 1
                # Count all commands for this node as failed
                failed_commands += len([
                    cmd for cmd in result.get('commands', [])
                    if cmd.get('node') == hostname
                ])
            else:
                # Process individual command results
                for cmd_result in node_result.get('commands', []):
                    if cmd_result['result'].get('success'):
                        successful_commands += 1
                    else:
                        failed_commands += 1
        
        return {
            'total_commands': total_commands,
            'total_nodes': total_nodes,
            'successful_commands': successful_commands,
            'failed_commands': failed_commands,
            'node_failures': node_failures,
            'success_rate': (successful_commands / total_commands * 100) if total_commands > 0 else 0.0
        }
    
    def close(self):
        """Close the underlying session."""
        self.session.close()
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()