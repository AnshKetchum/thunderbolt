import pytest
import docker
import time
from typing import List, Optional
import logging
from pathlib import Path
from thunderbolt.api import ThunderboltAPI

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ThunderboltTestCluster:
    """Manages a test cluster of master and slave containers."""
    
    def __init__(
        self, 
        num_slaves: int = 2,
        shared_dir: Optional[str] = None,
        shared_dir_threshold: int = 10,
        shared_dir_poll_interval: float = 0.5
    ):
        self.client = docker.from_env()
        self.num_slaves = num_slaves
        self.master_container = None
        self.slave_containers: List[docker.models.containers.Container] = []
        self.network = None
        self.command_port = 8000
        self.health_port = 8100
        self.api_port = 8001
        self.master_host = "thunderbolt-master"
        self.api = None
        
        # Shared directory configuration
        self.shared_dir = shared_dir
        self.shared_dir_threshold = shared_dir_threshold
        self.shared_dir_poll_interval = shared_dir_poll_interval
        self.shared_dir_container_path = "/shared" if shared_dir else None
        
    def setup(self):
        """Set up the test cluster with master and slave containers."""
        logger.info("Setting up thunderbolt test cluster...")
        logger.info(f"  Slaves: {self.num_slaves}")
        if self.shared_dir:
            logger.info(f"  Shared directory: {self.shared_dir}")
            logger.info(f"  Shared dir threshold: {self.shared_dir_threshold}")
        
        # Create a dedicated network for the test
        network_name = f"thunderbolt-test-{int(time.time())}"
        self.network = self.client.networks.create(
            name=network_name,
            driver="bridge"
        )
        logger.info(f"Created network: {network_name}")
        
        # Build the thunderbolt image if it doesn't exist
        try:
            self.client.images.get("thunderbolt:test")
            logger.info("Using existing thunderbolt:test image")
        except docker.errors.ImageNotFound:
            logger.info("Building thunderbolt:test image...")
            self.client.images.build(
                path=".",
                tag="thunderbolt:test",
                rm=True
            )
        
        # Start master container
        logger.info("Starting master container...")
        self.master_container = self._start_master(network_name)
        
        # Wait for master to be ready
        self._wait_for_master()
        logger.info("Master container is ready")
        
        # Initialize API client
        self.api = ThunderboltAPI(host="localhost", port=self.api_port)
        
        # Start slave containers
        for i in range(self.num_slaves):
            slave_name = f"thunderbolt-slave-{i}"
            logger.info(f"Starting slave container: {slave_name}")
            
            slave_container = self._start_slave(network_name, slave_name)
            self.slave_containers.append(slave_container)
        
        # Wait for slaves to connect
        self._wait_for_slaves()
        logger.info(f"All {self.num_slaves} slave containers are connected")
        
    def _start_master(self, network_name: str):
        """Start the master container with appropriate configuration."""
        # Base command
        command = [
            "thunderbolt-master",
            "--host", "0.0.0.0",
            "--command-port", str(self.command_port),
            "--health-port", str(self.health_port),
            "--api-port", str(self.api_port)
        ]
        
        # Add shared directory args if configured
        if self.shared_dir:
            command.extend([
                "--shared-dir", self.shared_dir_container_path,
                "--shared-dir-threshold", str(self.shared_dir_threshold),
                "--shared-dir-poll-interval", str(self.shared_dir_poll_interval)
            ])
        
        # Prepare container config
        container_config = {
            "image": "thunderbolt:test",
            "command": command,
            "name": self.master_host,
            "network": network_name,
            "ports": {
                f"{self.api_port}/tcp": self.api_port  # Expose API port to host
            },
            "detach": True,
            "remove": True,
            "environment": {
                "PYTHONUNBUFFERED": "1"
            }
        }
        
        # Add volume mount if shared directory configured
        if self.shared_dir:
            container_config["volumes"] = {
                self.shared_dir: {
                    "bind": self.shared_dir_container_path,
                    "mode": "rw"
                }
            }
        
        return self.client.containers.run(**container_config)
    
    def _start_slave(self, network_name: str, slave_name: str):
        """Start a slave container with appropriate configuration."""
        # Base command
        command = [
            "thunderbolt-slave",
            "--master", self.master_host,
            "--command-port", str(self.command_port),
            "--health-port", str(self.health_port),
            "--hostname", slave_name
        ]
        
        # Add shared directory args if configured
        if self.shared_dir:
            command.extend([
                "--shared-dir", self.shared_dir_container_path,
                "--shared-dir-poll-interval", str(self.shared_dir_poll_interval)
            ])
        
        # Prepare container config
        container_config = {
            "image": "thunderbolt:test",
            "command": command,
            "name": slave_name,
            "network": network_name,
            "detach": True,
            "remove": True,
            "environment": {
                "PYTHONUNBUFFERED": "1"
            }
        }
        
        # Add volume mount if shared directory configured
        if self.shared_dir:
            container_config["volumes"] = {
                self.shared_dir: {
                    "bind": self.shared_dir_container_path,
                    "mode": "rw"
                }
            }
        
        return self.client.containers.run(**container_config)
    
    def _wait_for_master(self, timeout: int = 30, interval: float = 0.5):
        """Wait for master to be ready to accept connections."""
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            try:
                # Try to get master health using temporary API client
                temp_api = ThunderboltAPI(host="localhost", port=self.api_port)
                health_data = temp_api.health()
                temp_api.close()
                logger.info(f"Master health check: {health_data}")
                return
            except Exception as e:
                logger.debug(f"Waiting for master: {e}")
            
            time.sleep(interval)
        
        # Try to get logs for debugging
        if self.master_container:
            logs = self.master_container.logs().decode('utf-8')
            logger.error(f"Master container logs:\n{logs}")
        
        raise TimeoutError(f"Master did not become ready within {timeout}s")
    
    def _wait_for_slaves(self, timeout: int = 60, interval: float = 1.0):
        """Wait for all slaves to connect to master (both channels)."""
        start_time = time.time()
        
        # Increase timeout for large clusters
        if self.num_slaves > 10:
            timeout = max(timeout, self.num_slaves * 5)
        
        while time.time() - start_time < timeout:
            try:
                nodes_data = self.api.list_nodes()
                connected_slaves = nodes_data.get("total", 0)
                nodes = nodes_data.get("nodes", [])
                
                # Count fully connected slaves (both channels)
                fully_connected = sum(
                    1 for node in nodes 
                    if node.get('command_connected') and node.get('health_connected')
                )
                
                if fully_connected >= self.num_slaves:
                    logger.info(f"All slaves fully connected: {[n['hostname'] for n in nodes]}")
                    
                    # If using shared directory, verify slave directories were created
                    if self.shared_dir:
                        self._verify_shared_dir_setup()
                    
                    return
                
                logger.info(f"Waiting for slaves... ({fully_connected}/{self.num_slaves} fully connected)")
                    
            except Exception as e:
                logger.debug(f"Waiting for slaves to connect: {e}")
            
            time.sleep(interval)
        
        # Try to get logs for debugging
        logger.error("Timeout waiting for slaves. Getting logs...")
        if self.master_container:
            master_logs = self.master_container.logs().decode('utf-8')
            logger.error(f"Master logs:\n{master_logs}")
        
        for i, container in enumerate(self.slave_containers):
            slave_logs = container.logs().decode('utf-8')
            logger.error(f"Slave {i} logs:\n{slave_logs}")
        
        raise TimeoutError(f"Slaves did not connect within {timeout}s")
    
    def _verify_shared_dir_setup(self):
        """Verify shared directory setup is correct."""
        if not self.shared_dir:
            return
        
        shared_path = Path(self.shared_dir)
        
        # Check jobs.json exists
        jobs_file = shared_path / "jobs.json"
        if not jobs_file.exists():
            logger.warning("jobs.json not found in shared directory")
            return
        
        # Check slave directories exist (with a small delay for filesystem sync)
        time.sleep(1)
        nodes_data = self.api.list_nodes()
        for node in nodes_data.get("nodes", []):
            hostname = node["hostname"]
            node_dir = shared_path / hostname
            if not node_dir.exists():
                logger.warning(f"Node directory not found for {hostname}")
            else:
                logger.debug(f"Verified node directory for {hostname}")
    
    def teardown(self):
        """Tear down the test cluster."""
        logger.info("Tearing down thunderbolt test cluster...")
        
        # Close API client
        if self.api:
            self.api.close()
        
        # Stop and remove slave containers
        for i, container in enumerate(self.slave_containers):
            try:
                logger.info(f"Stopping slave container {i}...")
                container.stop(timeout=5)
            except Exception as e:
                logger.warning(f"Error stopping slave container {i}: {e}")
        
        # Stop and remove master container
        if self.master_container:
            try:
                logger.info("Stopping master container...")
                self.master_container.stop(timeout=5)
            except Exception as e:
                logger.warning(f"Error stopping master container: {e}")
        
        # Remove network
        if self.network:
            try:
                logger.info("Removing network...")
                self.network.remove()
            except Exception as e:
                logger.warning(f"Error removing network: {e}")
        
        logger.info("Teardown complete")
    
    def get_api(self) -> ThunderboltAPI:
        """Get the Thunderbolt API client."""
        return self.api
    
    def get_slave_logs(self, index: int) -> str:
        """Get logs from a specific slave container."""
        if 0 <= index < len(self.slave_containers):
            return self.slave_containers[index].logs().decode('utf-8')
        return ""
    
    def get_master_logs(self) -> str:
        """Get logs from the master container."""
        if self.master_container:
            return self.master_container.logs().decode('utf-8')
        return ""
    
    def get_all_logs(self) -> dict:
        """Get logs from all containers."""
        logs = {
            "master": self.get_master_logs(),
            "slaves": {}
        }
        
        for i in range(len(self.slave_containers)):
            logs["slaves"][f"slave-{i}"] = self.get_slave_logs(i)
        
        return logs