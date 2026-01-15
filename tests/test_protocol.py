"""
Integration tests for thunderbolt master-slave protocol.

This test suite spins up dockerized master and slave containers,
tests the communication protocol, and tears down the infrastructure.
"""

import pytest
import docker
import time
from typing import List
import logging
from thunderbolt.api import ThunderboltAPI

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ThunderboltTestCluster:
    """Manages a test cluster of master and slave containers."""
    
    def __init__(self, num_slaves: int = 2):
        self.client = docker.from_env()
        self.num_slaves = num_slaves
        self.master_container = None
        self.slave_containers: List[docker.models.containers.Container] = []
        self.network = None
        self.websocket_port = 8000
        self.api_port = 8001
        self.master_host = "thunderbolt-master"
        self.api = None
        
    def setup(self):
        """Set up the test cluster with master and slave containers."""
        logger.info("Setting up thunderbolt test cluster...")
        
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
        self.master_container = self.client.containers.run(
            "thunderbolt:test",
            command=[
                "thunderbolt-master",
                "--host", "0.0.0.0",
                "--port", str(self.websocket_port),
                "--api-port", str(self.api_port)
            ],
            name=self.master_host,
            network=network_name,
            ports={
                f"{self.api_port}/tcp": self.api_port  # Expose API port to host
            },
            detach=True,
            remove=True,
            environment={
                "PYTHONUNBUFFERED": "1"
            }
        )
        
        # Wait for master to be ready
        self._wait_for_master()
        logger.info("Master container is ready")
        
        # Initialize API client
        self.api = ThunderboltAPI(host="localhost", port=self.api_port)
        
        # Start slave containers
        for i in range(self.num_slaves):
            slave_name = f"thunderbolt-slave-{i}"
            logger.info(f"Starting slave container: {slave_name}")
            
            slave_container = self.client.containers.run(
                "thunderbolt:test",
                command=[
                    "thunderbolt-slave",
                    "--master-ip", self.master_host,
                    "--port", str(self.websocket_port),
                    "--hostname", slave_name
                ],
                name=slave_name,
                network=network_name,
                detach=True,
                remove=True,
                environment={
                    "PYTHONUNBUFFERED": "1"
                }
            )
            self.slave_containers.append(slave_container)
        
        # Wait for slaves to connect
        self._wait_for_slaves()
        logger.info(f"All {self.num_slaves} slave containers are connected")
        
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
    
    def _wait_for_slaves(self, timeout: int = 30, interval: float = 1.0):
        """Wait for all slaves to connect to master."""
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            try:
                nodes_data = self.api.list_nodes()
                connected_slaves = nodes_data.get("total", 0)
                nodes = nodes_data.get("nodes", [])
                
                if connected_slaves >= self.num_slaves:
                    logger.info(f"All slaves connected: {[n['hostname'] for n in nodes]}")
                    return
                
                logger.info(f"Waiting for slaves... ({connected_slaves}/{self.num_slaves})")
                    
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


@pytest.fixture(scope="function")
def thunderbolt_cluster():
    """Pytest fixture that provides a thunderbolt test cluster."""
    cluster = ThunderboltTestCluster(num_slaves=2)
    
    try:
        cluster.setup()
        yield cluster
    finally:
        cluster.teardown()


class TestThunderboltProtocol:
    """Test cases for the thunderbolt master-slave protocol."""
    
    def test_master_slave_communication(self, thunderbolt_cluster):
        """
        Test basic master-slave communication.
        
        This test verifies that:
        1. Master is running and accessible
        2. Slaves have connected to the master
        3. A dummy command can be executed on slaves
        4. Results are returned successfully
        """
        api = thunderbolt_cluster.get_api()
        
        # Test 1: Verify master is accessible
        logger.info("Test 1: Verifying master is accessible...")
        health_data = api.health()
        assert health_data["status"] == "healthy", "Master not healthy"
        assert health_data["connected_slaves"] == 2, "Expected 2 connected slaves"
        logger.info(f"✓ Master is accessible: {health_data}")
        
        # Test 2: Verify slaves are connected
        logger.info("Test 2: Verifying slaves are connected...")
        nodes_data = api.list_nodes()
        assert nodes_data["total"] == 2, f"Expected 2 slaves, got {nodes_data['total']}"
        
        nodes = nodes_data["nodes"]
        node_hostnames = [node["hostname"] for node in nodes]
        logger.info(f"✓ Connected nodes: {node_hostnames}")
        
        # Test 3: Execute a dummy command on all slaves
        logger.info("Test 3: Executing dummy command on all slaves...")
        results = api.run_command(
            command="echo 'Hello from thunderbolt test!'",
            nodes=node_hostnames,
            timeout=10,
            use_sudo=False
        )
        
        # Test 4: Verify results
        logger.info("Test 4: Verifying command results...")
        assert "results" in results, "Response missing 'results' field"
        assert results["total_nodes"] == 2, "Expected 2 target nodes"
        assert results["responses_received"] == 2, "Expected 2 responses"
        
        slave_results = results["results"]
        assert len(slave_results) == 2, f"Expected results from 2 slaves, got {len(slave_results)}"
        
        # Check each slave's result
        for hostname in node_hostnames:
            assert hostname in slave_results, f"Missing result from {hostname}"
            result = slave_results[hostname]
            
            logger.info(f"Slave {hostname} result: status={result.get('status')}, "
                       f"returncode={result.get('returncode')}, "
                       f"stdout={result.get('stdout', '')[:50]}")
            
            assert result.get("status") == "success", \
                f"Slave {hostname} execution failed: {result}"
            assert result.get("returncode") == 0, \
                f"Slave {hostname} returned non-zero exit code"
            assert "Hello from thunderbolt test!" in result.get("stdout", ""), \
                f"Unexpected output from slave {hostname}"
        
        logger.info("✓ All tests passed!")
    
    def test_command_with_specific_nodes(self, thunderbolt_cluster):
        """
        Test executing a command on specific nodes only.
        """
        api = thunderbolt_cluster.get_api()
        
        # Get all nodes
        all_hostnames = api.get_node_hostnames()
        
        # Execute command on only the first node
        target_node = all_hostnames[0]
        logger.info(f"Executing command on specific node: {target_node}")
        
        results = api.run_command(
            command="echo 'Specific node test'",
            nodes=[target_node],
            timeout=10,
            use_sudo=False
        )
        
        # Should only have one result
        assert results["total_nodes"] == 1
        assert results["responses_received"] == 1
        assert target_node in results["results"]
        
        # Verify the result is successful
        result = results["results"][target_node]
        assert result["status"] == "success"
        assert result["returncode"] == 0
        assert "Specific node test" in result["stdout"]
        
        logger.info("✓ Specific node execution successful")
    
    def test_command_with_nonexistent_node(self, thunderbolt_cluster):
        """
        Test that requesting a non-existent node returns proper error.
        """
        api = thunderbolt_cluster.get_api()
        
        logger.info("Testing command with non-existent node...")
        
        # Should raise an exception for non-existent nodes
        with pytest.raises(Exception) as exc_info:
            api.run_command(
                command="echo test",
                nodes=["nonexistent-node"],
                timeout=10,
                use_sudo=False
            )
        
        # Verify error message contains the node name
        error_message = str(exc_info.value)
        assert "nonexistent-node" in error_message or "404" in error_message
        
        logger.info("✓ Non-existent node handled correctly")
    
    def test_command_timeout(self, thunderbolt_cluster):
        """
        Test that commands that exceed timeout are handled properly.
        """
        api = thunderbolt_cluster.get_api()
        
        # Get all nodes
        all_hostnames = api.get_node_hostnames()
        
        logger.info("Executing command with timeout...")
        
        results = api.run_command(
            command="sleep 10",
            nodes=all_hostnames,
            timeout=2,  # 2 second timeout for a 10 second sleep
            use_sudo=False
        )
        
        # Check that slaves reported timeout
        for hostname, result in results["results"].items():
            logger.info(f"Slave {hostname} timeout result: {result}")
            assert result["status"] == "timeout", \
                f"Expected timeout status, got {result['status']}"
            assert "timed out" in result["stderr"].lower(), \
                "Expected timeout message in stderr"
        
        logger.info("✓ Command timeout handled correctly")
    
    def test_command_with_error(self, thunderbolt_cluster):
        """
        Test execution of a command that returns non-zero exit code.
        """
        api = thunderbolt_cluster.get_api()
        
        # Get all nodes
        all_hostnames = api.get_node_hostnames()
        
        logger.info("Executing command that will fail...")
        
        results = api.run_command(
            command="exit 1",  # Command that returns error
            nodes=all_hostnames,
            timeout=10,
            use_sudo=False
        )
        
        # Check that slaves reported error status
        for hostname, result in results["results"].items():
            logger.info(f"Slave {hostname} error result: {result}")
            assert result["status"] == "error", \
                f"Expected error status, got {result['status']}"
            assert result["returncode"] == 1, \
                f"Expected returncode 1, got {result['returncode']}"
        
        logger.info("✓ Command error handled correctly")
    
    def test_run_on_all_nodes(self, thunderbolt_cluster):
        """
        Test the convenience method to run on all nodes.
        """
        api = thunderbolt_cluster.get_api()
        
        logger.info("Testing run_on_all_nodes convenience method...")
        
        results = api.run_on_all_nodes(
            command="echo 'Testing all nodes'",
            timeout=10,
            use_sudo=False
        )
        
        # Verify we got results from all nodes
        assert results["total_nodes"] == 2
        assert results["responses_received"] == 2
        
        # Check all results are successful
        for hostname, result in results["results"].items():
            assert result["status"] == "success"
            assert "Testing all nodes" in result["stdout"]
        
        logger.info("✓ Run on all nodes successful")


if __name__ == "__main__":
    # Allow running tests directly with pytest
    pytest.main([__file__, "-v", "-s"])