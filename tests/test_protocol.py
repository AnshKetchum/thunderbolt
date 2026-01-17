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
from cluster_utils import ThunderboltTestCluster, logger

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
        Test basic master-slave communication with dual-channel architecture.
        
        This test verifies that:
        1. Master is running and accessible
        2. Slaves have connected to the master (both channels)
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
        
        # Test 2: Verify slaves are connected (both channels)
        logger.info("Test 2: Verifying slaves are fully connected...")
        nodes_data = api.list_nodes()
        assert nodes_data["total"] == 2, f"Expected 2 slaves, got {nodes_data['total']}"
        
        nodes = nodes_data["nodes"]
        node_hostnames = [node["hostname"] for node in nodes]
        
        # Verify both channels are connected for each node
        for node in nodes:
            assert node["command_connected"], f"Node {node['hostname']} command channel not connected"
            assert node["health_connected"], f"Node {node['hostname']} health channel not connected"
            logger.info(f"✓ Node {node['hostname']}: CMD=✓ HEALTH=✓")
        
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
        assert results.get("failed_sends", 0) == 0, "Expected 0 failed sends"
        
        slave_results = results["results"]
        assert len(slave_results) == 2, f"Expected results from 2 slaves, got {len(slave_results)}"
        
        # Check each slave's result (new format)
        for hostname in node_hostnames:
            assert hostname in slave_results, f"Missing result from {hostname}"
            result = slave_results[hostname]
            
            logger.info(f"Slave {hostname} result: success={result.get('success')}, "
                       f"exit_code={result.get('exit_code')}, "
                       f"stdout={result.get('stdout', '')[:50]}")
            
            # New format uses 'success' boolean instead of 'status' string
            assert result.get("success") is True, \
                f"Slave {hostname} execution failed: {result.get('error', 'Unknown error')}"
            assert result.get("exit_code") == 0, \
                f"Slave {hostname} returned non-zero exit code"
            assert "Hello from thunderbolt test!" in result.get("stdout", ""), \
                f"Unexpected output from slave {hostname}"
        
        logger.info("✓ All tests passed!")
    
    def test_dual_channel_architecture(self, thunderbolt_cluster):
        """
        Test that the dual-channel architecture is working correctly.
        
        Verifies:
        1. Both command and health channels are established
        2. Health checks work independently of command execution
        """
        api = thunderbolt_cluster.get_api()
        
        logger.info("Testing dual-channel architecture...")
        
        # Get node info
        nodes_data = api.list_nodes()
        nodes = nodes_data["nodes"]
        
        # Verify both channels for each node
        for node in nodes:
            hostname = node["hostname"]
            cmd_connected = node.get("command_connected", False)
            health_connected = node.get("health_connected", False)
            failed_checks = node.get("failed_healthchecks", 0)
            
            logger.info(f"Node {hostname}: CMD={cmd_connected}, HEALTH={health_connected}, "
                       f"failed_healthchecks={failed_checks}")
            
            assert cmd_connected, f"Command channel not connected for {hostname}"
            assert health_connected, f"Health channel not connected for {hostname}"
            assert failed_checks == 0, f"Health checks failing for {hostname}"
        
        logger.info("✓ Dual-channel architecture verified")
    
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
        assert results.get("failed_sends", 0) == 0
        assert target_node in results["results"]
        
        # Verify the result is successful (new format)
        result = results["results"][target_node]
        assert result["success"] is True, f"Command failed: {result.get('error')}"
        assert result["exit_code"] == 0
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
        
        # Verify error message contains the node name or 404
        error_message = str(exc_info.value)
        assert "nonexistent-node" in error_message or "404" in error_message, \
            f"Unexpected error message: {error_message}"
        
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
        
        # Check that slaves reported timeout (new format: success=False with error)
        for hostname, result in results["results"].items():
            logger.info(f"Slave {hostname} timeout result: {result}")
            assert result["success"] is False, \
                f"Expected failure for timeout, got success=True"
            assert "error" in result, "Expected error field in timeout result"
            assert "timed out" in result["error"].lower() or "timeout" in result["error"].lower(), \
                f"Expected timeout message in error, got: {result['error']}"
        
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
        
        # Check that slaves reported error (new format: success=True but exit_code != 0)
        for hostname, result in results["results"].items():
            logger.info(f"Slave {hostname} error result: {result}")
            
            # Command executed successfully but returned non-zero exit code
            assert result["success"] is True, \
                "Command should execute successfully even with non-zero exit"
            assert result["exit_code"] == 1, \
                f"Expected exit_code 1, got {result['exit_code']}"
        
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
            use_sudo=False,
            require_fully_connected=True
        )
        
        # Verify we got results from all nodes
        assert results["total_nodes"] == 2
        assert results["responses_received"] == 2
        assert results.get("failed_sends", 0) == 0
        
        # Check all results are successful (new format)
        for hostname, result in results["results"].items():
            assert result["success"] is True, \
                f"Command failed on {hostname}: {result.get('error')}"
            assert "Testing all nodes" in result["stdout"]
        
        logger.info("✓ Run on all nodes successful")
    
    def test_command_summary(self, thunderbolt_cluster):
        """
        Test the command summary helper function.
        """
        api = thunderbolt_cluster.get_api()
        
        logger.info("Testing command summary generation...")
        
        # Execute a command
        results = api.run_on_all_nodes(
            command="echo 'Summary test'",
            timeout=10,
            use_sudo=False
        )
        
        # Generate summary
        summary = api.get_command_summary(results)
        
        logger.info(f"Command summary: {summary}")
        
        # Verify summary fields
        assert summary["total_nodes"] == 2
        assert summary["successful"] == 2
        assert summary["failed"] == 0
        assert summary["timed_out"] == 0
        assert summary["failed_sends"] == 0
        assert summary["success_rate"] == 100.0
        
        logger.info("✓ Command summary generation successful")
    
    def test_master_info_endpoint(self, thunderbolt_cluster):
        """
        Test the master info endpoint that shows port configuration.
        """
        api = thunderbolt_cluster.get_api()
        
        logger.info("Testing master info endpoint...")
        
        info = api.info()
        
        logger.info(f"Master info: {info}")
        
        # Verify info contains expected fields
        assert "message" in info
        assert "connected_slaves" in info
        assert "command_port" in info
        assert "health_check_port" in info
        
        # Verify port values
        assert info["command_port"] == 8000
        assert info["health_check_port"] == 8100
        assert info["connected_slaves"] == 2
        
        logger.info("✓ Master info endpoint working correctly")
    
    def test_get_fully_connected_nodes(self, thunderbolt_cluster):
        """
        Test filtering for fully connected nodes (both channels).
        """
        api = thunderbolt_cluster.get_api()
        
        logger.info("Testing fully connected nodes filter...")
        
        # Get fully connected nodes
        fully_connected = api.get_fully_connected_nodes()
        
        logger.info(f"Fully connected nodes: {fully_connected}")
        
        # All test nodes should be fully connected
        assert len(fully_connected) == 2, \
            f"Expected 2 fully connected nodes, got {len(fully_connected)}"
        
        # Verify they match all nodes (since all should be connected)
        all_nodes = api.get_node_hostnames()
        assert set(fully_connected) == set(all_nodes), \
            "Fully connected nodes should match all nodes in test"
        
        logger.info("✓ Fully connected nodes filter working correctly")


if __name__ == "__main__":
    # Allow running tests directly with pytest
    pytest.main([__file__, "-v", "-s"])