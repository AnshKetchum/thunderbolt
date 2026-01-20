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
        health = api.health()
        print("Health status ", health)
        assert health.status == "healthy", "Master not healthy"
        assert health.connected_slaves == 2, "Expected 2 connected slaves"
        logger.info(f"✓ Master is accessible: {health.dict()}")
        
        # Test 2: Verify slaves are connected (both channels)
        logger.info("Test 2: Verifying slaves are fully connected...")
        nodes_response = api.list_nodes()
        assert nodes_response.total == 2, f"Expected 2 slaves, got {nodes_response.total}"
        
        node_hostnames = [node.hostname for node in nodes_response.nodes]
        
        # Verify both channels are connected for each node
        for node in nodes_response.nodes:
            assert node.command_connected, f"Node {node.hostname} command channel not connected"
            assert node.health_connected, f"Node {node.hostname} health channel not connected"
            logger.info(f"✓ Node {node.hostname}: CMD=✓ HEALTH=✓")
        
        logger.info(f"✓ Connected nodes: {node_hostnames}")
        
        # Test 3: Execute a dummy command on all slaves
        logger.info("Test 3: Executing dummy command on all slaves...")
        result = api.run_command(
            command="echo 'Hello from thunderbolt test!'",
            nodes=node_hostnames,
            timeout=10,
            use_sudo=False
        )
        
        # Test 4: Verify results
        logger.info("Test 4: Verifying command results...")
        assert result.total_nodes == 2, "Expected 2 target nodes"
        assert result.responses_received == 2, "Expected 2 responses"
        assert result.failed_sends == 0, "Expected 0 failed sends"
        
        assert len(result.results) == 2, f"Expected results from 2 slaves, got {len(result.results)}"
        
        # Check each slave's result
        for hostname in node_hostnames:
            assert hostname in result.results, f"Missing result from {hostname}"
            slave_result = result.results[hostname]
            
            logger.info(f"Slave {hostname} result: success={slave_result.get('success')}, "
                       f"exit_code={slave_result.get('exit_code')}, "
                       f"stdout={slave_result.get('stdout', '')[:50]}")
            
            assert slave_result.get("success") is True, \
                f"Slave {hostname} execution failed: {slave_result.get('error', 'Unknown error')}"
            assert slave_result.get("exit_code") == 0, \
                f"Slave {hostname} returned non-zero exit code"
            assert "Hello from thunderbolt test!" in slave_result.get("stdout", ""), \
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
        nodes_response = api.list_nodes()
        
        # Verify both channels for each node
        for node in nodes_response.nodes:
            logger.info(f"Node {node.hostname}: CMD={node.command_connected}, "
                       f"HEALTH={node.health_connected}, "
                       f"failed_healthchecks={node.failed_healthchecks}")
            
            assert node.command_connected, f"Command channel not connected for {node.hostname}"
            assert node.health_connected, f"Health channel not connected for {node.hostname}"
            assert node.failed_healthchecks == 0, f"Health checks failing for {node.hostname}"
        
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
        
        result = api.run_command(
            command="echo 'Specific node test'",
            nodes=[target_node],
            timeout=10,
            use_sudo=False
        )
        
        # Should only have one result
        assert result.total_nodes == 1
        assert result.responses_received == 1
        assert result.failed_sends == 0
        assert target_node in result.results
        
        # Verify the result is successful
        node_result = result.results[target_node]
        assert node_result["success"] is True, f"Command failed: {node_result.get('error')}"
        assert node_result["exit_code"] == 0
        assert "Specific node test" in node_result["stdout"]
        
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
        
        result = api.run_command(
            command="sleep 10",
            nodes=all_hostnames,
            timeout=2,  # 2 second timeout for a 10 second sleep
            use_sudo=False
        )
        
        # Check that slaves reported timeout
        for hostname, node_result in result.results.items():
            logger.info(f"Slave {hostname} timeout result: {node_result}")
            assert node_result["success"] is False, \
                f"Expected failure for timeout, got success=True"
            assert "error" in node_result, "Expected error field in timeout result"
            assert "timed out" in node_result["error"].lower() or "timeout" in node_result["error"].lower(), \
                f"Expected timeout message in error, got: {node_result['error']}"
        
        logger.info("✓ Command timeout handled correctly")
    
    def test_command_with_error(self, thunderbolt_cluster):
        """
        Test execution of a command that returns non-zero exit code.
        """
        api = thunderbolt_cluster.get_api()
        
        # Get all nodes
        all_hostnames = api.get_node_hostnames()
        
        logger.info("Executing command that will fail...")
        
        result = api.run_command(
            command="exit 1",  # Command that returns error
            nodes=all_hostnames,
            timeout=10,
            use_sudo=False
        )
        
        # Check that slaves reported error
        for hostname, node_result in result.results.items():
            logger.info(f"Slave {hostname} error result: {node_result}")
            
            # Command executed successfully but returned non-zero exit code
            assert node_result["success"] is True, \
                "Command should execute successfully even with non-zero exit"
            assert node_result["exit_code"] == 1, \
                f"Expected exit_code 1, got {node_result['exit_code']}"
        
        logger.info("✓ Command error handled correctly")
    
    def test_run_on_all_nodes(self, thunderbolt_cluster):
        """
        Test the convenience method to run on all nodes.
        """
        api = thunderbolt_cluster.get_api()
        
        logger.info("Testing run_on_all_nodes convenience method...")
        
        result = api.run_on_all_nodes(
            command="echo 'Testing all nodes'",
            timeout=10,
            use_sudo=False,
            require_fully_connected=True
        )
        
        # Verify we got results from all nodes
        assert result.total_nodes == 2
        assert result.responses_received == 2
        assert result.failed_sends == 0
        
        # Check all results are successful
        for hostname, node_result in result.results.items():
            assert node_result["success"] is True, \
                f"Command failed on {hostname}: {node_result.get('error')}"
            assert "Testing all nodes" in node_result["stdout"]
        
        logger.info("✓ Run on all nodes successful")
    
    def test_command_summary(self, thunderbolt_cluster):
        """
        Test the command summary helper function.
        """
        api = thunderbolt_cluster.get_api()
        
        logger.info("Testing command summary generation...")
        
        # Execute a command
        result = api.run_on_all_nodes(
            command="echo 'Summary test'",
            timeout=10,
            use_sudo=False
        )
        
        # Generate summary
        summary = api.get_command_summary(result)
        
        logger.info(f"Command summary: {summary.dict()}")
        
        # Verify summary fields using typed attributes
        assert summary.total_nodes == 2
        assert summary.successful == 2
        assert summary.failed == 0
        assert summary.timed_out == 0
        assert summary.failed_sends == 0
        assert summary.success_rate == 100.0
        
        logger.info("✓ Command summary generation successful")
    
    def test_master_info_endpoint(self, thunderbolt_cluster):
        """
        Test the master info endpoint that shows port configuration.
        """
        api = thunderbolt_cluster.get_api()
        
        logger.info("Testing master info endpoint...")
        
        info = api.info()
        
        logger.info(f"Master info: {info.dict()}")
        
        # Verify info contains expected fields using typed attributes
        assert info.message is not None
        assert info.connected_slaves == 2
        assert info.command_port == 8000
        assert info.health_check_port == 8100
        
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