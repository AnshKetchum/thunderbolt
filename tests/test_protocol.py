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
        results = api.run_command(
            command="echo 'Hello from thunderbolt test!'",
            nodes=node_hostnames,
            timeout=10,
            use_sudo=False
        )
        
        # Test 4: Verify results
        logger.info("Test 4: Verifying command results...")
        assert len(results) == 2, f"Expected results from 2 slaves, got {len(results)}"
        
        # Check each slave's result
        for result in results:
            logger.info(f"Slave {result.node} result: error={result.error}, "
                       f"exit_code={result.exit_code}, "
                       f"stdout={result.stdout[:50] if result.stdout else ''}")
            
            assert result.error is None, \
                f"Slave {result.node} execution failed: {result.error}"
            assert result.exit_code == 0, \
                f"Slave {result.node} returned non-zero exit code"
            assert "Hello from thunderbolt test!" in (result.stdout or ""), \
                f"Unexpected output from slave {result.node}"
        
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
        
        results = api.run_command(
            command="echo 'Specific node test'",
            nodes=[target_node],
            timeout=10,
            use_sudo=False
        )
        
        # Should only have one result
        assert len(results) == 1, f"Expected 1 result, got {len(results)}"
        
        result = results[0]
        assert result.node == target_node, f"Expected result from {target_node}, got {result.node}"
        
        # Verify the result is successful
        assert result.error is None, f"Command failed: {result.error}"
        assert result.exit_code == 0, f"Expected exit code 0, got {result.exit_code}"
        assert "Specific node test" in (result.stdout or ""), \
            f"Expected output not found in stdout: {result.stdout}"
        
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
        
        # Check that slaves reported timeout
        for result in results:
            logger.info(f"Slave {result.node} timeout result: error={result.error}, "
                       f"timed_out={result.timed_out}")
            assert result.error is not None, \
                f"Expected error for timeout on {result.node}"
            assert result.timed_out is True, \
                f"Expected timed_out=True for {result.node}, got {result.timed_out}"
            assert "timed out" in result.error.lower() or "timeout" in result.error.lower(), \
                f"Expected timeout message in error, got: {result.error}"
        
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
        
        # Check that slaves reported error
        for result in results:
            logger.info(f"Slave {result.node} error result: exit_code={result.exit_code}, "
                       f"error={result.error}")
            
            # Command executed successfully but returned non-zero exit code
            assert result.error is None, \
                f"Command should execute without error even with non-zero exit on {result.node}"
            assert result.exit_code == 1, \
                f"Expected exit_code 1 on {result.node}, got {result.exit_code}"
        
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
        assert len(results) == 2, f"Expected 2 results, got {len(results)}"
        
        # Check all results are successful
        for result in results:
            assert result.error is None, \
                f"Command failed on {result.node}: {result.error}"
            assert "Testing all nodes" in (result.stdout or ""), \
                f"Expected output not found on {result.node}"
        
        logger.info("✓ Run on all nodes successful")
    
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
    
    def test_batched_commands(self, thunderbolt_cluster):
        """
        Test executing batched commands on different nodes.
        """
        api = thunderbolt_cluster.get_api()
        
        logger.info("Testing batched commands...")
        
        # Get all nodes
        all_hostnames = api.get_node_hostnames()
        
        # Create batched commands
        commands = [
            {"node": all_hostnames[0], "command": "echo 'First command'", "timeout": 10},
            {"node": all_hostnames[0], "command": "echo 'Second command'", "timeout": 10},
            {"node": all_hostnames[1], "command": "echo 'Third command'", "timeout": 10},
        ]
        
        results = api.run_batched_commands(commands)
        
        # Verify we got results in the same order
        assert len(results) == 3, f"Expected 3 results, got {len(results)}"
        
        # Check results match input order
        assert results[0].node == all_hostnames[0]
        assert "First command" in (results[0].stdout or "")
        
        assert results[1].node == all_hostnames[0]
        assert "Second command" in (results[1].stdout or "")
        
        assert results[2].node == all_hostnames[1]
        assert "Third command" in (results[2].stdout or "")
        
        # Verify all succeeded
        for result in results:
            assert result.error is None, f"Command failed on {result.node}: {result.error}"
            assert result.exit_code == 0, f"Non-zero exit code on {result.node}"
        
        logger.info("✓ Batched commands successful")