"""
Integration tests for thunderbolt batched command execution.

This test suite verifies the batched command endpoint that allows
executing different commands on different nodes in parallel.
"""

import pytest
import time
from typing import List
import logging
from thunderbolt.api import ThunderboltAPI
from cluster_utils import ThunderboltTestCluster, logger

@pytest.fixture(scope="function")
def thunderbolt_cluster():
    """Pytest fixture that provides a thunderbolt test cluster."""
    cluster = ThunderboltTestCluster(num_slaves=2, privileged=True)
    
    try:
        cluster.setup()
        yield cluster
    finally:
        cluster.teardown()


class TestThunderboltBatchedCommandsPrivileged:
    """Test cases for the thunderbolt batched command execution."""
    
    def test_batched_single_command_per_node(self, thunderbolt_cluster):
        """
        Test batched execution with one command per node.
        
        This verifies basic batched execution functionality where each
        node receives exactly one command.
        """
        api = thunderbolt_cluster.get_api()
        
        # Get all nodes
        all_hostnames = api.get_node_hostnames()
        assert len(all_hostnames) == 2, "Expected 2 nodes"
        
        logger.info("Test: Single command per node via batched endpoint...")
        
        # Prepare commands - one per node
        commands = [
            {
                "node": all_hostnames[0],
                "command": "echo 'Hello from node 1'",
                "timeout": 10,
                "use_sudo": False
            },
            {
                "node": all_hostnames[1],
                "command": "echo 'Hello from node 2'",
                "timeout": 10,
                "use_sudo": False
            }
        ]
        
        # Execute batched commands
        result = api.run_batched_commands(commands)
        
        # Verify response structure using typed response
        assert result.total_commands == 2, "Expected 2 total commands"
        assert result.total_nodes == 2, "Expected 2 nodes"
        assert len(result.results) == 2, "Expected 2 results"
        
        # Verify each command result in order
        for i, cmd_result in enumerate(result.results):
            expected_hostname = all_hostnames[i]
            expected_msg = f"Hello from node {i + 1}"
            
            logger.info(f"Result {i}: node={cmd_result.node}, "
                       f"exit_code={cmd_result.exit_code}")
            
            assert cmd_result.node == expected_hostname, \
                f"Expected node {expected_hostname}, got {cmd_result.node}"
            assert cmd_result.exit_code == 0, \
                f"Expected exit code 0, got {cmd_result.exit_code}"
            assert expected_msg in (cmd_result.stdout or ""), \
                f"Expected '{expected_msg}' in stdout"
        
        logger.info("✓ Single command per node batched execution successful")
    
    def test_batched_multiple_commands_per_node(self, thunderbolt_cluster):
        """
        Test batched execution with multiple commands per node.
        
        Verifies that commands on the same node are executed sequentially
        while different nodes execute in parallel.
        """
        api = thunderbolt_cluster.get_api()
        
        all_hostnames = api.get_node_hostnames()
        node1 = all_hostnames[0]
        node2 = all_hostnames[1]
        
        logger.info("Test: Multiple commands per node via batched endpoint...")
        
        # Prepare commands - multiple per node
        commands = [
            {"node": node1, "command": "echo 'Command 1 on node1'", "timeout": 10},
            {"node": node1, "command": "echo 'Command 2 on node1'", "timeout": 10},
            {"node": node1, "command": "echo 'Command 3 on node1'", "timeout": 10},
            {"node": node2, "command": "echo 'Command 1 on node2'", "timeout": 10},
            {"node": node2, "command": "echo 'Command 2 on node2'", "timeout": 10},
        ]
        
        # Execute batched commands
        result = api.run_batched_commands(commands)
        
        # Verify response structure using typed response
        assert result.total_commands == 5, "Expected 5 total commands"
        assert result.total_nodes == 2, "Expected 2 nodes"
        assert len(result.results) == 5, "Expected 5 results"
        
        # Verify order preservation - commands should be in same order as input
        expected_outputs = [
            (node1, "Command 1 on node1"),
            (node1, "Command 2 on node1"),
            (node1, "Command 3 on node1"),
            (node2, "Command 1 on node2"),
            (node2, "Command 2 on node2"),
        ]
        
        for i, (expected_node, expected_msg) in enumerate(expected_outputs):
            cmd_result = result.results[i]
            
            assert cmd_result.node == expected_node, \
                f"Command {i}: expected node {expected_node}, got {cmd_result.node}"
            assert cmd_result.exit_code == 0, \
                f"Command {i} failed with exit code {cmd_result.exit_code}"
            assert expected_msg in (cmd_result.stdout or ""), \
                f"Command {i}: expected '{expected_msg}' in stdout"
        
        logger.info("✓ Multiple commands per node batched execution successful")
    
    def test_batched_with_sudo(self, thunderbolt_cluster):
        """
        Test batched execution with sudo commands.
        """
        api = thunderbolt_cluster.get_api()
        
        all_hostnames = api.get_node_hostnames()
        node1 = all_hostnames[0]
        
        logger.info("Test: Batched commands with sudo...")
        
        commands = [
            {"node": node1, "command": "whoami", "timeout": 10, "use_sudo": False},
            {"node": node1, "command": "whoami", "timeout": 10, "use_sudo": True},
        ]
        
        result = api.run_batched_commands(commands)
        
        # Verify both commands executed using typed response
        assert result.total_commands == 2
        assert len(result.results) == 2
        
        # Both should succeed
        for cmd_result in result.results:
            assert cmd_result.exit_code == 0, \
                f"Command failed: {cmd_result.error}"
        
        logger.info("✓ Batched sudo commands successful")
    
    def test_batched_with_timeouts(self, thunderbolt_cluster):
        """
        Test batched execution with commands that timeout.
        """
        api = thunderbolt_cluster.get_api()
        
        all_hostnames = api.get_node_hostnames()
        node1 = all_hostnames[0]
        
        logger.info("Test: Batched commands with timeout...")
        
        commands = [
            {"node": node1, "command": "echo 'Quick command'", "timeout": 10},
            {"node": node1, "command": "sleep 10", "timeout": 2},  # Will timeout
            {"node": node1, "command": "echo 'After timeout'", "timeout": 10},
        ]
        
        result = api.run_batched_commands(commands)
        
        # Verify structure using typed response
        assert result.total_commands == 3
        assert len(result.results) == 3
        
        # First command should succeed
        assert result.results[0].exit_code == 0
        assert "Quick command" in (result.results[0].stdout or "")
        
        # Second command should timeout
        assert result.results[1].error or result.results[1].timed_out, \
            "Expected timeout to be reported"
        if result.results[1].error:
            assert "timeout" in result.results[1].error.lower(), \
                f"Expected timeout error, got: {result.results[1].error}"
        
        # Third command should still execute (sequential execution continues)
        assert result.results[2].exit_code == 0
        assert "After timeout" in (result.results[2].stdout or "")
        
        logger.info("✓ Batched timeout handling successful")
    
    def test_batched_with_command_errors(self, thunderbolt_cluster):
        """
        Test batched execution with commands that return non-zero exit codes.
        """
        api = thunderbolt_cluster.get_api()
        
        all_hostnames = api.get_node_hostnames()
        node1 = all_hostnames[0]
        
        logger.info("Test: Batched commands with errors...")
        
        commands = [
            {"node": node1, "command": "echo 'Success'", "timeout": 10},
            {"node": node1, "command": "exit 1", "timeout": 10},  # Error
            {"node": node1, "command": "echo 'After error'", "timeout": 10},
        ]
        
        result = api.run_batched_commands(commands)
        
        # Verify structure using typed response
        assert len(result.results) == 3
        
        # First command succeeds
        assert result.results[0].exit_code == 0
        
        # Second command executes but returns error code
        assert result.results[1].exit_code == 1, \
            f"Expected exit code 1, got {result.results[1].exit_code}"
        
        # Third command still executes (sequential execution continues)
        assert result.results[2].exit_code == 0
        assert "After error" in (result.results[2].stdout or "")
        
        logger.info("✓ Batched error handling successful")
    
    def test_batched_with_nonexistent_node(self, thunderbolt_cluster):
        """
        Test that batched commands fail properly with non-existent nodes.
        """
        api = thunderbolt_cluster.get_api()
        
        logger.info("Test: Batched commands with non-existent node...")
        
        commands = [
            {"node": "nonexistent-node", "command": "echo test", "timeout": 10}
        ]
        
        # Should raise an exception for non-existent nodes
        with pytest.raises(Exception) as exc_info:
            api.run_batched_commands(commands)
        
        # Verify error message
        error_message = str(exc_info.value)
        assert "nonexistent-node" in error_message or "404" in error_message, \
            f"Unexpected error message: {error_message}"
        
        logger.info("✓ Non-existent node in batched commands handled correctly")
    
    def test_batched_empty_commands(self, thunderbolt_cluster):
        """
        Test batched execution with empty command list.
        """
        api = thunderbolt_cluster.get_api()
        
        logger.info("Test: Batched commands with empty list...")
        
        commands = []
        result = api.run_batched_commands(commands)
        
        # Should return empty results using typed response
        assert result.total_commands == 0
        assert result.total_nodes == 0
        assert len(result.results) == 0
        
        logger.info("✓ Empty batched commands handled correctly")
    
    def test_batched_mixed_nodes_and_commands(self, thunderbolt_cluster):
        """
        Test complex batched execution with different commands on different nodes.
        """
        api = thunderbolt_cluster.get_api()
        
        all_hostnames = api.get_node_hostnames()
        node1 = all_hostnames[0]
        node2 = all_hostnames[1]
        
        logger.info("Test: Complex batched commands across nodes...")
        
        # Mix of commands across both nodes
        commands = [
            {"node": node1, "command": "echo 'N1-C1'", "timeout": 10},
            {"node": node2, "command": "echo 'N2-C1'", "timeout": 10},
            {"node": node1, "command": "echo 'N1-C2'", "timeout": 10},
            {"node": node2, "command": "echo 'N2-C2'", "timeout": 10},
            {"node": node1, "command": "echo 'N1-C3'", "timeout": 10},
        ]
        
        result = api.run_batched_commands(commands)
        
        # Verify totals using typed response
        assert result.total_commands == 5
        assert result.total_nodes == 2
        assert len(result.results) == 5
        
        # Verify order preservation - results should match input order
        expected = [
            (node1, "N1-C1"),
            (node2, "N2-C1"),
            (node1, "N1-C2"),
            (node2, "N2-C2"),
            (node1, "N1-C3"),
        ]
        
        for i, (expected_node, expected_msg) in enumerate(expected):
            cmd_result = result.results[i]
            
            assert cmd_result.node == expected_node, \
                f"Result {i}: expected node {expected_node}, got {cmd_result.node}"
            assert expected_msg in (cmd_result.stdout or ""), \
                f"Result {i}: expected '{expected_msg}' in stdout"
        
        logger.info("✓ Complex batched execution successful")
    
    def test_batched_parallel_execution_timing(self, thunderbolt_cluster):
        """
        Test that batched commands execute in parallel across nodes.
        
        This test verifies that commands on different nodes run simultaneously,
        not sequentially.
        """
        api = thunderbolt_cluster.get_api()
        
        all_hostnames = api.get_node_hostnames()
        node1 = all_hostnames[0]
        node2 = all_hostnames[1]
        
        logger.info("Test: Parallel execution timing...")
        
        # Each node sleeps for 2 seconds
        # If truly parallel, total time should be ~2 seconds
        # If sequential, it would be ~4 seconds
        commands = [
            {"node": node1, "command": "sleep 2", "timeout": 5},
            {"node": node2, "command": "sleep 2", "timeout": 5},
        ]
        
        start_time = time.time()
        result = api.run_batched_commands(commands)
        elapsed_time = time.time() - start_time
        
        logger.info(f"Elapsed time: {elapsed_time:.2f} seconds")
        
        # Verify both commands succeeded using typed response
        assert len(result.results) == 2
        for cmd_result in result.results:
            assert cmd_result.exit_code == 0 or not cmd_result.error
        
        # Should take roughly 2 seconds (parallel), not 4 (sequential)
        # Allow some overhead for network/processing
        assert elapsed_time < 3.5, \
            f"Expected parallel execution (~2s), but took {elapsed_time:.2f}s"
        assert elapsed_time >= 2.0, \
            f"Execution too fast ({elapsed_time:.2f}s), commands may not have run"
        
        logger.info("✓ Parallel execution timing verified")
    
    def test_batched_sequential_per_node(self, thunderbolt_cluster):
        """
        Test that multiple commands on the same node execute sequentially.
        """
        api = thunderbolt_cluster.get_api()
        
        all_hostnames = api.get_node_hostnames()
        node1 = all_hostnames[0]
        
        logger.info("Test: Sequential execution per node...")
        
        # Multiple commands on the same node, each sleeping 1 second
        # Should take ~3 seconds total for this node
        commands = [
            {"node": node1, "command": "sleep 1 && echo 'Command 1'", "timeout": 5},
            {"node": node1, "command": "sleep 1 && echo 'Command 2'", "timeout": 5},
            {"node": node1, "command": "sleep 1 && echo 'Command 3'", "timeout": 5},
        ]
        
        start_time = time.time()
        result = api.run_batched_commands(commands)
        elapsed_time = time.time() - start_time
        
        logger.info(f"Elapsed time: {elapsed_time:.2f} seconds")
        
        # Verify all commands succeeded in order using typed response
        assert len(result.results) == 3
        
        for i, cmd_result in enumerate(result.results, 1):
            assert cmd_result.exit_code == 0
            assert f"Command {i}" in (cmd_result.stdout or "")
        
        # Should take roughly 3 seconds (sequential)
        assert elapsed_time >= 3.0, \
            f"Execution too fast ({elapsed_time:.2f}s), commands may have run in parallel"
        assert elapsed_time < 4.5, \
            f"Execution too slow ({elapsed_time:.2f}s)"
        
        logger.info("✓ Sequential per-node execution verified")
    
    def test_batched_different_timeouts(self, thunderbolt_cluster):
        """
        Test batched commands with different timeout values.
        """
        api = thunderbolt_cluster.get_api()
        
        all_hostnames = api.get_node_hostnames()
        node1 = all_hostnames[0]
        
        logger.info("Test: Batched commands with different timeouts...")
        
        commands = [
            {"node": node1, "command": "echo 'Short timeout'", "timeout": 5},
            {"node": node1, "command": "echo 'Long timeout'", "timeout": 30},
            {"node": node1, "command": "sleep 3", "timeout": 10},  # Will succeed
            {"node": node1, "command": "sleep 10", "timeout": 2},  # Will timeout
        ]
        
        result = api.run_batched_commands(commands)
        
        assert len(result.results) == 4
        
        # First two should succeed
        assert result.results[0].exit_code == 0
        assert result.results[1].exit_code == 0
        
        # Third should succeed (sleeps 3s, timeout 10s)
        assert result.results[2].exit_code == 0
        
        # Fourth should timeout (sleeps 10s, timeout 2s)
        assert result.results[3].error or result.results[3].timed_out, \
            "Expected timeout for 4th command"
        if result.results[3].error:
            assert "timeout" in result.results[3].error.lower()
        
        logger.info("✓ Different timeouts handled correctly")


if __name__ == "__main__":
    # Allow running tests directly with pytest
    pytest.main([__file__, "-v", "-s"])