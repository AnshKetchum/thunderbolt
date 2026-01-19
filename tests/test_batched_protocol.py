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
    cluster = ThunderboltTestCluster(num_slaves=2)
    
    try:
        cluster.setup()
        yield cluster
    finally:
        cluster.teardown()


class TestThunderboltBatchedCommands:
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
        results = api.run_batched_commands(commands)
        
        # Verify response structure
        assert results["total_commands"] == 2, "Expected 2 total commands"
        assert results["total_nodes"] == 2, "Expected 2 nodes"
        assert "results" in results, "Missing results field"
        
        # Verify each node's results
        for hostname in all_hostnames:
            assert hostname in results["results"], f"Missing results for {hostname}"
            node_result = results["results"][hostname]
            
            # Should have commands list (not error)
            assert "commands" in node_result, f"Missing commands list for {hostname}"
            assert "error" not in node_result, f"Unexpected error for {hostname}: {node_result.get('error')}"
            
            # Should have exactly 1 command result
            assert len(node_result["commands"]) == 1, \
                f"Expected 1 command for {hostname}, got {len(node_result['commands'])}"
            
            cmd_result = node_result["commands"][0]
            assert "command" in cmd_result, "Missing command field"
            assert "result" in cmd_result, "Missing result field"
            
            # Verify command executed successfully
            result = cmd_result["result"]
            assert result["success"] is True, \
                f"Command failed on {hostname}: {result.get('error')}"
            assert result["exit_code"] == 0, \
                f"Non-zero exit code on {hostname}: {result['exit_code']}"
            assert "Hello from" in result["stdout"], \
                f"Unexpected stdout on {hostname}: {result['stdout']}"
        
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
        results = api.run_batched_commands(commands)
        
        # Verify response structure
        assert results["total_commands"] == 5, "Expected 5 total commands"
        assert results["total_nodes"] == 2, "Expected 2 nodes"
        
        # Verify node1 results (3 commands)
        assert node1 in results["results"], f"Missing results for {node1}"
        node1_result = results["results"][node1]
        assert len(node1_result["commands"]) == 3, \
            f"Expected 3 commands for {node1}, got {len(node1_result['commands'])}"
        
        # Verify all commands on node1 succeeded
        for i, cmd_result in enumerate(node1_result["commands"], 1):
            result = cmd_result["result"]
            assert result["success"] is True, \
                f"Command {i} failed on {node1}: {result.get('error')}"
            assert f"Command {i} on node1" in result["stdout"], \
                f"Unexpected output for command {i} on {node1}"
        
        # Verify node2 results (2 commands)
        assert node2 in results["results"], f"Missing results for {node2}"
        node2_result = results["results"][node2]
        assert len(node2_result["commands"]) == 2, \
            f"Expected 2 commands for {node2}, got {len(node2_result['commands'])}"
        
        # Verify all commands on node2 succeeded
        for i, cmd_result in enumerate(node2_result["commands"], 1):
            result = cmd_result["result"]
            assert result["success"] is True, \
                f"Command {i} failed on {node2}: {result.get('error')}"
            assert f"Command {i} on node2" in result["stdout"], \
                f"Unexpected output for command {i} on {node2}"
        
        logger.info("✓ Multiple commands per node batched execution successful")
    
    # def test_batched_with_sudo(self, thunderbolt_cluster):
    #     """
    #     Test batched execution with sudo commands.
    #     """
    #     api = thunderbolt_cluster.get_api()
        
    #     all_hostnames = api.get_node_hostnames()
    #     node1 = all_hostnames[0]
        
    #     logger.info("Test: Batched commands with sudo...")
        
    #     commands = [
    #         {"node": node1, "command": "whoami", "timeout": 10, "use_sudo": False},
    #         {"node": node1, "command": "whoami", "timeout": 10, "use_sudo": True},
    #     ]
        
    #     results = api.run_batched_commands(commands)
        
    #     # Verify both commands executed
    #     assert results["total_commands"] == 2
    #     node_result = results["results"][node1]
    #     assert len(node_result["commands"]) == 2
        
    #     # Both should succeed
    #     for cmd_result in node_result["commands"]:
    #         result = cmd_result["result"]
    #         assert result["success"] is True, \
    #             f"Command failed: {result.get('error')}"
    #         assert result["exit_code"] == 0
        
    #     logger.info("✓ Batched sudo commands successful")
    
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
        
        results = api.run_batched_commands(commands)
        
        # Verify structure
        assert results["total_commands"] == 3
        node_result = results["results"][node1]
        assert len(node_result["commands"]) == 3
        
        # First command should succeed
        cmd1_result = node_result["commands"][0]["result"]
        assert cmd1_result["success"] is True
        assert "Quick command" in cmd1_result["stdout"]
        
        # Second command should timeout
        cmd2_result = node_result["commands"][1]["result"]
        assert cmd2_result["success"] is False, \
            "Expected timeout to report as failure"
        # assert "timeout" in cmd2_result.get("error", "").lower(), \
        #     f"Expected timeout error, got: {cmd2_result.get('error')}"
        
        # Third command should still execute (sequential execution continues)
        cmd3_result = node_result["commands"][2]["result"]
        assert cmd3_result["success"] is True
        assert "After timeout" in cmd3_result["stdout"]
        
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
        
        results = api.run_batched_commands(commands)
        
        # Verify structure
        node_result = results["results"][node1]
        assert len(node_result["commands"]) == 3
        
        # First command succeeds
        cmd1_result = node_result["commands"][0]["result"]
        assert cmd1_result["success"] is True
        assert cmd1_result["exit_code"] == 0
        
        # Second command executes but returns error code
        cmd2_result = node_result["commands"][1]["result"]
        assert cmd2_result["success"] is True, \
            "Command should execute successfully even with non-zero exit"
        assert cmd2_result["exit_code"] == 1, \
            f"Expected exit code 1, got {cmd2_result['exit_code']}"
        
        # Third command still executes (sequential execution continues)
        cmd3_result = node_result["commands"][2]["result"]
        assert cmd3_result["success"] is True
        assert "After error" in cmd3_result["stdout"]
        
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
        results = api.run_batched_commands(commands)
        
        # Should return empty results
        assert results["total_commands"] == 0
        assert results["total_nodes"] == 0
        assert results["results"] == {}
        
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
        
        results = api.run_batched_commands(commands)
        
        # Verify totals
        assert results["total_commands"] == 5
        assert results["total_nodes"] == 2
        
        # Verify node1 got 3 commands
        node1_result = results["results"][node1]
        assert len(node1_result["commands"]) == 3
        assert "N1-C1" in node1_result["commands"][0]["result"]["stdout"]
        assert "N1-C2" in node1_result["commands"][1]["result"]["stdout"]
        assert "N1-C3" in node1_result["commands"][2]["result"]["stdout"]
        
        # Verify node2 got 2 commands
        node2_result = results["results"][node2]
        assert len(node2_result["commands"]) == 2
        assert "N2-C1" in node2_result["commands"][0]["result"]["stdout"]
        assert "N2-C2" in node2_result["commands"][1]["result"]["stdout"]
        
        logger.info("✓ Complex batched execution successful")
    
    def test_batched_summary_helper(self, thunderbolt_cluster):
        """
        Test the batched command summary helper function.
        """
        api = thunderbolt_cluster.get_api()
        
        all_hostnames = api.get_node_hostnames()
        node1 = all_hostnames[0]
        node2 = all_hostnames[1]
        
        logger.info("Test: Batched command summary generation...")
        
        # Execute some commands with mixed success
        commands = [
            {"node": node1, "command": "echo 'Success 1'", "timeout": 10},
            {"node": node1, "command": "exit 1", "timeout": 10},  # Non-zero exit
            {"node": node2, "command": "echo 'Success 2'", "timeout": 10},
            {"node": node2, "command": "echo 'Success 3'", "timeout": 10},
        ]
        
        results = api.run_batched_commands(commands)
        
        # Generate summary
        summary = api.get_batched_summary(results)
        
        logger.info(f"Batched command summary: {summary}")
        
        # Verify summary fields
        assert summary["total_commands"] == 4
        assert summary["total_nodes"] == 2
        assert summary["successful_commands"] == 4, \
            "All commands should execute successfully (even with non-zero exit)"
        assert summary["failed_commands"] == 0
        assert summary["node_failures"] == 0
        assert summary["success_rate"] == 100.0
        
        logger.info("✓ Batched command summary generation successful")
    
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
        results = api.run_batched_commands(commands)
        elapsed_time = time.time() - start_time
        
        logger.info(f"Elapsed time: {elapsed_time:.2f} seconds")
        
        # Verify both commands succeeded
        for hostname in [node1, node2]:
            node_result = results["results"][hostname]
            assert len(node_result["commands"]) == 1
            assert node_result["commands"][0]["result"]["success"] is True
        
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
        results = api.run_batched_commands(commands)
        elapsed_time = time.time() - start_time
        
        logger.info(f"Elapsed time: {elapsed_time:.2f} seconds")
        
        # Verify all commands succeeded in order
        node_result = results["results"][node1]
        assert len(node_result["commands"]) == 3
        
        for i, cmd_result in enumerate(node_result["commands"], 1):
            result = cmd_result["result"]
            assert result["success"] is True
            assert f"Command {i}" in result["stdout"]
        
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
        
        results = api.run_batched_commands(commands)
        
        node_result = results["results"][node1]
        assert len(node_result["commands"]) == 4
        
        # First two should succeed
        assert node_result["commands"][0]["result"]["success"] is True
        assert node_result["commands"][1]["result"]["success"] is True
        
        # Third should succeed (sleeps 3s, timeout 10s)
        assert node_result["commands"][2]["result"]["success"] is True
        
        # Fourth should timeout (sleeps 10s, timeout 2s)
        cmd4_result = node_result["commands"][3]["result"]
        assert cmd4_result["success"] is False
        # assert "timeout" in cmd4_result.get("error", "").lower()
        
        logger.info("✓ Different timeouts handled correctly")


if __name__ == "__main__":
    # Allow running tests directly with pytest
    pytest.main([__file__, "-v", "-s"])