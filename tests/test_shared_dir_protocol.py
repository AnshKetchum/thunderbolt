"""
Integration tests for thunderbolt master-slave protocol with shared directory.

This test suite spins up dockerized master and slave containers with a shared
volume mount, tests the shared directory broadcast protocol, and tears down
the infrastructure.
"""

import os
import shutil
import pytest
import docker
import time
import json
from pathlib import Path
from typing import List
import logging
from thunderbolt.api import ThunderboltAPI
from cluster_utils import ThunderboltTestCluster, logger
import tempfile


@pytest.fixture(scope="function")
def thunderbolt_shared_dir_cluster():
    """Pytest fixture that provides a thunderbolt test cluster with shared directory."""
    # Use a temporary directory that will be mounted as shared storage
    import tempfile
    
    shared_dir = tempfile.mkdtemp(prefix="thunderbolt_test_", dir=os.getcwd())
    print(f"Created shared directory: {shared_dir}")
    
    cluster = ThunderboltTestCluster(
        num_slaves=15,  # Use 15 slaves to trigger shared dir threshold (default: 10)
        shared_dir=shared_dir,
        shared_dir_threshold=10
    )
    
    try:
        cluster.setup()
        yield cluster
    finally:
        cluster.teardown()
        # Cleanup shared directory
        import shutil
        shutil.rmtree(shared_dir, ignore_errors=True)
        print(f"Cleaned up shared directory: {shared_dir}")


@pytest.fixture(scope="function")
def thunderbolt_small_cluster_with_shared_dir():
    """Pytest fixture for small cluster (below threshold) with shared dir configured."""
    
    shared_dir = tempfile.mkdtemp(prefix="thunderbolt_test_small_", dir=os.getcwd())
    print(f"Created shared directory: {shared_dir}")
    
    cluster = ThunderboltTestCluster(
        num_slaves=5,  # Below threshold, should use WebSocket
        shared_dir=shared_dir,
        shared_dir_threshold=10
    )
    
    try:
        cluster.setup()
        yield cluster
    finally:
        cluster.teardown()
        shutil.rmtree(shared_dir, ignore_errors=True)
        print(f"Cleaned up shared directory: {shared_dir}")


class TestThunderboltSharedDirectory:
    """Test cases for the thunderbolt shared directory broadcast protocol."""
    
    def test_shared_dir_setup(self, thunderbolt_shared_dir_cluster):
        """
        Test that shared directory is properly initialized.
        
        Verifies:
        1. Shared directory exists
        2. jobs.json file is created
        3. Each slave has its own subdirectory
        """
        cluster = thunderbolt_shared_dir_cluster
        shared_dir = Path(cluster.shared_dir)
        
        print("Test 1: Verifying shared directory setup...")
        print(thunderbolt_shared_dir_cluster.num_slaves, thunderbolt_shared_dir_cluster.shared_dir_threshold)
        
        # Check shared directory exists
        assert shared_dir.exists(), "Shared directory does not exist"
        assert shared_dir.is_dir(), "Shared directory is not a directory"
        
        # Check jobs.json exists
        jobs_file = shared_dir / "jobs.json"
        assert jobs_file.exists(), "jobs.json file not created"
        
        # Verify it's valid JSON
        with open(jobs_file, 'r') as f:
            jobs = json.load(f)
            assert isinstance(jobs, dict), "jobs.json should contain a dict"
        
        print("✓ Shared directory setup verified")
        
        # Wait a bit for slaves to create their directories
        time.sleep(2)
        
        # Check that slave directories were created
        api = cluster.get_api()
        nodes = api.get_node_hostnames()
        
        for hostname in nodes:
            node_dir = shared_dir / hostname
            print(f"Checking node directory: {node_dir}")
            assert node_dir.exists(), f"Node directory not created for {hostname}"
            assert node_dir.is_dir(), f"Node path is not a directory for {hostname}"
        
        print("✓ All slave directories created")
    
    def test_broadcast_mode_execution(self, thunderbolt_shared_dir_cluster):
        """
        Test that commands are executed via shared directory broadcast.
        
        With 15 slaves and threshold of 10, should use shared directory.
        """
        api = thunderbolt_shared_dir_cluster.get_api()
        cluster = thunderbolt_shared_dir_cluster
        
        print("Test: Executing command via broadcast mode...")
        
        # Get all nodes
        all_hostnames = api.get_node_hostnames()
        assert len(all_hostnames) == 15, f"Expected 15 nodes, got {len(all_hostnames)}"
        
        # Execute command (should use shared directory)
        results = api.run_command(
            command="echo 'Broadcast mode test'",
            nodes=all_hostnames,
            timeout=30,
            use_sudo=False
        )
        
        # Verify execution method
        assert results.get("method") == "shared_directory", \
            f"Expected shared_directory method, got {results.get('method')}"
        
        # Verify results
        assert results["total_nodes"] == 15
        assert results["responses_received"] == 15, \
            f"Expected 15 responses, got {results['responses_received']}"
        
        # Check all results are successful
        for hostname, result in results["results"].items():
            print(f"Node {hostname}: success={result.get('success')}")
            assert result.get("success") is True, \
                f"Command failed on {hostname}: {result.get('error')}"
            assert "Broadcast mode test" in result.get("stdout", "")
        
        print("✓ Broadcast mode execution successful")
        
        # Verify cleanup - jobs.json should not have the command anymore
        shared_dir = Path(cluster.shared_dir)
        jobs_file = shared_dir / "jobs.json"
        with open(jobs_file, 'r') as f:
            jobs = json.load(f)
            assert len(jobs) == 0, "jobs.json should be empty after execution"
        
        print("✓ Job cleanup verified")
    
    def test_websocket_mode_below_threshold(self, thunderbolt_small_cluster_with_shared_dir):
        """
        Test that small clusters use WebSocket even with shared dir configured.
        
        With 5 slaves and threshold of 10, should use WebSocket.
        """
        api = thunderbolt_small_cluster_with_shared_dir.get_api()
        
        print("Test: Executing command via WebSocket (below threshold)...")
        
        # Get all nodes
        all_hostnames = api.get_node_hostnames()
        assert len(all_hostnames) == 5, f"Expected 5 nodes, got {len(all_hostnames)}"
        
        # Execute command (should use WebSocket)
        results = api.run_command(
            command="echo 'WebSocket mode test'",
            nodes=all_hostnames,
            timeout=30,
            use_sudo=False
        )
        
        # Verify execution method
        assert results.get("method") == "websocket", \
            f"Expected websocket method, got {results.get('method')}"
        
        # Verify results
        assert results["total_nodes"] == 5
        assert results["responses_received"] == 5
        
        print("✓ WebSocket mode used correctly below threshold")
    
    def test_force_broadcast_mode(self, thunderbolt_small_cluster_with_shared_dir):
        """
        Test forcing broadcast mode even below threshold.
        """
        api = thunderbolt_small_cluster_with_shared_dir.get_api()
        
        print("Test: Forcing broadcast mode...")
        
        # Get all nodes
        all_hostnames = api.get_node_hostnames()
        
        # Force shared directory mode
        results = api.run_command(
            command="echo 'Forced broadcast test'",
            nodes=all_hostnames,
            timeout=30,
            use_sudo=False,
            force_method="shared_dir"
        )
        
        # Verify execution method
        assert results.get("method") == "shared_directory", \
            f"Expected shared_directory method, got {results.get('method')}"
        
        # Verify results
        assert results["responses_received"] == 5
        
        for hostname, result in results["results"].items():
            assert result.get("success") is True
            assert "Forced broadcast test" in result.get("stdout", "")
        
        print("✓ Forced broadcast mode successful")
    
    def test_force_websocket_mode(self, thunderbolt_shared_dir_cluster):
        """
        Test forcing WebSocket mode even above threshold.
        """
        api = thunderbolt_shared_dir_cluster.get_api()
        
        print("Test: Forcing WebSocket mode...")
        
        # Get all nodes
        all_hostnames = api.get_node_hostnames()
        
        # Force WebSocket mode
        results = api.run_command(
            command="echo 'Forced WebSocket test'",
            nodes=all_hostnames,
            timeout=30,
            use_sudo=False,
            force_method="websocket"
        )
        
        # Verify execution method
        assert results.get("method") == "websocket", \
            f"Expected websocket method, got {results.get('method')}"
        
        # Verify results
        assert results["responses_received"] == 15
        
        print("✓ Forced WebSocket mode successful")
    
    def test_batched_commands_broadcast(self, thunderbolt_shared_dir_cluster):
        """
        Test batched commands execution via shared directory.
        """
        api = thunderbolt_shared_dir_cluster.get_api()
        cluster = thunderbolt_shared_dir_cluster
        
        print("Test: Batched commands via broadcast...")
        
        # Get all nodes
        all_hostnames = api.get_node_hostnames()
        
        # Create batched commands - different commands for each node
        commands = []
        for i, hostname in enumerate(all_hostnames):
            commands.append({
                "node": hostname,
                "command": f"echo 'Batch command {i}'",
                "timeout": 10,
                "use_sudo": False
            })
        
        # Execute batched commands
        results = api.run_batched_commands(commands)
        
        # Verify execution method
        assert results.get("method") == "shared_directory", \
            f"Expected shared_directory method for batched"
        
        # Verify results
        assert results["total_commands"] == 15
        assert results["total_nodes"] == 15
        
        # Check each node's results
        for i, hostname in enumerate(all_hostnames):
            assert hostname in results["results"], f"Missing results for {hostname}"
            node_results = results["results"][hostname]
            
            # Check it's not an error dict
            assert "error" not in node_results or "commands" in node_results, \
                f"Node {hostname} had error: {node_results.get('error')}"
            
            # Get command results
            commands_results = node_results.get("commands", [])
            assert len(commands_results) == 1, f"Expected 1 command result for {hostname}"
            
            cmd_result = commands_results[0]
            result = cmd_result.get("result", {})
            
            print(f"Node {hostname} batch result: {result}")
            assert result.get("success") is True, \
                f"Batch command failed on {hostname}: {result.get('error')}"
            assert f"Batch command {i}" in result.get("stdout", "")
        
        print("✓ Batched commands broadcast successful")
        
        # Verify cleanup
        shared_dir = Path(cluster.shared_dir)
        jobs_file = shared_dir / "jobs.json"
        with open(jobs_file, 'r') as f:
            jobs = json.load(f)
            assert len(jobs) == 0, "jobs.json should be empty after batched execution"
        
        print("✓ Batched job cleanup verified")
    
    def test_batched_multiple_commands_per_node(self, thunderbolt_shared_dir_cluster):
        """
        Test multiple batched commands for the same node.
        """
        api = thunderbolt_shared_dir_cluster.get_api()
        
        print("Test: Multiple batched commands per node...")
        
        # Get first 3 nodes
        all_hostnames = api.get_node_hostnames()
        target_nodes = all_hostnames[:3]
        
        # Create multiple commands for each node
        commands = []
        for hostname in target_nodes:
            commands.append({
                "node": hostname,
                "command": "echo 'First command'",
                "timeout": 10,
                "use_sudo": False
            })
            commands.append({
                "node": hostname,
                "command": "echo 'Second command'",
                "timeout": 10,
                "use_sudo": False
            })
            commands.append({
                "node": hostname,
                "command": "echo 'Third command'",
                "timeout": 10,
                "use_sudo": False
            })
        
        # Execute batched commands
        results = api.run_batched_commands(commands)
        
        # Verify results
        assert results["total_commands"] == 9  # 3 nodes * 3 commands
        assert results["total_nodes"] == 3
        
        # Check each node executed all 3 commands
        for hostname in target_nodes:
            assert hostname in results["results"]
            node_results = results["results"][hostname]
            commands_results = node_results.get("commands", [])
            
            assert len(commands_results) == 3, \
                f"Expected 3 commands for {hostname}, got {len(commands_results)}"
            
            # Verify each command
            expected_outputs = ["First command", "Second command", "Third command"]
            for i, cmd_result in enumerate(commands_results):
                result = cmd_result.get("result", {})
                assert result.get("success") is True
                assert expected_outputs[i] in result.get("stdout", "")
        
        print("✓ Multiple batched commands per node successful")
    
    def test_shared_dir_timeout_handling(self, thunderbolt_shared_dir_cluster):
        """
        Test timeout handling in shared directory mode.
        """
        api = thunderbolt_shared_dir_cluster.get_api()
        
        print("Test: Timeout handling in broadcast mode...")
        
        # Get subset of nodes
        all_hostnames = api.get_node_hostnames()
        target_nodes = all_hostnames[:5]
        
        # Execute command with short timeout
        results = api.run_command(
            command="sleep 20",
            nodes=target_nodes,
            timeout=2,  # 2 second timeout for 20 second sleep
            use_sudo=False
        )
        
        # Check that nodes reported timeout
        for hostname, result in results["results"].items():
            print(f"Node {hostname} timeout result: {result}")
            assert result.get("success") is False, \
                f"Expected failure for timeout on {hostname}"
            assert "error" in result
            assert "timed out" in result["error"].lower() or \
                   "timeout" in result["error"].lower()
        
        print("✓ Timeout handling in broadcast mode verified")
    
    def test_shared_dir_error_handling(self, thunderbolt_shared_dir_cluster):
        """
        Test error handling in shared directory mode.
        """
        api = thunderbolt_shared_dir_cluster.get_api()
        
        print("Test: Error handling in broadcast mode...")
        
        # Get all nodes
        all_hostnames = api.get_node_hostnames()
        
        # Execute command that returns error
        results = api.run_command(
            command="exit 42",
            nodes=all_hostnames,
            timeout=10,
            use_sudo=False
        )
        
        # Check that nodes reported non-zero exit code
        for hostname, result in results["results"].items():
            print(f"Node {hostname} error result: {result}")
            assert result.get("success") is True, \
                "Command should execute successfully even with non-zero exit"
            assert result.get("exit_code") == 42, \
                f"Expected exit_code 42, got {result.get('exit_code')}"
        
        print("✓ Error handling in broadcast mode verified")
    
    def test_result_file_cleanup(self, thunderbolt_shared_dir_cluster):
        """
        Test that result files are properly cleaned up after job completion.
        """
        api = thunderbolt_shared_dir_cluster.get_api()
        cluster = thunderbolt_shared_dir_cluster
        shared_dir = Path(cluster.shared_dir)
        
        print("Test: Result file cleanup...")
        
        # Get all nodes
        all_hostnames = api.get_node_hostnames()
        
        # Execute a command
        results = api.run_command(
            command="echo 'Cleanup test'",
            nodes=all_hostnames,
            timeout=10,
            use_sudo=False
        )
        
        # Give a moment for cleanup
        time.sleep(1)
        
        # Check that result files were cleaned up
        for hostname in all_hostnames:
            node_dir = shared_dir / hostname
            result_files = list(node_dir.glob("*.json"))
            
            print(f"Node {hostname} directory has {len(result_files)} result files")
            
            # Should be empty or only contain very recent files
            # (in case of race condition with another test)
            assert len(result_files) == 0, \
                f"Result files not cleaned up for {hostname}: {result_files}"
        
        print("✓ Result file cleanup verified")
    
    def test_concurrent_job_execution(self, thunderbolt_shared_dir_cluster):
        """
        Test that multiple concurrent jobs can execute properly.
        """
        api = thunderbolt_shared_dir_cluster.get_api()
        
        print("Test: Concurrent job execution...")
        
        # Get all nodes
        all_hostnames = api.get_node_hostnames()
        
        # Split nodes into two groups
        group1 = all_hostnames[:8]
        group2 = all_hostnames[8:]
        
        # Execute two commands concurrently
        import concurrent.futures
        
        def execute_command(nodes, msg):
            return api.run_command(
                command=f"echo '{msg}'",
                nodes=nodes,
                timeout=10,
                use_sudo=False
            )
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            future1 = executor.submit(execute_command, group1, "Group 1 test")
            future2 = executor.submit(execute_command, group2, "Group 2 test")
            
            results1 = future1.result()
            results2 = future2.result()
        
        # Verify both jobs completed successfully
        assert results1["responses_received"] == len(group1)
        assert results2["responses_received"] == len(group2)
        
        for hostname, result in results1["results"].items():
            assert result.get("success") is True
            assert "Group 1 test" in result.get("stdout", "")
        
        for hostname, result in results2["results"].items():
            assert result.get("success") is True
            assert "Group 2 test" in result.get("stdout", "")
        
        print("✓ Concurrent job execution successful")
    
    def test_hybrid_operation(self, thunderbolt_shared_dir_cluster):
        """
        Test that slaves can handle both WebSocket and shared dir jobs.
        """
        api = thunderbolt_shared_dir_cluster.get_api()
        
        print("Test: Hybrid operation (WebSocket + Shared Dir)...")
        
        # Get subset of nodes
        all_hostnames = api.get_node_hostnames()
        target_nodes = all_hostnames[:10]
        
        # Execute via WebSocket (forced)
        results_ws = api.run_command(
            command="echo 'WebSocket test'",
            nodes=target_nodes,
            timeout=10,
            use_sudo=False,
            force_method="websocket"
        )
        
        assert results_ws.get("method") == "websocket"
        assert results_ws["responses_received"] == 10
        
        # Execute via shared directory (forced)
        results_sd = api.run_command(
            command="echo 'Shared dir test'",
            nodes=target_nodes,
            timeout=10,
            use_sudo=False,
            force_method="shared_dir"
        )
        
        assert results_sd.get("method") == "shared_directory"
        assert results_sd["responses_received"] == 10
        
        # Verify both worked correctly
        for hostname in target_nodes:
            assert results_ws["results"][hostname].get("success") is True
            assert "WebSocket test" in results_ws["results"][hostname].get("stdout", "")
            
            assert results_sd["results"][hostname].get("success") is True
            assert "Shared dir test" in results_sd["results"][hostname].get("stdout", "")
        
        print("✓ Hybrid operation successful")
    
    def test_partial_node_completion(self, thunderbolt_shared_dir_cluster):
        """
        Test that partial completions are handled correctly.
        
        Some nodes complete, others timeout.
        """
        api = thunderbolt_shared_dir_cluster.get_api()
        
        print("Test: Partial node completion...")
        
        # Get nodes
        all_hostnames = api.get_node_hostnames()
        target_nodes = all_hostnames[:10]
        
        # Create batched commands where some will timeout
        commands = []
        for i, hostname in enumerate(target_nodes):
            if i < 5:
                # Fast commands
                commands.append({
                    "node": hostname,
                    "command": "echo 'Fast command'",
                    "timeout": 10,
                    "use_sudo": False
                })
            else:
                # Slow commands that will timeout
                commands.append({
                    "node": hostname,
                    "command": "sleep 30",
                    "timeout": 2,
                    "use_sudo": False
                })
        
        results = api.run_batched_commands(commands)
        print("Fast commands to ", target_nodes[:5], "slow to", target_nodes[5:])
        
        # Check that we got results from all nodes
        assert len(results["results"]) == 10
        
        # First 5 should succeed
        for hostname in target_nodes[:5]:
            node_results = results["results"][hostname]
            cmd_result = node_results.get("commands", [{}])[0]
            result = cmd_result.get("result", {})
            
            assert result.get("success") is True
            assert "Fast command" in result.get("stdout", "")
        
        # Last 5 should timeout
        for hostname in target_nodes[5:]:
            node_results = results["results"][hostname]
            cmd_result = node_results.get("commands", [{}])[0]
            result = cmd_result.get("result", {})
            
            assert result.get("success") is False
            assert "timed out" in result.get("error", "").lower() or \
                   "timeout" in result.get("error", "").lower()
        
        print("✓ Partial node completion handled correctly")


if __name__ == "__main__":
    # Allow running tests directly with pytest
    pytest.main([__file__, "-v", "-s"])