#!/usr/bin/env python3
"""
Test script to launch many dummy echo commands to a single node.
"""
import time
from thunderbolt import ThunderboltAPI


def test_many_commands(num_commands: int = 100):
    """
    Launch many echo commands to cnode-95 using batched execution.
    
    Args:
        num_commands: Number of commands to send
    """
    api = ThunderboltAPI(host="localhost", port=8001)
    
    print(f"Launching {num_commands} echo commands to cnode-95...")
    
    # Build batched command list
    commands = []
    for i in range(num_commands):
        commands.append({
            "node": "cnode-95",
            "command": f"echo 'Command {i}: Hello from test script'",
            "timeout": 30,
            "use_sudo": False
        })
    
    # Execute all commands
    start_time = time.time()
    result = api.run_batched_commands(commands)
    elapsed = time.time() - start_time
    
    # Get summary
    summary = api.get_batched_summary(result)
    
    print(f"\n{'='*60}")
    print(f"Execution completed in {elapsed:.2f} seconds")
    print(f"{'='*60}")
    print(f"Total commands:       {summary['total_commands']}")
    print(f"Successful commands:  {summary['successful_commands']}")
    print(f"Failed commands:      {summary['failed_commands']}")
    print(f"Node failures:        {summary['node_failures']}")
    print(f"Success rate:         {summary['success_rate']:.1f}%")
    print(f"Avg time per command: {elapsed / num_commands:.3f}s")
    print(f"{'='*60}\n")
    
    # Show first few results
    if "cnode-95" in result['results']:
        node_result = result['results']['cnode-95']
        if 'commands' in node_result:
            print(f"First 5 command results from cnode-95:")
            for i, cmd_result in enumerate(node_result['commands'][:5]):
                cmd = cmd_result['command']
                res = cmd_result['result']
                if res.get('success'):
                    stdout = res['stdout'].strip()
                    print(f"  [{i}] SUCCESS: {cmd[:50]}... -> {stdout}")
                else:
                    error = res.get('error', 'Unknown error')
                    print(f"  [{i}] FAILED: {cmd[:50]}... -> {error}")
        elif 'error' in node_result:
            print(f"Node-level error: {node_result['error']}")
    
    api.close()


def test_parallel_single_commands(num_commands: int = 50):
    """
    Launch many echo commands as individual parallel requests (not batched).
    This tests WebSocket throughput differently than batched execution.
    
    Args:
        num_commands: Number of commands to send
    """
    api = ThunderboltAPI(host="localhost", port=8001)
    
    print(f"\nLaunching {num_commands} parallel echo commands to cnode-95...")
    
    # Build command list (same command repeated to single node)
    start_time = time.time()
    
    results = []
    for i in range(num_commands):
        result = api.run_command(
            command=f"echo 'Parallel command {i}'",
            nodes=["cnode-95"],
            timeout=30
        )
        results.append(result)
    
    elapsed = time.time() - start_time
    
    # Count successes
    successful = sum(1 for r in results if r['responses_received'] > 0 
                     and list(r['results'].values())[0].get('success'))
    
    print(f"\n{'='*60}")
    print(f"Parallel execution completed in {elapsed:.2f} seconds")
    print(f"{'='*60}")
    print(f"Total commands:       {num_commands}")
    print(f"Successful commands:  {successful}")
    print(f"Failed commands:      {num_commands - successful}")
    print(f"Success rate:         {successful / num_commands * 100:.1f}%")
    print(f"Avg time per command: {elapsed / num_commands:.3f}s")
    print(f"{'='*60}\n")
    
    api.close()


def test_force_methods(num_commands: int = 20):
    """
    Test both websocket and shared_dir methods with echo commands.
    
    Args:
        num_commands: Number of commands to send per method
    """
    api = ThunderboltAPI(host="localhost", port=8001)
    
    # Build command list
    commands = [
        {
            "node": "cnode-95",
            "command": f"echo 'Test command {i}'",
            "timeout": 30
        }
        for i in range(num_commands)
    ]
    
    print(f"\nTesting both execution methods with {num_commands} commands each...\n")
    
    # Test WebSocket method
    print("Testing WebSocket method...")
    start_ws = time.time()
    result_ws = api.run_batched_commands(commands, force_method="websocket")
    elapsed_ws = time.time() - start_ws
    summary_ws = api.get_batched_summary(result_ws)
    
    print(f"  WebSocket: {elapsed_ws:.3f}s, "
          f"{summary_ws['successful_commands']}/{summary_ws['total_commands']} successful")
    
    # Test Shared Dir method (if available)
    print("Testing Shared Directory method...")
    try:
        start_sd = time.time()
        result_sd = api.run_batched_commands(commands, force_method="shared_dir")
        elapsed_sd = time.time() - start_sd
        summary_sd = api.get_batched_summary(result_sd)
        
        print(f"  Shared Dir: {elapsed_sd:.3f}s, "
              f"{summary_sd['successful_commands']}/{summary_sd['total_commands']} successful")
        
        print(f"\nSpeedup: {elapsed_ws / elapsed_sd:.2f}x "
              f"({'Shared Dir' if elapsed_sd < elapsed_ws else 'WebSocket'} faster)")
    except Exception as e:
        print(f"  Shared Dir: Not available or failed - {e}")
    
    api.close()


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        num = int(sys.argv[1])
    else:
        num = 100
    
    print(f"Thunderbolt API Test Script")
    print(f"Target: cnode-95")
    print(f"API: localhost:8001\n")
    
    # Test 1: Batched execution (recommended for many commands)
    test_many_commands(num_commands=num)
    
    # Test 2: Parallel individual commands (alternative approach)
    # test_parallel_single_commands(num_commands=50)
    
    # Test 3: Compare execution methods
    test_force_methods(num_commands=1000)