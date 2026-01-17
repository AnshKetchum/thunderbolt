"""CLI commands for Thunderbolt master and slave."""
import argparse
import sys
import os
from thunderbolt.master import ThunderboltMaster
from thunderbolt.slave import ThunderboltSlave


def master_cli():
    """CLI entry point for thunderbolt-master."""
    parser = argparse.ArgumentParser(
        description="Run Thunderbolt master node",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        "--command-port",
        type=int,
        default=8000,
        help="WebSocket port for command channel"
    )
    parser.add_argument(
        "--health-port",
        type=int,
        default=None,
        help="WebSocket port for health check channel (default: command-port + 100)"
    )
    parser.add_argument(
        "--api-port",
        type=int,
        default=None,
        help="REST API port (default: command-port + 1)"
    )
    parser.add_argument(
        "--host",
        type=str,
        default="0.0.0.0",
        help="Host to bind to"
    )
    parser.add_argument(
        "--health-check-interval",
        type=int,
        default=10,
        help="Seconds between health checks"
    )
    parser.add_argument(
        "--max-failed-healthchecks",
        type=int,
        default=15,
        help="Failed health checks before disconnecting slave"
    )
    parser.add_argument(
        "--max-concurrent-sends",
        type=int,
        default=200,
        help="Maximum concurrent message sends (rate limiting)"
    )
    parser.add_argument(
        "--routes-prefix",
        type=str,
        default=None,
        help="Prefix for API routes (e.g., /api/v1)"
    )
    
    args = parser.parse_args()
    
    # Calculate default ports
    health_port = args.health_port or (args.command_port + 100)
    api_port = args.api_port or (args.command_port + 1)
    
    print("=" * 60)
    print("Starting Thunderbolt Master")
    print("=" * 60)
    print(f"  Command WebSocket Port:  {args.command_port}")
    print(f"  Health WebSocket Port:   {health_port}")
    print(f"  REST API Port:           {api_port}")
    print(f"  Host:                    {args.host}")
    print(f"  Health Check Interval:   {args.health_check_interval}s")
    print(f"  Max Failed Health Checks: {args.max_failed_healthchecks}")
    print(f"  Max Concurrent Sends:    {args.max_concurrent_sends}")
    if args.routes_prefix:
        print(f"  Routes Prefix:           {args.routes_prefix}")
    print("=" * 60)
    print()
    
    master = ThunderboltMaster(
        port=args.command_port,
        health_check_port=health_port,
        health_check_interval=args.health_check_interval,
        max_failed_healthchecks=args.max_failed_healthchecks,
        max_concurrent_sends=args.max_concurrent_sends,
        routes_prefix=args.routes_prefix
    )
    
    try:
        master.run(host=args.host, port=api_port)
    except KeyboardInterrupt:
        print("\n\nShutting down master...")
        sys.exit(0)
    except Exception as e:
        print(f"\n\nError: {e}")
        sys.exit(1)


def slave_cli():
    """CLI entry point for thunderbolt-slave."""
    parser = argparse.ArgumentParser(
        description="Run Thunderbolt slave node",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        "--master",
        type=str,
        required=True,
        help="Master node hostname or IP address"
    )
    parser.add_argument(
        "--command-port",
        type=int,
        default=8000,
        help="Master command channel port"
    )
    parser.add_argument(
        "--health-port",
        type=int,
        default=8100,
        help="Master health check channel port"
    )
    parser.add_argument(
        "--hostname",
        type=str,
        default=None,
        help="Custom hostname (default: system hostname)"
    )
    parser.add_argument(
        "--api-key",
        type=str,
        default=None,
        help="API key for authentication (default: env THUNDERBOLT_API_KEY or 'default-key')"
    )
    parser.add_argument(
        "--reconnect-interval",
        type=int,
        default=5,
        help="Seconds between reconnection attempts"
    )
    parser.add_argument(
        "--max-reconnect-attempts",
        type=int,
        default=-1,
        help="Maximum reconnection attempts, -1 for infinite"
    )
    
    args = parser.parse_args()
    
    # Get API key from args or environment
    api_key = args.api_key or os.getenv("THUNDERBOLT_API_KEY", "default-key")
    
    print("=" * 60)
    print("Starting Thunderbolt Slave")
    print("=" * 60)
    print(f"  Master Host:             {args.master}")
    print(f"  Command Port:            {args.command_port}")
    print(f"  Health Port:             {args.health_port}")
    print(f"  Hostname:                {args.hostname or 'system hostname'}")
    print(f"  API Key:                 {'*' * len(api_key)}")
    print(f"  Reconnect Interval:      {args.reconnect_interval}s")
    if args.max_reconnect_attempts > 0:
        print(f"  Max Reconnect Attempts:  {args.max_reconnect_attempts}")
    else:
        print(f"  Max Reconnect Attempts:  Infinite")
    print("=" * 60)
    print()
    
    slave = ThunderboltSlave(
        master_host=args.master,
        command_port=args.command_port,
        health_port=args.health_port,
        hostname=args.hostname,
        api_key=api_key,
        reconnect_interval=args.reconnect_interval,
        max_reconnect_attempts=args.max_reconnect_attempts
    )
    
    try:
        slave.run()
    except KeyboardInterrupt:
        print("\n\nShutting down slave...")
        sys.exit(0)
    except Exception as e:
        print(f"\n\nError: {e}")
        sys.exit(1)


if __name__ == "__main__":
    print("Use 'thunderbolt-master' or 'thunderbolt-slave' commands")
    sys.exit(1)