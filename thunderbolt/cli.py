"""CLI commands for Thunderbolt master and slave."""
import argparse
import sys
from thunderbolt.master import ThunderboltMaster
from thunderbolt.slave import ThunderboltSlave


def master_cli():
    """CLI entry point for thunderbolt-master."""
    parser = argparse.ArgumentParser(
        description="Run Thunderbolt master node",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        "--port",
        type=int,
        default=8000,
        help="WebSocket port for slave connections"
    )
    parser.add_argument(
        "--api-port",
        type=int,
        default=None,
        help="REST API port (default: websocket port + 1)"
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
    
    args = parser.parse_args()
    
    print(f"Starting Thunderbolt Master")
    print(f"  WebSocket Port: {args.port}")
    print(f"  API Port: {args.api_port or args.port + 1}")
    print(f"  Health Check Interval: {args.health_check_interval}s")
    print(f"  Max Failed Health Checks: {args.max_failed_healthchecks}")
    print()
    
    master = ThunderboltMaster(
        port=args.port,
        health_check_interval=args.health_check_interval,
        max_failed_healthchecks=args.max_failed_healthchecks
    )
    
    try:
        master.run(host=args.host, port=args.api_port)
    except KeyboardInterrupt:
        print("\nShutting down master...")
        sys.exit(0)


def slave_cli():
    """CLI entry point for thunderbolt-slave."""
    parser = argparse.ArgumentParser(
        description="Run Thunderbolt slave node",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        "--master-ip",
        type=str,
        default="localhost",
        help="Master node IP address"
    )
    parser.add_argument(
        "--port",
        type=int,
        default=8000,
        help="Master WebSocket port to connect to"
    )
    parser.add_argument(
        "--hostname",
        type=str,
        default=None,
        help="Custom hostname (default: system hostname)"
    )
    
    args = parser.parse_args()
    
    print(f"Starting Thunderbolt Slave")
    print(f"  Master IP: {args.master_ip}")
    print(f"  Master Port: {args.port}")
    print(f"  Hostname: {args.hostname or 'system hostname'}")
    print()
    
    slave = ThunderboltSlave(
        master_ip=args.master_ip,
        port=args.port,
        hostname=args.hostname
    )
    
    try:
        slave.run()
    except KeyboardInterrupt:
        print("\nShutting down slave...")
        sys.exit(0)


if __name__ == "__main__":
    print("Use 'thunderbolt-master' or 'thunderbolt-slave' commands")
    sys.exit(1)