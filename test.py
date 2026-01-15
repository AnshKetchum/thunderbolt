from thunderbolt import ThunderBoltAPI

# Quick start - run commands across your cluster
# This assumes you have a thunderbolt master running on port 9000 and serving the API on 9001
with ThunderBoltAPI(host="localhost", port=9001) as api:
    # See what's connected
    nodes = api.list_nodes()
    print(f"📡 {nodes['total']} nodes online")
    
    # Run a command everywhere
    result = api.run_on_all_nodes("nvidia-smi --query-gpu=name --format=csv,noheader")
    for host, res in result['results'].items():
        if res['status'] == 'success':
            print(f"  ✓ {host}: {res['stdout'].strip()}")
    
    # Or target specific nodes
    result = api.run_command(
        command="df -h /workspace",
        nodes=["node-01", "node-02"],
        timeout=10
    )