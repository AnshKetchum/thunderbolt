from fastapi import HTTPException, APIRouter
from pydantic import BaseModel
from typing import List, Dict, Optional


class CommandRequest(BaseModel):
    command: str
    nodes: List[str]
    timeout: Optional[int] = 30
    use_sudo: Optional[bool] = False
    force_method: Optional[str] = None


class BatchedCommandRequest(BaseModel):
    commands: List[Dict]
    force_method: Optional[str] = None


def create_router(master) -> APIRouter:
    """Create the FastAPI router with all endpoints."""
    
    if isinstance(master.routes_prefix, str) and master.routes_prefix:
        router = APIRouter(prefix=master.routes_prefix)
    else:
        router = APIRouter()
    
    @router.post("/run")
    async def run_command(request: CommandRequest):
        """Execute a command on specified nodes in parallel."""
        # Validate nodes
        invalid_nodes = [node for node in request.nodes if node not in master.slaves]
        if invalid_nodes:
            raise HTTPException(
                status_code=404,
                detail=f"Nodes not found: {invalid_nodes}"
            )
        
        # Determine execution method
        use_shared_dir = False
        if request.force_method == "shared_dir":
            if not master.shared_dir_executor:
                raise HTTPException(
                    status_code=400,
                    detail="Shared directory not configured"
                )
            use_shared_dir = True
        elif request.force_method == "websocket":
            use_shared_dir = False
        else:
            use_shared_dir = master._should_use_shared_dir(len(request.nodes))
        
        # Execute command using appropriate method
        if use_shared_dir:
            return await master.shared_dir_executor.execute_command(
                request.command,
                request.nodes,
                request.timeout,
                request.use_sudo
            )
        else:
            return await master.ws_executor.execute_command(
                request.command,
                request.nodes,
                request.timeout,
                request.use_sudo
            )
    
    @router.post("/run_batched")
    async def run_batched_commands(request: BatchedCommandRequest):
        """Execute different commands on different nodes in parallel."""
        if not request.commands:
            return {
                "total_commands": 0,
                "total_nodes": 0,
                "results": {}
            }
        
        # Organize commands by node
        node_queues = {}
        for cmd_spec in request.commands:
            node = cmd_spec.get("node")
            command = cmd_spec.get("command")
            timeout = cmd_spec.get("timeout", 30)
            use_sudo = cmd_spec.get("use_sudo", False)
            
            if not node or not command:
                continue
                
            if node not in node_queues:
                node_queues[node] = []
            
            node_queues[node].append({
                "command": command,
                "timeout": timeout,
                "use_sudo": use_sudo
            })
        
        # Validate all nodes exist
        invalid_nodes = [node for node in node_queues.keys() if node not in master.slaves]
        if invalid_nodes:
            raise HTTPException(
                status_code=404,
                detail=f"Nodes not found: {invalid_nodes}"
            )
        
        # Determine execution method
        use_shared_dir = False
        if request.force_method == "shared_dir":
            if not master.shared_dir_executor:
                raise HTTPException(
                    status_code=400,
                    detail="Shared directory not configured"
                )
            use_shared_dir = True
        elif request.force_method == "websocket":
            use_shared_dir = False
        else:
            total_commands = len(request.commands)
            use_shared_dir = master._should_use_shared_dir(total_commands)
        
        if use_shared_dir:
            return await master.shared_dir_executor.execute_batched(node_queues)
        else:
            return await master.ws_executor.execute_batched(node_queues)
    
    @router.get("/nodes")
    async def list_nodes():
        """List all connected slave nodes."""
        return {
            "total": len(master.slaves),
            "nodes": [
                {
                    "hostname": hostname,
                    "last_seen": info["last_seen"].isoformat(),
                    "failed_healthchecks": info["failed_healthchecks"],
                    "command_connected": info.get("command_ws") is not None,
                    "health_connected": info.get("health_ws") is not None
                }
                for hostname, info in master.slaves.items()
            ]
        }
    
    @router.get("/")
    async def root():
        return {
            "message": "Thunderbolt Master Command Runner",
            "connected_slaves": len(master.slaves),
            "command_port": master.port,
            "health_check_port": master.health_check_port,
            "shared_directory": str(master.shared_dir) if master.shared_dir else None
        }
    
    @router.get("/health")
    async def health():
        return {
            "status": "healthy", 
            "connected_slaves": len(master.slaves),
            "pending_commands": len(master.pending_commands)
        }
    
    return router