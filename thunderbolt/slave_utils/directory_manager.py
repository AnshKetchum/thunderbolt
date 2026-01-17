from pathlib import Path


class DirectoryManager:
    """Manages shared directory structure for a slave node."""
    
    def __init__(self, shared_dir: Path, hostname: str):
        self.shared_dir = shared_dir
        self.hostname = hostname
        self.node_dir = None
        self.jobs_file = None
    
    def setup(self):
        """Initialize slave's directory in shared storage."""
        try:
            # Create node-specific directory
            self.node_dir = self.shared_dir / self.hostname
            self.node_dir.mkdir(parents=True, exist_ok=True)
            
            # Reference to jobs file
            self.jobs_file = self.shared_dir / "jobs.json"
            
            print(f"[DirMgr] Initialized: {self.node_dir}")
            print(f"[DirMgr] Watching jobs: {self.jobs_file}")
            
        except Exception as e:
            print(f"[DirMgr] Setup failed: {e}")
            raise
    
    def get_result_path(self, command_id: str) -> Path:
        """Get path for storing command result."""
        return self.node_dir / f"{command_id}.json"
    
    def result_exists(self, command_id: str) -> bool:
        """Check if result file already exists."""
        return self.get_result_path(command_id).exists()