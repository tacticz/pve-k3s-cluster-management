# K3s Cluster Management

A set of tools for managing a highly available K3s cluster running on Proxmox VMs. This suite provides utilities for safe node operations, backups, and disaster recovery.

## Features

- **Safe Node Operations**: Properly cordon and drain nodes before shutting down or maintenance
- **Cluster Backup**: Create consistent backups of the entire cluster (Proxmox VM snapshots + etcd)
- **Node Replacement**: Replace failed nodes while maintaining cluster integrity
- **Cluster Restoration**: Restore from backups/snapshots to a consistent point-in-time state
- **Validation**: Extensive health checks for cluster verification
- **Multiple Interfaces**: Run via config file, command-line, or interactive mode

## Requirements

- Proxmox Virtual Environment (PVE) with cluster of K3s nodes
- Root SSH access from Proxmox hosts to VMs (passwordless)
- Root SSH access between K3s nodes (passwordless)
- Required tools:
  - `yq` for YAML parsing
  - `ssh` for remote command execution
  - `kubectl` for Kubernetes operations
  - `jq` for JSON parsing

### Install Required Packages

Installation on the Proxmox hosts:
```shell
> apt update
> apt install -y jq
> wget -qO /usr/local/bin/yq https://github.com/mikefarah/yq/releases/latest/download/yq_linux_amd64
> chmod +x /usr/local/bin/yq
```

To check the installation:
```shell
> yq --version
> jq --version
```

## Installation

1. Clone the repository or download the scripts:

```bash
git clone https://your-repo/k3s-cluster-management.git
cd k3s-cluster-management
```

2. Make the scripts executable:

```bash
chmod +x k3s-cluster-admin.sh
chmod +x lib/*.sh
```

3. Create a configuration file (or use the generated sample):

```bash
./k3s-cluster-admin.sh --interactive
# Select option 8 to generate a sample config
```

## Directory Structure

```
.
└── k3s-cluster-management
    ├── k3s-cluster-admin.sh    # Main script
    ├── config.sh               # Configuration script for standalone usage
    └── lib                     # Library modules
        ├── backup.sh           # Backup operations
        ├── node-ops.sh         # Node management operations
        ├── restore.sh          # Restoration operations
        ├── utils.sh            # Utility functions
        └── validation.sh       # Cluster validation functions
```

## Configuration

Create a YAML configuration file with details about your cluster. Example:

```yaml
# K3s Cluster Admin Configuration
---
cluster:
  name: k3s-cluster
  api_server: https://10.0.7.235:6443

# Nodes in the cluster
nodes:
  - k3s-node1
  - k3s-node2
  - k3s-node3

# Node details (IP addresses, VM IDs, etc.)
node_details:
  k3s-node1:
    ip: 10.0.7.235
    proxmox_vmid: 101
    proxmox_host: hasrv1.ldv.corp
    role: master
  k3s-node2:
    ip: 10.0.7.236
    proxmox_vmid: 102
    proxmox_host: hasrv2.ldv.corp
    role: master
  k3s-node3:
    ip: 10.0.7.237
    proxmox_vmid: 103
    proxmox_host: hasrv3.ldv.corp
    role: master

# Retention policy
retention:
  count: 5
  max_age_days: 30

# Operation timeouts (in seconds)
timeouts:
  draining: 300
  operation: 600

# Validation settings
validation:
  level: basic  # basic, extended, full
  etcd: true
  storage: true
  network: true

# Proxmox settings
proxmox:
  hosts:
    - hasrv1.ldv.corp
    - hasrv2.ldv.corp
    - hasrv3.ldv.corp
  user: root
  storage: pvecephfs-1-k3s
  templates:
    master: 9000  # Template VM ID for master node
    worker: 9001  # Template VM ID for worker node

# Backup settings
backup:
  prefix: k3s-backup
  location: /mnt/pvecephfs-1-backup
  compress: true
  include_etcd: true

# Notification settings
notifications:
  enabled: false
  email: admin@example.com

# Advanced settings
advanced:
  verbose: false
  debug: false
  ssh_key_path: ~/.ssh/id_rsa
  kubectl_path: /usr/local/bin/kubectl
```

## Usage

### Basic Usage

```bash
# Validate cluster health
./k3s-cluster-admin.sh validate

# Safely shutdown a node
./k3s-cluster-admin.sh --node k3s-node1 shutdown

# Create a backup of the entire cluster
./k3s-cluster-admin.sh backup

# Create a snapshot of the entire cluster
./k3s-cluster-admin.sh snapshot

# Replace a node
./k3s-cluster-admin.sh --node k3s-node1 replace

# Restore a cluster from backup/snapshot
./k3s-cluster-admin.sh --interactive
# Then select option 7 for restoration
```

### Command-Line Options

| Option | Description |
|--------|-------------|
| `-c, --config FILE` | Path to configuration file (default: `./conf/cluster-config.yaml`) |
| `-n, --node NODE` | Target node (hostname or IP) |
| `-a, --all-nodes` | Apply operation to all nodes sequentially |
| `-f, --force` | Skip confirmations |
| `-r, --retention N` | Number of snapshots/backups to keep |
| `-i, --interactive` | Run in interactive mode |
| `-v, --validate LEVEL` | Validation level: basic, extended, full |
| `-h, --help` | Show help message |
| `-d, --dry-run` | Show what would be done without doing it |

### Available Commands

| Command | Description |
|---------|-------------|
| `shutdown` | Safely shutdown a node VM |
| `backup` | Create a backup of the entire cluster |
| `snapshot` | Create a snapshot of the entire cluster |
| `replace` | Replace a node in the cluster |
| `validate` | Validate cluster health |
| `help` | Show help message |

### Interactive Mode

Run the script in interactive mode for guided operations:

```bash
./k3s-cluster-admin.sh --interactive
```

This will present a menu of operations:

1. Validate cluster health
2. Shutdown node
3. Start node
4. Create backup
5. Create snapshot
6. Replace node
7. Restore cluster
8. Generate sample config
9. Exit

## Backup and Snapshot Management

### Backup Process

When creating a backup, the script:

1. Takes an etcd snapshot for cluster state preservation
2. Creates Proxmox VM backups for all nodes
3. Links the etcd snapshot to the VM backups in the backup description
4. Cleans up old backups based on retention policy

### Restoration Process

The restoration process:

1. Finds the corresponding etcd snapshot based on backup/snapshot metadata
2. Restores etcd state to maintain cluster consistency
3. Restores VM state from backup/snapshot
4. Restarts nodes and verifies cluster health

## Validation Levels

The script offers three validation levels:

- **Basic**: Quick check of node status and cluster health
- **Extended**: Basic checks + etcd health and storage verification
- **Full**: Extended checks + network health and workload verification

## Troubleshooting

### Common Issues

1. **SSH Connection Issues**: Ensure passwordless SSH is properly set up between all nodes and from Proxmox hosts to VMs.

   ```bash
   # Test SSH connection
   ssh -o BatchMode=yes root@node_ip echo Success
   ```

2. **K3s Control Plane Unavailable**: If unable to access the K3s API, verify etcd health and check if at least one control plane node is running.

3. **Backups Not Creating**: Verify Proxmox storage configuration and ensure sufficient space in the backup location.

### Logs

Logs are stored in `/var/log/k3s-admin/` by default if the LOG_DIR environment variable is set.

## Safety Features

The script includes several safety mechanisms:

- Checks for minimum number of control plane nodes before operations
- Validates cluster health before and after operations
- Attempts proper cordoning and draining before shutting down nodes
- Takes automatic backups before risky operations (like node replacement)

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.
