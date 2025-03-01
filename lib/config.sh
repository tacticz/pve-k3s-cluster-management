#!/bin/bash
# config.sh - Configuration module for k3s-cluster-admin
#
# This module is part of the k3s-cluster-management
# It handles loading and processing configuration from YAML files,
# command-line arguments, and default values.
#
# Author: S-tor + claude.ai
# Date: February 2025
# Version: 0.1.0

# Require yq for YAML parsing
function check_dependencies() {
  if ! command -v yq &> /dev/null; then
    log_error "yq is required but not installed. Please install it first."
    log_info "You can install it with: sudo wget -qO /usr/local/bin/yq https://github.com/mikefarah/yq/releases/latest/download/yq_linux_amd64 && sudo chmod +x /usr/local/bin/yq"
    exit 1
  fi
}

# Load configuration from YAML file
function load_config() {
  local config_file="$1"
  check_dependencies
  
  # Check if file exists
  if [[ ! -f "$config_file" ]]; then
    log_warn "Configuration file $config_file not found, using default values."
    return 1
  fi
  
  # Load values from YAML using yq
  CONFIG_NODES=($(yq -r '.nodes[]' "$config_file" 2>/dev/null || echo ""))
  
  # Cluster configuration
  CLUSTER_NAME=$(yq -r '.cluster.name // ""' "$config_file")
  K3S_API_SERVER=$(yq -r '.cluster.api_server // ""' "$config_file")
  
  # Operation parameters
  CONFIG_RETENTION_COUNT=$(yq -r '.retention.count // 0' "$config_file")
  CONFIG_DRAINING_TIMEOUT=$(yq -r '.timeouts.draining // 0' "$config_file")
  CONFIG_OPERATION_TIMEOUT=$(yq -r '.timeouts.operation // 0' "$config_file")
  
  # Validation settings
  CONFIG_VALIDATE_LEVEL=$(yq -r '.validation.level // ""' "$config_file")
  CONFIG_VALIDATE_ETCD=$(yq -r '.validation.etcd // "true"' "$config_file")
  CONFIG_VALIDATE_STORAGE=$(yq -r '.validation.storage // "true"' "$config_file")
  CONFIG_VALIDATE_NETWORK=$(yq -r '.validation.network // "true"' "$config_file")
  
  # Proxmox settings
  PROXMOX_HOSTS=($(yq -r '.proxmox.hosts[]' "$config_file" 2>/dev/null || echo ""))
  PROXMOX_USER=$(yq -r '.proxmox.user // "root"' "$config_file")
  PROXMOX_STORAGE=$(yq -r '.proxmox.storage // ""' "$config_file")
  
  # Backup settings
  BACKUP_PREFIX=$(yq -r '.backup.prefix // "k3s-backup"' "$config_file")
  BACKUP_LOCATION=$(yq -r '.backup.location // ""' "$config_file")
  
  # Notification settings
  NOTIFY_ENABLED=$(yq -r '.notifications.enabled // "false"' "$config_file")
  NOTIFY_EMAIL=$(yq -r '.notifications.email // ""' "$config_file")
  
  # Advanced settings
  VERBOSE=$(yq -r '.advanced.verbose // "false"' "$config_file")
  DEBUG=$(yq -r '.advanced.debug // "false"' "$config_file")
  
  log_info "Configuration loaded from $config_file"
  return 0
}

# Merge configuration from defaults, config file, and command line
function merge_config() {
  # Apply defaults for values not set
  CLUSTER_NAME="${CLUSTER_NAME:-k3s-cluster}"
  
  # Merge retention settings (CLI > Config > Default)
  RETENTION_COUNT="${RETENTION_COUNT:-${CONFIG_RETENTION_COUNT:-$DEFAULT_RETENTION_COUNT}}"
  
  # Merge timeout settings
  DRAINING_TIMEOUT="${CONFIG_DRAINING_TIMEOUT:-$DEFAULT_DRAINING_TIMEOUT}"
  OPERATION_TIMEOUT="${CONFIG_OPERATION_TIMEOUT:-$DEFAULT_OPERATION_TIMEOUT}"
  
  # Merge validation settings
  VALIDATE_LEVEL="${VALIDATE_LEVEL:-${CONFIG_VALIDATE_LEVEL:-$DEFAULT_VALIDATE_LEVEL}}"
  VALIDATE_ETCD="${CONFIG_VALIDATE_ETCD:-true}"
  VALIDATE_STORAGE="${CONFIG_VALIDATE_STORAGE:-true}"
  VALIDATE_NETWORK="${CONFIG_VALIDATE_NETWORK:-true}"
  
  # Set nodes to operate on (CLI > Config)
  if [[ -n "$TARGET_NODE" ]]; then
    NODES=("$TARGET_NODE")
  elif [[ "$ALL_NODES" == "true" ]]; then
    # If all_nodes flag is set, get all nodes from the cluster
    NODES=($(get_all_cluster_nodes))
  elif [[ ${#CONFIG_NODES[@]} -gt 0 ]]; then
    NODES=("${CONFIG_NODES[@]}")
  else
    # If no nodes specified, we'll ask in interactive mode or fail otherwise
    if [[ "$INTERACTIVE" != "true" ]]; then
      log_error "No target nodes specified. Use --node, --all-nodes, or specify nodes in config file."
      exit 1
    fi
  fi
  
  # Set backup settings
  BACKUP_PREFIX="${BACKUP_PREFIX:-k3s-backup}"
  TIMESTAMP=$(date +"%Y%m%d-%H%M%S")
  BACKUP_NAME="${BACKUP_PREFIX}-${TIMESTAMP}"
  
  # Set verbose/debug mode
  VERBOSE="${VERBOSE:-false}"
  DEBUG="${DEBUG:-false}"
  
  # If in debug mode, also enable verbose
  if [[ "$DEBUG" == "true" ]]; then
    VERBOSE="true"
  fi
}

# Print current configuration (for verbose mode)
function print_config() {
  echo "=== Current Configuration ==="
  echo "Command: $COMMAND"
  echo "Cluster: $CLUSTER_NAME"
  echo "Target Nodes: ${NODES[*]}"
  echo "Retention Count: $RETENTION_COUNT"
  echo "Validation Level: $VALIDATE_LEVEL"
  echo "Interactive Mode: $INTERACTIVE"
  echo "Force Mode: $FORCE"
  echo "Dry Run: $DRY_RUN"
  echo "Backup Name: $BACKUP_NAME"
  echo "==========================="
}

# Generate a sample configuration file
function generate_sample_config() {
  local output_file="$1"
  
  if [[ -f "$output_file" ]] && [[ "$FORCE" != "true" ]]; then
    log_error "Configuration file $output_file already exists. Use --force to overwrite."
    return 1
  fi
  
  cat > "$output_file" <<EOF
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

# Backup settings
backup:
  prefix: k3s-backup
  location: /mnt/pve/pvecephfs-1-backup
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
EOF

  log_info "Sample configuration generated at $output_file"
  return 0
}
