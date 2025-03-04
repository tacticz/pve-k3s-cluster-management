#!/bin/bash
# config.sh - Configuration module for k3s-cluster-admin
#
# This module is part of the k3s-cluster-management
# It handles loading and processing configuration from YAML files,
# command-line arguments, and default values.
#
# Author: S-tor + claude.ai
# Date: February 2025
# Version: 0.1.1

# Global variables for discovery
declare -a NODES
declare -A NODE_DETAILS
declare -a PROXMOX_HOSTS
PROXMOX_STORAGE=""
BACKUP_LOCATION=""
SSH_PORT="22"

# Require yq for YAML parsing
function check_dependencies() {
  if ! command -v yq &> /dev/null; then
    log_error "yq is required but not installed. Please install it first."
    log_info "You can install it with: sudo wget -qO /usr/local/bin/yq https://github.com/mikefarah/yq/releases/latest/download/yq_linux_amd64 && sudo chmod +x /usr/local/bin/yq"
    exit 1
  fi
}

# Verify SSH access to a node
function verify_ssh_access() {
  local node="$1"
  local port="${2:-$SSH_PORT}"
  
  ssh -o BatchMode=yes -o ConnectTimeout=5 -p "$port" root@$node "echo 'OK'" &>/dev/null
  return $?
}

# Initial SSH connection to accept fingerprint
function initial_ssh_connection() {
  local node="$1"
  local port="${2:-$SSH_PORT}"
  
  log_info "Verifying SSH fingerprint for $node:$port..."
  
  # Save current FORCE value and temporarily set it to false
  local original_force="$FORCE"
  FORCE="false"
  
  # Ask user whether to trust the host automatically
  if confirm "Would you like to automatically accept the SSH fingerprint for $node?"; then
    # Using StrictHostKeyChecking=no to automatically accept the fingerprint
    ssh -o StrictHostKeyChecking=no -o ConnectTimeout=5 -p "$port" root@$node "echo Connection established" &>/dev/null
    local status=$?
    
    # Restore original FORCE value
    FORCE="$original_force"
    
    if [[ $status -eq 0 ]]; then
      log_success "SSH fingerprint accepted for $node"
      return 0
    else
      log_error "Failed to establish SSH connection to $node"
      return 1
    fi
  else
    # Restore original FORCE value
    FORCE="$original_force"
    
    # Manual verification - instruct the user
    log_info "Please manually verify the SSH fingerprint by connecting to the node:"
    log_info "Run: ssh -p $port root@$node"
    log_info "After verification is complete, press Enter to continue"
    read -p "Press Enter when done..." confirm
    
    # Check if the connection works now
    if verify_ssh_access "$node" "$port"; then
      log_success "SSH connection verified for $node"
      return 0
    else
      log_error "SSH connection still failing for $node"
      return 1
    fi
  fi
}

# Setup SSH key authentication
function setup_ssh_key() {
  local node="$1"
  local port="${2:-$SSH_PORT}"
  local ssh_key_path="${SSH_KEY_PATH:-~/.ssh/id_rsa}"
  
  # Check if SSH key exists
  if [[ ! -f "$ssh_key_path" ]]; then
    log_info "SSH key not found at $ssh_key_path"
    if confirm "Would you like to generate a new SSH key?"; then
      ssh-keygen -t rsa -b 4096 -f "$ssh_key_path" -N "" || return 1
    else
      return 1
    fi
  fi
  
  # First establish initial connection to handle fingerprint verification
  initial_ssh_connection "$node" "$port" || return 1
  
  # Ask for password to copy SSH key
  log_info "Please enter the password for root@$node when prompted"
  ssh-copy-id -i "$ssh_key_path" -p "$port" root@$node || return 1
  
  # Verify access
  if verify_ssh_access "$node" "$port"; then
    log_success "SSH key authentication set up successfully"
    return 0
  else
    log_error "SSH key authentication setup failed"
    return 1
  fi
}

# Discover cluster nodes VM details using pvesh
function discover_node_proxmox_details() {
  log_info "Retrieving Proxmox VM details using pvesh..."
  
  # Check if pvesh is available
  if ! command -v pvesh &>/dev/null; then
    log_warn "pvesh command not available - cannot automatically discover VM details"
    return 1
  fi
  
  # Get resources from Proxmox API in JSON format
  local vm_resources=$(pvesh get /cluster/resources --type vm --output-format json 2>/dev/null)
  
  if [[ -z "$vm_resources" ]]; then
    log_warn "Failed to get VM resources from Proxmox API"
    return 1
  fi
  
  log_info "Retrieved VM details from Proxmox, parsing..."
  
  # Process each node to find its VM details
  for node in "${NODES[@]}"; do
    # Extract VM details using grep and sed
    # Look for the node name in the JSON output
    local node_data=$(echo "$vm_resources" | grep -o '{[^}]*"name":"'"$node"'"[^}]*}')
    
    if [[ -n "$node_data" ]]; then
      # Extract VMID and Proxmox host from the JSON data
      local vm_id=$(echo "$node_data" | grep -o '"vmid":[0-9]*' | cut -d':' -f2)
      local proxmox_host=$(echo "$node_data" | grep -o '"node":"[^"]*"' | cut -d':' -f2 | tr -d '"')
      
      if [[ -n "$vm_id" && -n "$proxmox_host" ]]; then
        log_info "Found VM ID $vm_id on host $proxmox_host for node $node"
        
        # Get current node details
        local details="${NODE_DETAILS[$node]}"
        # Update VM ID and Proxmox host
        if [[ -n "$details" ]]; then
          # Parse existing details
          local ip=""
          local role="worker"
          
          IFS=',' read -ra detail_parts <<< "$details"
          for part in "${detail_parts[@]}"; do
            key="${part%%=*}"
            value="${part#*=}"
            
            case "$key" in
              ip) ip="$value" ;;
              role) role="$value" ;;
            esac
          done
          
          # Update details with new VM ID and host
          NODE_DETAILS["$node"]="ip=$ip,role=$role,proxmox_vmid=$vm_id,proxmox_host=$proxmox_host"
        else
          NODE_DETAILS["$node"]="proxmox_vmid=$vm_id,proxmox_host=$proxmox_host"
        fi
      fi
    else
      log_warn "Could not find VM details for node $node in Proxmox resources"
    fi
  done
  
  log_success "VM details discovery completed"
  return 0
}

# Discover cluster nodes
function discover_cluster_nodes() {
  local control_node="$1"
  
  # Get nodes using kubectl
  log_info "Getting nodes via kubectl from $control_node..."
  local node_list=$(ssh -p "$SSH_PORT" root@$control_node "kubectl get nodes -o=jsonpath='{range .items[*]}{.metadata.name}{\"\\n\"}{end}'" 2>/dev/null)
  
  if [[ -z "$node_list" ]]; then
    log_error "Failed to get nodes from the cluster"
    return 1
  fi
  
  # Store nodes in the global array
  readarray -t NODES <<< "$node_list"
  log_info "Discovered nodes: ${NODES[*]}"
  
  # Get node details
  for node in "${NODES[@]}"; do
    # Try to get IP address
    local node_ip=$(ssh -p "$SSH_PORT" root@$control_node "kubectl get node $node -o=jsonpath='{.status.addresses[?(@.type==\"InternalIP\")].address}'" 2>/dev/null)
    
    # Get node role (master or worker)
    local is_master=$(ssh -p "$SSH_PORT" root@$control_node "kubectl get node $node -o=jsonpath='{.metadata.labels.node-role\\.kubernetes\\.io/master}'" 2>/dev/null)
    local is_controlplane=$(ssh -p "$SSH_PORT" root@$control_node "kubectl get node $node -o=jsonpath='{.metadata.labels.node-role\\.kubernetes\\.io/control-plane}'" 2>/dev/null)
    
    local role="worker"
    if [[ -n "$is_master" || -n "$is_controlplane" ]]; then
      role="master"
    fi
    
    log_info "Node $node: IP=$node_ip, Role=$role"
    
    # Check SSH access to this node
    if ! verify_ssh_access "$node" "$SSH_PORT"; then
      log_warn "Cannot SSH to $node with key authentication"
      
      # First try initial connection to handle fingerprint
      initial_ssh_connection "$node" "$SSH_PORT"
      
      # If still can't access with key, offer to set up SSH key
      if ! verify_ssh_access "$node" "$SSH_PORT"; then
        if confirm "Would you like to set up SSH key authentication for $node?"; then
          setup_ssh_key "$node" "$SSH_PORT" || log_warn "Failed to set up SSH key authentication for $node"
        fi
      fi
    fi
    
    # Store basic node details - VM ID and host will be added later
    NODE_DETAILS["$node"]="ip=$node_ip,role=$role,proxmox_vmid=,proxmox_host="
  done
  
  # Attempt to discover VM details directly from Proxmox API
  log_info "Attempting to discover VM details from Proxmox..."
  discover_node_proxmox_details
  
  # If discovery wasn't successful or we're missing some details, try manually
  for node in "${NODES[@]}"; do
    local details="${NODE_DETAILS[$node]}"
    
    # Parse details to check if VM ID and host are set
    local vm_id=""
    local proxmox_host=""
    
    IFS=',' read -ra detail_parts <<< "$details"
    for part in "${detail_parts[@]}"; do
      key="${part%%=*}"
      value="${part#*=}"
      
      case "$key" in
        proxmox_vmid) vm_id="$value" ;;
        proxmox_host) proxmox_host="$value" ;;
      esac
    done
    
    # If VM ID or host is missing, ask user
    if [[ -z "$vm_id" || -z "$proxmox_host" ]]; then
      log_warn "Missing Proxmox details for node $node"
      
      # Ask for VM ID if missing
      if [[ -z "$vm_id" ]]; then
        read -p "Enter VM ID for node $node (leave empty to skip): " vm_id
      fi
      
      # Ask for Proxmox host if missing
      if [[ -n "$vm_id" && -z "$proxmox_host" ]]; then
        if [[ ${#PROXMOX_HOSTS[@]} -eq 1 ]]; then
          # If only one Proxmox host, use it by default
          proxmox_host="${PROXMOX_HOSTS[0]}"
          log_info "Using only available Proxmox host: $proxmox_host"
        else
          # List available hosts for selection
          if [[ ${#PROXMOX_HOSTS[@]} -gt 0 ]]; then
            echo "Available Proxmox hosts:"
            for i in "${!PROXMOX_HOSTS[@]}"; do
              echo "$((i+1)). ${PROXMOX_HOSTS[$i]}"
            done
            read -p "Enter Proxmox host number for VM $vm_id: " host_idx
            
            if [[ "$host_idx" =~ ^[0-9]+$ && "$host_idx" -ge 1 && "$host_idx" -le "${#PROXMOX_HOSTS[@]}" ]]; then
              proxmox_host="${PROXMOX_HOSTS[$((host_idx-1))]}"
            else
              read -p "Enter Proxmox host for VM $vm_id: " proxmox_host
            fi
          else
            read -p "Enter Proxmox host for VM $vm_id: " proxmox_host
          fi
        fi
      fi
      
      # Update node details with user-provided information
      if [[ -n "$vm_id" || -n "$proxmox_host" ]]; then
        local ip=""
        local role="worker"
        
        IFS=',' read -ra detail_parts <<< "$details"
        for part in "${detail_parts[@]}"; do
          key="${part%%=*}"
          value="${part#*=}"
          
          case "$key" in
            ip) ip="$value" ;;
            role) role="$value" ;;
          esac
        done
        
        NODE_DETAILS["$node"]="ip=$ip,role=$role,proxmox_vmid=$vm_id,proxmox_host=$proxmox_host"
        log_info "Updated details for node $node: VM ID=$vm_id, Proxmox Host=$proxmox_host"
      fi
    fi
  done
  
  return 0
}

# Discover Proxmox hosts
function discover_proxmox_hosts() {  
  # First check if we're running on a Proxmox host
  if command -v pvecm &>/dev/null; then
    # We're on a Proxmox host, so use local command
    log_info "Detected script running on a Proxmox host, getting cluster information locally..."
    local pvecm_output=$(pvecm status 2>/dev/null)
    
    if [[ -n "$pvecm_output" ]]; then
      # Extract hosts from membership information
      local hosts=$(echo "$pvecm_output" | grep -A 100 "Membership information" | grep -B 100 -m 1 "^$" | grep -v "Membership information" | grep -v "^--" | grep -v "^$" | awk '{print $NF}' | sort | uniq)
      
      if [[ -n "$hosts" ]]; then
        readarray -t PROXMOX_HOSTS <<< "$hosts"
        log_info "Discovered Proxmox hosts: ${PROXMOX_HOSTS[*]}"
        
        # Try to get storage info
        discover_proxmox_storage
        return 0
      fi
    fi
  fi
  
  # If we reach here, either we're not on a Proxmox host or couldn't get info
  log_info "Trying to detect Proxmox environment..."
  
  # Check if we're on a Proxmox host but pvecm failed
  if [ -f /etc/pve/storage.cfg ]; then
    log_info "Found Proxmox storage configuration file, running on a Proxmox host"
    # Get local hostname
    local hostname=$(hostname -f)
    PROXMOX_HOSTS=("$hostname")
    log_info "Using local Proxmox host: $hostname"
    
    # Try to get storage info
    discover_proxmox_storage
    return 0
  fi
  
  # Not running on Proxmox, ask user for information
  log_warn "Not running on a Proxmox host or unable to detect Proxmox environment"
  read -p "Enter comma-separated list of Proxmox hosts: " proxmox_hosts_input
  if [[ -n "$proxmox_hosts_input" ]]; then
    IFS=',' read -ra PROXMOX_HOSTS <<< "$proxmox_hosts_input"
    log_info "Using Proxmox hosts: ${PROXMOX_HOSTS[*]}"
    
    # Check SSH access to Proxmox hosts
    for host in "${PROXMOX_HOSTS[@]}"; do
      if ! verify_ssh_access "$host" "$SSH_PORT"; then
        log_warn "Cannot SSH to Proxmox host $host with key authentication"
        
        # Try initial connection first
        initial_ssh_connection "$host" "$SSH_PORT"
        
        # If still can't access with key, offer to set up SSH key
        if ! verify_ssh_access "$host" "$SSH_PORT"; then
          if confirm "Would you like to set up SSH key authentication for $host?"; then
            setup_ssh_key "$host" "$SSH_PORT" || log_warn "Failed to set up SSH key authentication for $host"
          fi
        fi
      fi
    done
    
    # Try to get storage info
    discover_proxmox_storage
  else
    log_warn "No Proxmox hosts specified, skipping Proxmox integration"
  fi
  
  return 0
}

# Discover Proxmox storage
function discover_proxmox_storage() {
  # First check if we're running on a Proxmox host
  if command -v pvesm &>/dev/null; then
    # We're on a Proxmox host, use local command
    log_info "Getting storage information from local Proxmox system..."
    local storage_output=$(pvesm status 2>/dev/null)
    
    if [[ -n "$storage_output" ]]; then
      # Look for cephfs storage
      local cephfs_storage=$(echo "$storage_output" | grep "cephfs" | awk '{print $1}' | head -1)
      
      if [[ -n "$cephfs_storage" ]]; then
        PROXMOX_STORAGE="$cephfs_storage"
        log_info "Discovered Proxmox CephFS storage: $PROXMOX_STORAGE"
        return 0
      else
        # Look for any shared storage
        local shared_storage=$(echo "$storage_output" | grep -v "local" | grep -v "^Name" | awk '{print $1}' | head -1)
        
        if [[ -n "$shared_storage" ]]; then
          PROXMOX_STORAGE="$shared_storage"
          log_info "Discovered Proxmox shared storage: $PROXMOX_STORAGE"
          return 0
        fi
      fi
    fi
  fi
  
  # If not running on a Proxmox host or local command failed, try SSH to hosts
  for host in "${PROXMOX_HOSTS[@]}"; do
    if verify_ssh_access "$host" "$SSH_PORT"; then
      log_info "Getting storage information from Proxmox host $host..."
      local storage_output=$(ssh -p "$SSH_PORT" root@$host "pvesm status" 2>/dev/null)
      
      if [[ -n "$storage_output" ]]; then
        # Look for cephfs storage
        local cephfs_storage=$(echo "$storage_output" | grep "cephfs" | awk '{print $1}' | head -1)
        
        if [[ -n "$cephfs_storage" ]]; then
          PROXMOX_STORAGE="$cephfs_storage"
          log_info "Discovered Proxmox CephFS storage: $PROXMOX_STORAGE"
          return 0
        else
          # Look for any shared storage
          local shared_storage=$(echo "$storage_output" | grep -v "local" | grep -v "^Name" | awk '{print $1}' | head -1)
          
          if [[ -n "$shared_storage" ]]; then
            PROXMOX_STORAGE="$shared_storage"
            log_info "Discovered Proxmox shared storage: $PROXMOX_STORAGE"
            return 0
          fi
        fi
      fi
    fi
  done
  
  # If still no storage found, ask user
  if [[ -z "$PROXMOX_STORAGE" ]]; then
    log_warn "Could not discover Proxmox storage"
    read -p "Enter Proxmox storage name for backups and VMs: " PROXMOX_STORAGE
    if [[ -n "$PROXMOX_STORAGE" ]]; then
      log_info "Using storage: $PROXMOX_STORAGE"
    fi
  fi
  
  return 0
}

# Discover storage configuration
function discover_storage_config() {
  local node="$1"
  
  # Check for CephFS mount
  local cephfs_mount=$(ssh -p "$SSH_PORT" root@$node "mount | grep cephfs" 2>/dev/null)
  
  if [[ -n "$cephfs_mount" ]]; then
    local mount_point=$(echo "$cephfs_mount" | awk '{print $3}' | head -1)
    BACKUP_LOCATION="$mount_point"
    log_info "Discovered CephFS mount point: $BACKUP_LOCATION"
  else
    log_warn "No CephFS mount found"
    BACKUP_LOCATION="/mnt/backup"
    log_info "Using default backup location: $BACKUP_LOCATION"
  fi
}

# Create config from discovered information
function create_config_from_discovery() {
  local output_file="$1"
  
  cat > "$output_file" <<EOF
# K3s Cluster Admin Configuration
---
cluster:
  name: $cluster_name
  api_server: $api_server

# Nodes in the cluster
nodes:
EOF
  
  # Add nodes
  for node in "${NODES[@]}"; do
    echo "  - $node" >> "$output_file"
  done
  
  # Add node details
  cat >> "$output_file" <<EOF

# Node details (IP addresses, VM IDs, etc.)
node_details:
EOF
  
  for node in "${NODES[@]}"; do
    local details="${NODE_DETAILS[$node]}"
    IFS=',' read -ra detail_parts <<< "$details"
    
    local ip=""
    local role="worker"
    local vmid=""
    local proxmox_host=""
    
    for part in "${detail_parts[@]}"; do
      key="${part%%=*}"
      value="${part#*=}"
      
      case "$key" in
        ip) ip="$value" ;;
        role) role="$value" ;;
        proxmox_vmid) vmid="$value" ;;
        proxmox_host) proxmox_host="$value" ;;
      esac
    done
    
    cat >> "$output_file" <<EOF
  $node:
    ip: $ip
    proxmox_vmid: $vmid
    proxmox_host: $proxmox_host
    role: $role
EOF
  done
  
  # Add remaining configuration
  cat >> "$output_file" <<EOF

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
EOF
  
  # Add Proxmox hosts
  for host in "${PROXMOX_HOSTS[@]}"; do
    echo "    - $host" >> "$output_file"
  done
  
  cat >> "$output_file" <<EOF
  user: root
  storage: $PROXMOX_STORAGE
  templates:
    master: 9000  # Template VM ID for master node (placeholder)
    worker: 9001  # Template VM ID for worker node (placeholder)

# Backup settings
backup:
  prefix: k3s-backup
  location: $BACKUP_LOCATION
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
  ssh_port: $SSH_PORT
EOF
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
  SSH_PORT=$(yq -r '.advanced.ssh_port // "22"' "$config_file")
  
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

# Interactive config generation
function generate_sample_config() {
  local output_file="$1"
  local interactive="${2:-false}"
  
  if [[ -f "$output_file" ]] && [[ "$FORCE" != "true" ]]; then
    log_error "Configuration file $output_file already exists. Use --force to overwrite."
    return 1
  fi
  
  # If not in interactive mode, generate static sample
  if [[ "$interactive" != "true" ]]; then
    create_static_sample_config "$output_file"
    return $?
  fi
  
  # Interactive config generation
  log_section "Interactive Configuration Generation"
  
  # Collect cluster information
  local cluster_name=""
  local control_plane_node=""
  local api_server=""
  local nodes=()
  local node_details=()
  local proxmox_hosts=()
  local proxmox_storage=""
  
  # Step 1: Ask for cluster name
  read -p "Enter k3s cluster name [k3s-cluster]: " cluster_name
  cluster_name="${cluster_name:-k3s-cluster}"
  log_info "Using cluster name: $cluster_name"
  
  # Step 2: Ask for SSH port
  read -p "Enter SSH port for nodes [22]: " ssh_port_input
  SSH_PORT="${ssh_port_input:-22}"
  log_info "Using SSH port: $SSH_PORT"
  
  # Step 3: Ask for control plane node
  read -p "Enter one of your k3s control plane nodes (hostname or IP): " control_plane_node
  if [[ -z "$control_plane_node" ]]; then
    log_error "Control plane node is required"
    return 1
  fi
  
  # Step 4: Verify SSH access
  log_info "Checking SSH access to $control_plane_node on port $SSH_PORT..."

  # First try to establish initial connection if needed
  if ! verify_ssh_access "$control_plane_node" "$SSH_PORT"; then
    log_info "Initial SSH connection required for $control_plane_node"
    initial_ssh_connection "$control_plane_node" "$SSH_PORT" || {
      log_error "Failed to establish initial SSH connection"
      return 1
    }
    
    # Now check if we need to set up SSH key authentication
    if ! verify_ssh_access "$control_plane_node" "$SSH_PORT"; then
      log_warn "Cannot SSH to $control_plane_node with key authentication"
      if confirm "Would you like to set up SSH key authentication?"; then
        setup_ssh_key "$control_plane_node" "$SSH_PORT" || {
          log_error "Failed to set up SSH key authentication"
          return 1
        }
      else
        log_error "SSH access is required for cluster discovery"
        return 1
      fi
    fi
  fi
  
  # Step 5: Discover cluster nodes
  log_info "Discovering k3s cluster nodes..."
  if ! discover_cluster_nodes "$control_plane_node"; then
    log_error "Failed to discover cluster nodes"
    return 1
  fi
  
  # Get API server address
  api_server=$(ssh -p "$SSH_PORT" root@$control_plane_node "cat /etc/rancher/k3s/k3s.yaml | grep server: | awk '{print \$2}'" 2>/dev/null)
  log_info "Detected API server: $api_server"
  
  # Step 6: Attempt to discover Proxmox VM details first if running on a Proxmox host
  log_info "Attempting to discover VM details from Proxmox..."
  discover_node_proxmox_details

  # Step 7: Discover Proxmox hosts if needed
  log_info "Discovering Proxmox hosts..."
  discover_proxmox_hosts

  # Re-attempt discovery of VM details if we found hosts but not VM details
  if [[ ${#PROXMOX_HOSTS[@]} -gt 0 ]]; then
    # Check if we're missing any VM details
    local missing_details=false
    for node in "${NODES[@]}"; do
      local details="${NODE_DETAILS[$node]}"
      if [[ "$details" == *"proxmox_vmid=,"* || "$details" == *"proxmox_host=,"* ]]; then
        missing_details=true
        break
      fi
    done
    
    if [[ "$missing_details" == "true" ]]; then
      log_info "Re-discovering node VM details with Proxmox host information..."
      discover_node_proxmox_details
    fi
  fi
  
  # After all automatic discovery attempts, check for any missing details
  for node in "${NODES[@]}"; do
    local details="${NODE_DETAILS[$node]}"
    
    # Parse details to check if VM ID and host are set
    local vm_id=""
    local proxmox_host=""
    
    IFS=',' read -ra detail_parts <<< "$details"
    for part in "${detail_parts[@]}"; do
      key="${part%%=*}"
      value="${part#*=}"
      
      case "$key" in
        proxmox_vmid) vm_id="$value" ;;
        proxmox_host) proxmox_host="$value" ;;
      esac
    done
    
    # If VM ID or host is missing, ask user
    if [[ -z "$vm_id" || -z "$proxmox_host" ]]; then
      log_warn "Missing Proxmox details for node $node"
      
      # Ask for VM ID if missing
      if [[ -z "$vm_id" ]]; then
        read -p "Enter VM ID for node $node (leave empty to skip): " vm_id
      fi
      
      # Ask for Proxmox host if missing
      if [[ -n "$vm_id" && -z "$proxmox_host" ]]; then
        if [[ ${#PROXMOX_HOSTS[@]} -eq 1 ]]; then
          # If only one Proxmox host, use it by default
          proxmox_host="${PROXMOX_HOSTS[0]}"
          log_info "Using only available Proxmox host: $proxmox_host"
        else
          # List available hosts for selection
          if [[ ${#PROXMOX_HOSTS[@]} -gt 0 ]]; then
            echo "Available Proxmox hosts:"
            for i in "${!PROXMOX_HOSTS[@]}"; do
              echo "$((i+1)). ${PROXMOX_HOSTS[$i]}"
            done
            read -p "Enter Proxmox host number for VM $vm_id: " host_idx
            
            if [[ "$host_idx" =~ ^[0-9]+$ && "$host_idx" -ge 1 && "$host_idx" -le "${#PROXMOX_HOSTS[@]}" ]]; then
              proxmox_host="${PROXMOX_HOSTS[$((host_idx-1))]}"
            else
              read -p "Enter Proxmox host for VM $vm_id: " proxmox_host
            fi
          else
            read -p "Enter Proxmox host for VM $vm_id: " proxmox_host
          fi
        fi
      fi
      
      # Update node details with user-provided information
      if [[ -n "$vm_id" || -n "$proxmox_host" ]]; then
        local ip=""
        local role="worker"
        
        IFS=',' read -ra detail_parts <<< "$details"
        for part in "${detail_parts[@]}"; do
          key="${part%%=*}"
          value="${part#*=}"
          
          case "$key" in
            ip) ip="$value" ;;
            role) role="$value" ;;
          esac
        done
        
        NODE_DETAILS["$node"]="ip=$ip,role=$role,proxmox_vmid=$vm_id,proxmox_host=$proxmox_host"
        log_info "Updated details for node $node: VM ID=$vm_id, Proxmox Host=$proxmox_host"
      fi
    fi
  done
  
  # Step 8: Detect storage configurations
  log_info "Detecting storage configurations..."
  discover_storage_config "$control_plane_node"
  
  # Step 9: Generate config file
  log_info "Generating configuration file at $output_file..."
  create_config_from_discovery "$output_file"
  
  log_success "Configuration generated at $output_file"
  return 0
}

# Create static sample config (original function)
function create_static_sample_config() {
  local output_file="$1"
  
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
EOF

  log_info "Static sample configuration generated at $output_file"
  return 0
}
