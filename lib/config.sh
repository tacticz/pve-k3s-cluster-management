#!/bin/bash
# config.sh - Configuration module for k3s-cluster-admin
#
# This module is part of the k3s-cluster-management
# It handles loading and processing configuration from YAML files,
# command-line arguments, and default values.

# Global variables for discovery
declare -a NODES
declare -A NODE_DETAILS
declare -a PROXMOX_HOSTS
PROXMOX_STORAGE=""
BACKUP_STORAGE=""
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

# Detect node architecture
function detect_node_architecture() {
  local node="$1"
  
  if verify_ssh_access "$node" "$port"; then
    # Try to get architecture using uname
    local arch=$(ssh_cmd_quiet "$node" "uname -m" "root")
    
    # Map architecture to amd64 or arm64
    case "$arch" in
      x86_64)
        echo "amd64"
        ;;
      aarch64)
        echo "arm64"
        ;;
      *)
        # Default to amd64 if unknown
        log_warn "Unknown architecture: $arch, defaulting to amd64"
        echo "amd64"
        ;;
    esac
  else
    # Default to amd64 if can't SSH
    log_warn "Cannot detect architecture for $node, defaulting to amd64"
    echo "amd64"
  fi
}

# Discover cluster nodes
function discover_cluster_nodes() {
  local control_node="$1"
  
  # Get nodes using kubectl
  log_info "Getting nodes via kubectl from $control_node..."
  local node_list=$(ssh_cmd_quiet "$control_node" "kubectl get nodes -o=jsonpath='{range .items[*]}{.metadata.name}{\"\\n\"}{end}'" "$PROXMOX_USER")
  
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
    local node_ip=$(ssh_cmd_quiet "$control_node" "kubectl get node $node -o=jsonpath='{.status.addresses[?(@.type==\"InternalIP\")].address}'" "$PROXMOX_USER")
    
    # Get node role (master or worker)
    local is_master=$(ssh_cmd_quiet "$control_node" "kubectl get node $node -o=jsonpath='{.metadata.labels.node-role\\.kubernetes\\.io/master}'" "$PROXMOX_USER")
    local is_controlplane=$(ssh_cmd_quiet "$control_node" "kubectl get node $node -o=jsonpath='{.metadata.labels.node-role\\.kubernetes\\.io/control-plane}'" "$PROXMOX_USER")
    
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
    
    # Detect node architecture
    local arch=$(detect_node_architecture "$node" "$SSH_PORT")
    log_info "Node $node: IP=$node_ip, Role=$role, Architecture=$arch"

    # Store basic node details - VM ID and host will be added later
    NODE_DETAILS["$node"]="ip=$node_ip,role=$role,proxmox_vmid=,proxmox_host=,arch=$arch"
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
        local arch="amd64"  # Default to amd64 if not found

        IFS=',' read -ra detail_parts <<< "$details"
        for part in "${detail_parts[@]}"; do
          key="${part%%=*}"
          value="${part#*=}"
          
          case "$key" in
            ip) ip="$value" ;;
            role) role="$value" ;;
            proxmox_vmid) vm_id="$value" ;;
            proxmox_host) proxmox_host="$value" ;;
            arch) arch="$value" ;;  # Extract architecture from existing details
          esac
        done
        
        NODE_DETAILS["$node"]="ip=$ip,role=$role,proxmox_vmid=$vm_id,proxmox_host=$proxmox_host,arch=$arch"
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
        discover_proxmox_storage_details
        # If that failed, fall back to the standard discovery
        if [[ -z "$PROXMOX_STORAGE" || -z "$BACKUP_STORAGE" ]]; then
          log_info "Falling back to basic storage discovery..."
          discover_proxmox_storage
        fi

        if [[ -z "$BACKUP_STORAGE" ]]; then
          # If no specific backup storage was found, look for backup storage in name
          for host in "${PROXMOX_HOSTS[@]}"; do
            if verify_ssh_access "$host" "$SSH_PORT"; then
              local storage_output=$(ssh_cmd_quiet "$host" "pvesm status" "$PROXMOX_USER")
              if [[ -n "$storage_output" ]]; then
                local backup_storage=$(echo "$storage_output" | grep -E "backup" | awk '{print $1}' | head -1)
                if [[ -n "$backup_storage" ]]; then
                  BACKUP_STORAGE="$backup_storage"
                  log_info "Found backup storage: $BACKUP_STORAGE"
                  break
                fi
              fi
            fi
          done
        fi
        
        # Try to discover templates
        discover_proxmox_templates
        return 0
      fi
    fi
  fi
  
  # If we're not on a Proxmox host or couldn't get cluster info, try another approach
  if command -v pvesh &>/dev/null; then
    # Use pvesh to get node list
    log_info "Using pvesh to discover Proxmox cluster nodes..."
    local nodes_json=$(pvesh get /nodes --output-format json 2>/dev/null)
    
    if [[ -n "$nodes_json" ]]; then
      # Extract node names
      local hosts=$(echo "$nodes_json" | grep -o '"node":"[^"]*"' | cut -d':' -f2 | tr -d '"' | sort | uniq)
      
      if [[ -n "$hosts" ]]; then
        readarray -t PROXMOX_HOSTS <<< "$hosts"
        log_info "Discovered Proxmox hosts using pvesh: ${PROXMOX_HOSTS[*]}"
        
        # Try to get storage info
        discover_proxmox_storage
        
        # Try to discover templates
        discover_proxmox_templates
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
    
    # Try to discover templates
    discover_proxmox_templates
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
    
    # Try to discover templates
    discover_proxmox_templates
  else
    log_warn "No Proxmox hosts specified, skipping Proxmox integration"
  fi
  
  return 0
}

# Discover Proxmox VM templates
function discover_proxmox_templates() {
  log_info "Discovering Proxmox VM templates..."
  
  # Initialize template variables
  TEMPLATE_MASTER_AMD64=""
  TEMPLATE_WORKER_AMD64=""
  TEMPLATE_FULL_AMD64=""
  TEMPLATE_MASTER_ARM64=""
  TEMPLATE_WORKER_ARM64=""
  TEMPLATE_FULL_ARM64=""
  
  # Try to get templates using pvesh
  if command -v pvesh &>/dev/null; then
    local vm_resources=$(pvesh get /cluster/resources --type vm --output-format json 2>/dev/null)
    
    if [[ -n "$vm_resources" ]]; then
      # Find template VMs
      local templates=$(echo "$vm_resources" | grep -o '{[^}]*"template":1[^}]*}')
      
      if [[ -n "$templates" ]]; then
        log_info "Found template VMs:"
        
        # Process each template
        while read -r template; do
          local template_id=$(echo "$template" | grep -o '"vmid":[0-9]*' | cut -d':' -f2)
          local template_name=$(echo "$template" | grep -o '"name":"[^"]*"' | cut -d':' -f2 | tr -d '"')
          
          log_info "  $template_id: $template_name"
          
          # Determine architecture (assume amd64 if not specified)
          local arch="amd64"
          if [[ "$template_name" == *"arm"* || "$template_name" == *"arm64"* ]]; then
            arch="arm64"
          fi
          
          # Look for keywords to identify template type
          if [[ "$template_name" == *"master"* || "$template_name" == *"control"* ]]; then
            if [[ "$arch" == "amd64" ]]; then
              TEMPLATE_MASTER_AMD64="$template_id"
              log_info "  ^ Identified as amd64 master template"
            else
              TEMPLATE_MASTER_ARM64="$template_id"
              log_info "  ^ Identified as arm64 master template"
            fi
          elif [[ "$template_name" == *"worker"* || "$template_name" == *"node"* ]]; then
            if [[ "$arch" == "amd64" ]]; then
              TEMPLATE_WORKER_AMD64="$template_id"
              log_info "  ^ Identified as amd64 worker template"
            else
              TEMPLATE_WORKER_ARM64="$template_id"
              log_info "  ^ Identified as arm64 worker template"
            fi
          elif [[ "$template_name" == *"full"* || "$template_name" == *"all"* ]]; then
            if [[ "$arch" == "amd64" ]]; then
              TEMPLATE_FULL_AMD64="$template_id"
              log_info "  ^ Identified as amd64 full node template"
            else
              TEMPLATE_FULL_ARM64="$template_id"
              log_info "  ^ Identified as arm64 full node template"
            fi
          else
            # Default assignment if no specific match
            if [[ "$arch" == "amd64" ]]; then
              if [[ -z "$TEMPLATE_FULL_AMD64" ]]; then
                TEMPLATE_FULL_AMD64="$template_id"
                log_info "  ^ Using as default amd64 template"
              fi
            else
              if [[ -z "$TEMPLATE_FULL_ARM64" ]]; then
                TEMPLATE_FULL_ARM64="$template_id"
                log_info "  ^ Using as default arm64 template"
              fi
            fi
          fi
        done <<< "$templates"
      else
        log_warn "No template VMs found in Proxmox"
      fi
    fi
  fi
  
  # If no templates found automatically, use fallbacks
  # For amd64
  if [[ -z "$TEMPLATE_FULL_AMD64" ]]; then
    if [[ -n "$TEMPLATE_MASTER_AMD64" ]]; then
      TEMPLATE_FULL_AMD64="$TEMPLATE_MASTER_AMD64"
    elif [[ -n "$TEMPLATE_WORKER_AMD64" ]]; then
      TEMPLATE_FULL_AMD64="$TEMPLATE_WORKER_AMD64"
    else
      read -p "Enter template VM ID for amd64 nodes: " TEMPLATE_FULL_AMD64
      TEMPLATE_FULL_AMD64="${TEMPLATE_FULL_AMD64:-9000}"  # Default if nothing entered
    fi
  fi
  
  # Use full template as fallback for specific templates if not found
  TEMPLATE_MASTER_AMD64="${TEMPLATE_MASTER_AMD64:-$TEMPLATE_FULL_AMD64}"
  TEMPLATE_WORKER_AMD64="${TEMPLATE_WORKER_AMD64:-$TEMPLATE_FULL_AMD64}"
  
  # For arm64
  if [[ -z "$TEMPLATE_FULL_ARM64" ]]; then
    if [[ -n "$TEMPLATE_MASTER_ARM64" ]]; then
      TEMPLATE_FULL_ARM64="$TEMPLATE_MASTER_ARM64"
    elif [[ -n "$TEMPLATE_WORKER_ARM64" ]]; then
      TEMPLATE_FULL_ARM64="$TEMPLATE_WORKER_ARM64"
    else
      read -p "Enter template VM ID for arm64 nodes (leave empty to skip): " TEMPLATE_FULL_ARM64
      TEMPLATE_FULL_ARM64="${TEMPLATE_FULL_ARM64:-}"  # No default for arm64
    fi
  fi
  
  # Use full template as fallback for specific templates if not found
  if [[ -n "$TEMPLATE_FULL_ARM64" ]]; then
    TEMPLATE_MASTER_ARM64="${TEMPLATE_MASTER_ARM64:-$TEMPLATE_FULL_ARM64}"
    TEMPLATE_WORKER_ARM64="${TEMPLATE_WORKER_ARM64:-$TEMPLATE_FULL_ARM64}"
  fi
  
  log_info "Using template IDs:"
  log_info "  AMD64: Full=$TEMPLATE_FULL_AMD64, Master=$TEMPLATE_MASTER_AMD64, Worker=$TEMPLATE_WORKER_AMD64"
  if [[ -n "$TEMPLATE_FULL_ARM64" ]]; then
    log_info "  ARM64: Full=$TEMPLATE_FULL_ARM64, Master=$TEMPLATE_MASTER_ARM64, Worker=$TEMPLATE_WORKER_ARM64"
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
      # First look for specific k3s cephfs storage
      local k3s_storage=$(echo "$storage_output" | grep -E "cephfs.*k3s" | awk '{print $1}' | head -1)
      
      if [[ -n "$k3s_storage" ]]; then
        PROXMOX_STORAGE="$k3s_storage"
        log_info "Discovered k3s-specific CephFS storage: $PROXMOX_STORAGE"
        return 0
      fi
      
      # If no k3s-specific storage, look for any cephfs storage
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
      local storage_output=$(ssh_cmd_quiet "$host" "pvesm status" "$PROXMOX_USER")
      
      if [[ -n "$storage_output" ]]; then
        # First look for specific k3s cephfs storage
        local k3s_storage=$(echo "$storage_output" | grep -E "cephfs.*k3s" | awk '{print $1}' | head -1)
        
        if [[ -n "$k3s_storage" ]]; then
          PROXMOX_STORAGE="$k3s_storage"
          log_info "Discovered k3s-specific CephFS storage: $PROXMOX_STORAGE"
          return 0
        fi
        
        # If no k3s-specific storage, look for any cephfs storage
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

      # Look for backup-specific storage
      local backup_storage=$(echo "$storage_output" | grep -E "cephfs.*backup|backup" | awk '{print $1}' | head -1)

      if [[ -n "$backup_storage" ]]; then
        BACKUP_STORAGE="$backup_storage"
        log_info "Discovered Proxmox backup storage: $BACKUP_STORAGE"
        return 0
      fi
    fi
  done
  
  # If still no storage found, ask user
  if [[ -z "$PROXMOX_STORAGE" ]]; then
    log_warn "Could not discover Proxmox storage"
    read -p "Enter Proxmox storage name for k3s storage (cephfs preferred): " PROXMOX_STORAGE
    if [[ -n "$PROXMOX_STORAGE" ]]; then
      log_info "Using storage: $PROXMOX_STORAGE"
    fi
  fi
  
  return 0
}

# Discover Proxmox storage details
function discover_proxmox_storage_details() {
  log_info "Getting detailed storage information from Proxmox..."
  
  local storage_json=""
  
  # Try to get storage info using pvesh locally first
  if command -v pvesh &>/dev/null; then
    storage_json=$(pvesh get /storage --output-format json 2>/dev/null)
  fi
  
  # If local command failed, try via SSH to one of the Proxmox hosts
  if [[ -z "$storage_json" ]] && [[ ${#PROXMOX_HOSTS[@]} -gt 0 ]]; then
    for host in "${PROXMOX_HOSTS[@]}"; do
      if verify_ssh_access "$host" "$SSH_PORT"; then
        storage_json=$(ssh_cmd_quiet "$host" "pvesh get /storage --output-format json" "$PROXMOX_USER")
        if [[ -n "$storage_json" ]]; then
          break
        fi
      fi
    done
  fi
  
  if [[ -z "$storage_json" ]]; then
    log_warn "Failed to get storage details using pvesh"
    return 1
  fi
  
  # Parse JSON to find suitable storage
  # Look for storage with 'backup' in content for BACKUP_STORAGE
  local backup_storage=$(echo "$storage_json" | grep -o '{[^}]*"content":[^}]*backup[^}]*}' | grep -o '"storage":"[^"]*"' | cut -d ':' -f2 | tr -d '"' | head -1)
  if [[ -n "$backup_storage" ]]; then
    BACKUP_STORAGE="$backup_storage"
    log_info "Found storage with backup content type: $BACKUP_STORAGE"
  fi
  
  # Look for storage with 'images' in content for PROXMOX_STORAGE (VM storage)
  local vm_storage=$(echo "$storage_json" | grep -o '{[^}]*"content":[^}]*images[^}]*}' | grep -o '"storage":"[^"]*"' | cut -d ':' -f2 | tr -d '"' | head -1)
  if [[ -n "$vm_storage" ]]; then
    # If there's a storage with 'k3s' in the name, prioritize it
    if echo "$storage_json" | grep -q '{[^}]*"storage":"[^"]*k3s[^"]*"[^}]*"content":[^}]*images[^}]*}'; then
      local k3s_storage=$(echo "$storage_json" | grep -o '{[^}]*"storage":"[^"]*k3s[^"]*"[^}]*"content":[^}]*images[^}]*}' | grep -o '"storage":"[^"]*"' | cut -d ':' -f2 | tr -d '"' | head -1)
      PROXMOX_STORAGE="$k3s_storage"
      log_info "Found k3s-specific storage for VMs: $PROXMOX_STORAGE"
    else
      PROXMOX_STORAGE="$vm_storage"
      log_info "Found general VM storage: $PROXMOX_STORAGE"
    fi
  fi
  
  return 0
}

# Discover storage configuration
function discover_storage_config() {
  local node="$1"
  
  # Check for CephFS mount
  local cephfs_mount=$(ssh_cmd_quiet "$node" "mount | grep cephfs" "$PROXMOX_USER")
  
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
    local arch="amd64"  # Default to amd64 if not found
    
    for part in "${detail_parts[@]}"; do
      key="${part%%=*}"
      value="${part#*=}"
      
      case "$key" in
        ip) ip="$value" ;;
        role) role="$value" ;;
        proxmox_vmid) vmid="$value" ;;
        proxmox_host) proxmox_host="$value" ;;
        arch) arch="$value" ;;
      esac
    done
    
    cat >> "$output_file" <<EOF
  $node:
    ip: $ip
    proxmox_vmid: $vmid
    proxmox_host: $proxmox_host
    role: $role
    arch: $arch
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
    # AMD64 templates
    master_amd64: $TEMPLATE_MASTER_AMD64  # Template VM ID for amd64 master node
    worker_amd64: $TEMPLATE_WORKER_AMD64  # Template VM ID for amd64 worker node
    full_amd64: $TEMPLATE_FULL_AMD64      # Template VM ID for amd64 full node
EOF

  # Only add ARM64 templates if we have them
  if [[ -n "$TEMPLATE_FULL_ARM64" ]]; then
    cat >> "$output_file" <<EOF
    # ARM64 templates
    master_arm64: $TEMPLATE_MASTER_ARM64  # Template VM ID for arm64 master node
    worker_arm64: $TEMPLATE_WORKER_ARM64  # Template VM ID for arm64 worker node
    full_arm64: $TEMPLATE_FULL_ARM64      # Template VM ID for arm64 full node
EOF
  fi

  if [[ -z "$BACKUP_STORAGE" ]]; then
    # If no specific backup storage was found, use the same as PROXMOX_STORAGE as fallback
    BACKUP_STORAGE="$PROXMOX_STORAGE"
    log_info "No specific backup storage found, using: $BACKUP_STORAGE as fallback"
  fi

  cat >> "$output_file" <<EOF

# Backup settings
backup:
  prefix: k3s-backup
  storage: $BACKUP_STORAGE
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
  BACKUP_STORAGE=$(yq -r '.backup.storage // ""' "$CONFIG_FILE")
  
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
  api_server=$(ssh_cmd_quiet "$control_plane_node" "cat /etc/rancher/k3s/k3s.yaml | grep server: | awk '{print \$2}'" "$PROXMOX_USER")
  log_info "Detected API server: $api_server"
  
  # Step 6: Attempt to discover Proxmox VM details first if running on a Proxmox host
  log_info "Attempting to discover VM details from Proxmox..."
  discover_node_proxmox_details

  # Step 7: Discover Proxmox hosts if needed
  log_info "Discovering Proxmox hosts..."
  discover_proxmox_hosts

  # Now that we have hosts, discover templates if not already discovered
  if [[ -z "$TEMPLATE_MASTER_AMD64" ]]; then
    log_info "Discovering VM templates..."
    discover_proxmox_templates
  fi

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

  # Ensure useer is prompted for backup storage if none was detected
  log_info "Detecting backup storage..."
  if [[ -z "$BACKUP_STORAGE" ]]; then
    read -p "Enter Proxmox storage for backups (leave empty to use $PROXMOX_STORAGE): " backup_storage_input
    if [[ -n "$backup_storage_input" ]]; then
      BACKUP_STORAGE="$backup_storage_input"
    else
      BACKUP_STORAGE="$PROXMOX_STORAGE"
    fi
  fi
  log_info "Using backup storage: $BACKUP_STORAGE"
  
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
  storage: pvecephfs-1-backup
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
