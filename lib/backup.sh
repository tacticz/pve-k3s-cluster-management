#!/bin/bash
# backup.sh - Backup operations module
#
# This module is part of the k3s-cluster-management
# It provides functions for backing up the k3s cluster:
# - Taking etcd snapshots
# - Creating Proxmox VM snapshots
# - Managing backup retention

# Validate and format snapshot name according to Proxmox requirements
# Returns a compliant snapshot name
function format_snapshot_name() {
  local specific_name="$1"
  local cluster_name="$2"
  local timestamp="$3"
  
  # Remove any characters that aren't allowed (only allow A-Z, a-z, 0-9, -, _)
  # We'll trim the string to remove any potential invisible characters
  specific_name=$(echo "$specific_name" | tr -cd 'A-Za-z0-9-_')
  
  # Ensure it starts with a letter
  if [[ ! "$specific_name" =~ ^[A-Za-z] ]]; then
    specific_name="s${specific_name}"
  fi
  
  # Create initial name with user's preferred format
  local snapshot_name="${specific_name}-${cluster_name}-${timestamp}"
  
  # Check if total length exceeds 40 characters
  if [[ ${#snapshot_name} -gt 40 ]]; then
    log_info "Initial snapshot name '${snapshot_name}' exceeds 40 characters"
    
    # Try with shorter timestamp (without seconds)
    local short_timestamp=$(date +"%y%m%d%H%M")
    snapshot_name="${specific_name}-${cluster_name}-${short_timestamp}"
    
    # If still too long, abbreviate cluster name
    if [[ ${#snapshot_name} -gt 40 ]]; then
      # Use first 3 characters of cluster name or less if needed
      local short_cluster="${cluster_name:0:3}"
      snapshot_name="${specific_name}-${short_cluster}-${short_timestamp}"
      
      # If still too long, truncate specific name
      if [[ ${#snapshot_name} -gt 40 ]]; then
        local remaining_space=$((40 - ${#short_cluster} - ${#short_timestamp} - 2)) # 2 for the hyphens
        specific_name="${specific_name:0:$remaining_space}"
        snapshot_name="${specific_name}-${short_cluster}-${short_timestamp}"
      fi
    fi
  fi
  
  # Ensure minimum length of 2 characters
  if [[ ${#snapshot_name} -lt 2 ]]; then
    snapshot_name="sn"
  fi
  
  echo "$snapshot_name"
}

# Create etcd snapshot
function create_etcd_snapshot() {
  local snapshot_name="$1"
  local first_node="${NODES[0]}"
  
  log_info "Creating etcd snapshot on $first_node..."
  
  if [[ "$DRY_RUN" == "true" ]]; then
    log_info "[DRY RUN] Would create etcd snapshot named $snapshot_name"
    return 0
  fi
  
  # Check if node is running k3s server (has etcd)
  local is_server=$(ssh_cmd_quiet "$first_node" "systemctl is-active k3s.service >/dev/null 2>&1 && echo 'true' || echo 'false'" "$PROXMOX_USER")
  
  if [[ "$is_server" != "true" ]]; then
    log_error "Node $first_node is not running k3s server, cannot create etcd snapshot"
    
    # Try to find another node running server
    for node in "${NODES[@]}"; do
      if [[ "$node" != "$first_node" ]]; then
        is_server=$(ssh_cmd_quiet "$node" "systemctl is-active k3s.service >/dev/null 2>&1 && echo 'true' || echo 'false'" "$PROXMOX_USER")
        if [[ "$is_server" == "true" ]]; then
          first_node="$node"
          log_info "Using $first_node for etcd snapshot instead"
          break
        fi
      fi
    done
    
    if [[ "$is_server" != "true" ]]; then
      log_error "No node running k3s server found, cannot create etcd snapshot"
      return 1
    fi
  fi
  
  # Create etcd snapshot
  local snapshot_result=$(ssh_cmd_capture "$first_node" "k3s etcd-snapshot save --name $snapshot_name" "$PROXMOX_USER")
  local exit_code=$?
  
  if [[ $exit_code -ne 0 ]]; then
    log_error "Failed to create etcd snapshot: $snapshot_result"
    return 1
  fi
  
  log_success "Etcd snapshot created successfully: $snapshot_name"
  
  # Get snapshot location
  local snapshot_location=$(ssh_cmd_quiet "$first_node" "ls -la /var/lib/rancher/k3s/server/db/snapshots/$snapshot_name* 2>/dev/null | tail -1" "$PROXMOX_USER")
  log_info "Snapshot stored at: $snapshot_location"
  
  return 0
}

# Helper function to create VM snapshot
function create_vm_snapshot() {
  local node="$1"
  local snapshot_name="$2"
  local etcd_snapshot_name="$3"
  local custom_description="$4"
  
  # Get node details from config
  local vm_id=$(yq -r ".node_details.$node.proxmox_vmid // \"\"" "$CONFIG_FILE")
  local proxmox_host=$(yq -r ".node_details.$node.proxmox_host // \"\"" "$CONFIG_FILE")
  
  if [[ -z "$vm_id" || -z "$proxmox_host" ]]; then
    log_error "Could not find VM ID or Proxmox host for node $node in config"
    return 1
  fi
  
  log_info "Creating snapshot for VM $vm_id on $proxmox_host..."
  
  if [[ "$DRY_RUN" == "true" ]]; then
    log_info "[DRY RUN] Would create snapshot for VM $vm_id"
    return 0
  fi
  
  # Create VM snapshot
  local snapshot_desc="K3s cluster snapshot - Node: $node - Etcd: $etcd_snapshot_name"
  
  # Add custom description if provided
  if [[ -n "$custom_description" ]]; then
    snapshot_desc="${snapshot_desc} - ${custom_description}"
  fi
  
  local snapshot_cmd="qm snapshot $vm_id \"$snapshot_name\" --description \"$snapshot_desc\""
  
  log_info "Running snapshot command: $snapshot_cmd"
  local snapshot_result=$(ssh_cmd_capture "$proxmox_host" "$snapshot_cmd" "$PROXMOX_USER")
  local exit_code=$?
  
  if [[ $exit_code -ne 0 ]]; then
    log_error "Failed to create snapshot for VM $vm_id: $snapshot_result"
    return 1
  fi

  # Verify snapshot was created
  local snapshot_check=$(ssh_cmd_capture "$proxmox_host" "qm listsnapshot $vm_id | grep -w \"$snapshot_name\"" "$PROXMOX_USER")
  if [[ -z "$snapshot_check" ]]; then
    log_error "Snapshot command appeared to succeed but snapshot '$snapshot_name' not found"
    return 1
  fi
  
  log_success "Snapshot for VM $vm_id created successfully"
  return 0
}

# Helper function to get the appropriate storage for backups
function get_backup_storage() {
  # First check if backup storage is defined in config
  local backup_storage=$(yq -r ".backup.storage // \"\"" "$CONFIG_FILE")
  
  # If not defined, try to discover a suitable backup storage
  if [[ -z "$backup_storage" ]]; then
    log_warn "No backup storage specified in config. Attempting to auto-detect..."
    
    # Try to get storage information from a Proxmox host
    if [[ ${#PROXMOX_HOSTS[@]} -gt 0 ]]; then
      local host="${PROXMOX_HOSTS[0]}"
      
      # Use pvesh to get detailed storage information
      local storage_json=$(ssh_cmd_quiet "$host" "pvesh get /storage --output-format json" "$PROXMOX_USER")
      
      if [[ -n "$storage_json" ]]; then
        # Look for storage with backup in content
        local backup_storage_candidates=$(echo "$storage_json" | grep -o '{[^}]*"content":[^}]*backup[^}]*}' | grep -o '"storage":"[^"]*"' | cut -d':' -f2 | tr -d '"' | tr -d ' ')
        
        if [[ -n "$backup_storage_candidates" ]]; then
          # Take the first backup-capable storage
          backup_storage=$(echo "$backup_storage_candidates" | head -1)
          log_info "Auto-detected backup-capable storage: $backup_storage"
        else
          # If no dedicated backup storage found, check if PROXMOX_STORAGE supports backups
          if [[ -n "$PROXMOX_STORAGE" ]]; then
            local proxmox_storage_content=$(echo "$storage_json" | grep -o "{[^}]*\"storage\":\"$PROXMOX_STORAGE\"[^}]*}" | grep -o '"content":"[^"]*"' | cut -d':' -f2 | tr -d '"')
            
            if [[ "$proxmox_storage_content" == *"backup"* ]]; then
              backup_storage="$PROXMOX_STORAGE"
              log_info "Verified PROXMOX_STORAGE '$PROXMOX_STORAGE' supports backups"
            else
              log_warn "PROXMOX_STORAGE '$PROXMOX_STORAGE' does not support backups"
              
              # Look for any storage with backup capability
              local any_backup_storage=$(echo "$storage_json" | grep -o '{[^}]*"content":[^}]*backup[^}]*}' | head -1 | grep -o '"storage":"[^"]*"' | cut -d':' -f2 | tr -d '"' | tr -d ' ')
              
              if [[ -n "$any_backup_storage" ]]; then
                backup_storage="$any_backup_storage"
                log_info "Found alternative backup-capable storage: $backup_storage"
              fi
            fi
          fi
        fi
      else
        # Fallback to simple pvesm command if pvesh fails
        local storage_output=$(ssh_cmd_quiet "$host" "pvesm status" "$PROXMOX_USER")
        if [[ -n "$storage_output" ]]; then
          # Look for backup in name or type
          local backup_candidate=$(echo "$storage_output" | grep -i "backup" | awk '{print $1}' | head -1)
          
          if [[ -n "$backup_candidate" ]]; then
            backup_storage="$backup_candidate"
            log_info "Found storage with backup in name: $backup_storage"
          fi
        fi
      fi
    fi
  fi
  
  # If still no storage found and we're in interactive mode, ask user
  if [[ -z "$backup_storage" && "$INTERACTIVE" == "true" ]]; then
    # Show available storages to the user
    if [[ ${#PROXMOX_HOSTS[@]} -gt 0 ]]; then
      local host="${PROXMOX_HOSTS[0]}"
      local storage_list=$(ssh_cmd_quiet "$host" "pvesm status" "$PROXMOX_USER")
      
      echo "Available storage:"
      echo "$storage_list"
      echo ""
    fi
    
    read -p "Please select a storage for backups: " user_storage
    if [[ -n "$user_storage" ]]; then
      backup_storage="$user_storage"
    else
      log_error "No storage specified for backups"
      return 1
    fi
  fi
  
  # Final fallback if still not found
  if [[ -z "$backup_storage" ]]; then
    log_error "Could not determine a suitable backup storage"
    return 1
  fi
    
  # Only return the storage name, nothing else
  printf "%s" "$backup_storage"
  return 0
}

# Helper function to create VM snapshot or backup
function create_vm_point_in_time() {
  local node="$1"
  local operation_label="$2"
  local etcd_snapshot_name="$3"
  local custom_description="$4"
  local operation_type="$5"  # 'snapshot' or 'backup'
  
  # Get node details from config
  local vm_id=$(yq -r ".node_details.$node.proxmox_vmid // \"\"" "$CONFIG_FILE")
  local proxmox_host=$(yq -r ".node_details.$node.proxmox_host // \"\"" "$CONFIG_FILE")
  
  if [[ -z "$vm_id" || -z "$proxmox_host" ]]; then
    log_error "Could not find VM ID or Proxmox host for node $node in config"
    return 1
  fi
  
  log_info "Creating $operation_type for VM $vm_id on $proxmox_host..."
  
  if [[ "$DRY_RUN" == "true" ]]; then
    log_info "[DRY RUN] Would create $operation_type for VM $vm_id"
    return 0
  fi
  
  # Create description
  local desc=""
  
  if [[ "$operation_type" == "snapshot" ]]; then
    # For snapshots, keep the etcd reference and more detailed format
    desc="K3s cluster $operation_type - Node: $node - Etcd: $etcd_snapshot_name"
    
    # Add custom description if provided
    if [[ -n "$custom_description" ]]; then
      desc="${desc} - ${custom_description}"
    fi
  else
    # For backups, use Proxmox-style format
    desc="{{node}} - {{vmid}} ({{guestname}})"
    
    # Add custom description if provided
    if [[ -n "$custom_description" ]]; then
      desc="${desc} - ${custom_description}"
    fi
  fi
  
  # Execute the appropriate command based on operation type
  if [[ "$operation_type" == "snapshot" ]]; then
    # Create VM snapshot
    local cmd="qm snapshot $vm_id \"$operation_label\" --description \"$desc\""
    
    log_info "Running snapshot command: $cmd"
    local result=$(ssh_cmd_capture "$proxmox_host" "$cmd" "$PROXMOX_USER")
    local exit_code=$?
    
    if [[ $exit_code -ne 0 ]]; then
      log_error "Failed to create snapshot for VM $vm_id: $result"
      return 1
    fi

    # Verify snapshot was created
    local check=$(ssh_cmd_capture "$proxmox_host" "qm listsnapshot $vm_id | grep -w \"$operation_label\"" "$PROXMOX_USER")
    if [[ -z "$check" ]]; then
      log_error "Snapshot command appeared to succeed but snapshot '$operation_label' not found"
      return 1
    fi
  else
    # Create VM backup (vzdump)
    # Get backup storage
    log_info "Getting backup storage for $node..."
    local backup_storage=$(get_backup_storage)
    if [[ -z "$backup_storage" ]]; then
      log_error "No valid backup storage found or specified"
      return 1
    fi
    
    log_info "Using backup storage: '${backup_storage}'"
    
    # Build the vzdump command according to man page 
    # --mode stop: automatically stop/start the VM
    local backup_cmd="vzdump ${vm_id} --compress zstd --mode stop --storage ${backup_storage}"
    backup_cmd="${backup_cmd} --notes-template \"${desc}\""
    
    log_info "Running backup command: $backup_cmd"
    log_info "This operation may take several minutes..."
    
    # Use ssh_cmd function to execute command - this handles host keys properly
    local result=$(ssh_cmd "$proxmox_host" "$backup_cmd" "root" "capture")
    local exit_code=$?
    
    if [[ $exit_code -ne 0 ]]; then
      log_error "Failed to backup VM $vm_id: $result"
      return 1
    fi
    
    # Check for success indicators in the output
    if echo "$result" | grep -q "Backup job finished successfully"; then
      log_success "Backup completed successfully according to vzdump output"
      
      # Extract backup filename if present in output
      local backup_file=$(echo "$result" | grep -o "creating vzdump archive '[^']*'" | cut -d "'" -f 2)
      if [[ -n "$backup_file" ]]; then
        log_info "Backup file created: $backup_file"
      fi
    # Also check for partial success indicators
    elif echo "$result" | grep -q "creating vzdump archive"; then
      log_success "Backup appears to have started successfully"
      log_info "Check Proxmox storage for the backup file"
    else
      log_warn "Could not confirm successful backup completion from vzdump output"
      log_warn "Check Proxmox logs or storage for verification"
    fi
  fi
  
  log_success "$operation_type for VM $vm_id created successfully"
  return 0
}

# Create snapshots or backups of all VMs in the cluster
function create_cluster_point_in_time() {
  local operation_type="${1:-snapshot}"  # Default to snapshot if not specified
  
  # Validate operation type
  if [[ "$operation_type" != "snapshot" && "$operation_type" != "backup" ]]; then
    log_error "Invalid operation type: $operation_type (must be 'snapshot' or 'backup')"
    return 1
  fi
  
  local operation_name
  local operation_verb
  
  if [[ "$operation_type" == "snapshot" ]]; then
    operation_name="Snapshots"
    operation_verb="Snapshotting"
  else
    operation_name="Backups"
    operation_verb="Backing up"
  fi
  
  log_section "Creating Cluster $operation_name"
  
  # Asking about the operation for the entire cluster is handled in run_interactive_mode
  local process_all_nodes=true
  
  # In non-interactive mode, or if called from run_interactive_mode after node selection
  # we should just proceed with whatever NODES contains
  if [[ "$INTERACTIVE" == "true" && -z "$2" ]]; then
    # Only if directly called in interactive mode and no second argument provided
    # (we'll use $2 as a flag to indicate we've been called from run_interactive_mode)
    original_nodes=("${NODES[@]}")
  fi
  
  # Validate cluster before operation (only once at the beginning)
  log_subsection "Pre-Operation Validation"
  validate_cluster || {
    if [[ "$FORCE" != "true" ]]; then
      log_error "Cluster validation failed. Use --force to $operation_type anyway."
      return 1
    fi
    log_warn "Continuing $operation_type creation despite validation failure due to --force flag"
  }
  
  # Ask for operation name or use default
  log_subsection "$operation_name Configuration"
  local specific_name="auto"
  local custom_description=""
  local operation_label="untitled"
  if [[ "$INTERACTIVE" == "true" ]]; then
    local name_accepted=false
    
    while [[ "$name_accepted" != "true" ]]; do
      read -p "Enter a specific name for this $operation_type (leave empty for 'auto'): " input_name
      if [[ -z "$input_name" ]]; then
        specific_name="auto"
        name_accepted=true
      else
        specific_name="$input_name"
        
        # Format and validate the name
        local raw_timestamp=$(date +"%Y%m%d-%H%M%S")
        local expected_name="${specific_name}-${CLUSTER_NAME}-${raw_timestamp}"
        local formatted_name=$(format_snapshot_name "$specific_name" "$CLUSTER_NAME" "$raw_timestamp")
        
        if [[ "$formatted_name" != "$expected_name" ]]; then
          log_info "$operation_name name adjusted for compatibility: '$formatted_name'"
          # Ask user to confirm the adjusted name
          read -p "Use this name? (y/n): " confirm_name
          if [[ "$confirm_name" == "y" ]]; then
            operation_label="$formatted_name"
            name_accepted=true
          fi
          # If user says no, the loop will continue and ask for a new name
        else
          operation_label="$formatted_name"
          name_accepted=true
        fi
      fi
    done
    
    # Ask for custom description
    read -p "Enter a custom description for this $operation_type (optional): " custom_description_input
    if [[ -n "$custom_description_input" ]]; then
      custom_description="$custom_description_input"
    fi
  else
    # In non-interactive mode, use BACKUP_NAME as the operation label
    operation_label="${BACKUP_PREFIX}-${TIMESTAMP}"
  fi
  
  # Create etcd snapshot name from validated operation name
  local etcd_snapshot_name="etcd-${operation_label}"
  
  log_info "Creating $operation_type with name: $operation_label"
  
  # Create etcd snapshot first
  log_subsection "Creating etcd Snapshot"
  create_etcd_snapshot "$etcd_snapshot_name" || {
    if [[ "$FORCE" != "true" ]]; then
      log_error "Failed to create etcd snapshot. Use --force to continue anyway."
      return 1
    fi
    log_warn "Continuing $operation_type creation despite etcd snapshot failure due to --force flag"
  }
  
  # Sort nodes by role - workers first, then masters
  declare -a worker_nodes
  declare -a master_nodes
  
  for node in "${NODES[@]}"; do
    local role=$(yq -r ".node_details.$node.role // \"worker\"" "$CONFIG_FILE")
    
    if [[ "$role" == "master" || "$role" == "control" ]]; then
      master_nodes+=("$node")
    else
      worker_nodes+=("$node")
    fi
  done
  
  log_info "Processing worker nodes first: ${worker_nodes[*]}"
  log_info "Then processing master nodes: ${master_nodes[*]}"
  
  # Save original nodes array
  if [[ "$process_all_nodes" == "true" ]]; then
    original_nodes=("${NODES[@]}")
  fi
  
  # Process worker nodes first
  if [[ ${#worker_nodes[@]} -gt 0 ]]; then
    log_subsection "Processing Worker Nodes"
    NODES=("${worker_nodes[@]}")
    
    if [[ "$operation_type" == "snapshot" ]]; then
      # For snapshots, we need to handle shutdown/start manually
      # Shutdown worker nodes
      log_info "Shutting down worker nodes..."
      shutdown_node "true" || {
        if [[ "$FORCE" != "true" ]]; then
          log_error "Failed to shutdown worker nodes. Aborting."
          NODES=("${original_nodes[@]}")
          return 1
        fi
        log_warn "Continuing despite worker node shutdown failure due to --force flag"
      }
      
      # Create snapshots for worker nodes
      log_operation_step "$operation_verb" "worker nodes"
      for node in "${worker_nodes[@]}"; do
        log_info "Creating $operation_type for worker node $node..."
        create_vm_point_in_time "$node" "$operation_label" "$etcd_snapshot_name" "$custom_description" "$operation_type" || {
          if [[ "$FORCE" != "true" ]]; then
            log_error "Failed to create $operation_type for worker node $node. Aborting."
            NODES=("${original_nodes[@]}")
            return 1
          fi
          log_warn "Continuing despite $operation_type failure for worker node $node due to --force flag"
        }
      done
      
      # Start worker nodes
      log_operation_step "Starting" "worker nodes"
      for node in "${worker_nodes[@]}"; do
        log_info "Starting worker node $node..."
        start_node "$node" || {
          if [[ "$FORCE" != "true" ]]; then
            log_error "Failed to start worker node $node. Aborting."
            NODES=("${original_nodes[@]}")
            return 1
          fi
          log_warn "Continuing despite start failure for worker node $node due to --force flag"
        }
      done
    else
      # For backups, vzdump handles VM shutdown/start
      # Just need to cordon and drain nodes, then stop k3s
      log_info "Preparing worker nodes for backup..."
      
      for node in "${worker_nodes[@]}"; do
        log_info "Cordoning worker node $node..."
        cordon_node "$node" || {
          if [[ "$FORCE" != "true" ]]; then
            log_error "Failed to cordon worker node $node. Aborting."
            NODES=("${original_nodes[@]}")
            return 1
          fi
          log_warn "Continuing despite cordon failure for worker node $node due to --force flag"
        }
        
        log_info "Draining worker node $node..."
        drain_node "$node" || {
          if [[ "$FORCE" != "true" ]]; then
            log_error "Failed to drain worker node $node. Aborting."
            # Uncordon the node since we're not continuing
            uncordon_node "$node"
            NODES=("${original_nodes[@]}")
            return 1
          fi
          log_warn "Continuing despite drain failure for worker node $node due to --force flag"
        }
        
        # Stop k3s service on the node
        log_info "Stopping k3s service on $node..."
        if ! stop_k3s_service "$node" "$FORCE"; then
          log_error "Failed to stop k3s service on $node. Aborting."
          # Uncordon the node since we're not continuing
          uncordon_node "$node"
          NODES=("${original_nodes[@]}")
          return 1
        fi
      done
      
      # Create backups for worker nodes - vzdump will handle VM shutdown/start
      log_operation_step "$operation_verb" "worker nodes"
      for node in "${worker_nodes[@]}"; do
        log_info "Creating $operation_type for worker node $node..."
        create_vm_point_in_time "$node" "$operation_label" "$etcd_snapshot_name" "$custom_description" "$operation_type" || {
          if [[ "$FORCE" != "true" ]]; then
            log_error "Failed to create $operation_type for worker node $node. Aborting."
            # Try to uncordon node 
            uncordon_node "$node"
            NODES=("${original_nodes[@]}")
            return 1
          fi
          log_warn "Continuing despite $operation_type failure for worker node $node due to --force flag"
        }
        
        # After backup is complete, vzdump has restarted the VM, wait for k3s to be ready
        log_info "Waiting for k3s service to be ready on $node..."
        wait_for_k3s_ready "$node" 180
        
        # Uncordon node
        log_info "Uncordoning worker node $node..."
        uncordon_node "$node" || {
          if [[ "$FORCE" != "true" ]]; then
            log_error "Failed to uncordon worker node $node. Aborting."
            NODES=("${original_nodes[@]}")
            return 1
          fi
          log_warn "Continuing despite uncordon failure for worker node $node due to --force flag"
        }
      done
    fi
  else
    log_info "No worker nodes to process"
  fi
  
  # Process master nodes one at a time
  if [[ ${#master_nodes[@]} -gt 0 ]]; then
    log_subsection "Processing Master Nodes"
    for node in "${master_nodes[@]}"; do
      log_info "Processing master node $node..."
      
      # Set NODES to just this master node for processing
      NODES=("$node")
      
      if [[ "$operation_type" == "snapshot" ]]; then
        # For snapshots, handle shutdown/start manually
        
        # Shutdown this master node
        log_operation_step "Shutting Down" "master node $node"
        shutdown_node "true" || {
          if [[ "$FORCE" != "true" ]]; then
            log_error "Failed to shutdown master node $node. Performing cleanup..."
            cleanup_node "$node" "$operation_type" "$etcd_snapshot_name"
            NODES=("${original_nodes[@]}")
            return 1
          fi
          log_warn "Continuing despite master node shutdown failure due to --force flag"
        }
        
        # Create snapshot for this master node
        log_operation_step "Creating $operation_name" "master node $node"
        create_vm_point_in_time "$node" "$operation_label" "$etcd_snapshot_name" "$custom_description" "$operation_type" || {
          if [[ "$FORCE" != "true" ]]; then
            log_error "Failed to create $operation_type for master node $node. Aborting."
            NODES=("${original_nodes[@]}")
            return 1
          fi
          log_warn "Continuing despite $operation_type failure for master node $node due to --force flag"
        }
        
        # Start this master node
        log_operation_step "Starting" "master node $node"
        start_node "$node" || {
          if [[ "$FORCE" != "true" ]]; then
            log_error "Failed to start master node $node. Aborting."
            NODES=("${original_nodes[@]}")
            return 1
          fi
          log_warn "Continuing despite start failure for master node $node due to --force flag"
        }
      else
        # For backups, vzdump handles VM shutdown/start
        # Prepare node (cordon, drain, stop k3s)
        log_operation_step "Preparing" "master node $node for backup"
        
        # Cordon the node
        log_info "Cordoning master node $node..."
        cordon_node "$node" || {
          if [[ "$FORCE" != "true" ]]; then
            log_error "Failed to cordon master node $node. Aborting."
            NODES=("${original_nodes[@]}")
            return 1
          fi
          log_warn "Continuing despite cordon failure for master node $node due to --force flag"
        }
        
        # Drain the node
        log_info "Draining master node $node..."
        drain_node "$node" || {
          if [[ "$FORCE" != "true" ]]; then
            log_error "Failed to drain master node $node. Aborting."
            # Uncordon the node since we're not continuing
            uncordon_node "$node"
            NODES=("${original_nodes[@]}")
            return 1
          fi
          log_warn "Continuing despite drain failure for master node $node due to --force flag"
        }
        
        # Stop k3s service on the node
        log_info "Stopping k3s service on $node..."
        if ! stop_k3s_service "$node" "$FORCE"; then
          log_error "Failed to stop k3s service on $node. Aborting."
          # Uncordon the node since we're not continuing
          uncordon_node "$node"
          NODES=("${original_nodes[@]}")
          return 1
        fi
        
        # Create backup for this master node - vzdump will handle VM shutdown/start
        log_operation_step "Creating $operation_name" "master node $node"
        create_vm_point_in_time "$node" "$operation_label" "$etcd_snapshot_name" "$custom_description" "$operation_type" || {
          if [[ "$FORCE" != "true" ]]; then
            log_error "Failed to create $operation_type for master node $node. Aborting."
            # Try to uncordon node
            uncordon_node "$node"
            NODES=("${original_nodes[@]}")
            return 1
          fi
          log_warn "Continuing despite $operation_type failure for master node $node due to --force flag"
        }
      fi
      
      # Wait for node initialization
      log_operation_step "Initializing" "master node $node"
      log_info "Waiting for k3s to initialize on $node..."
      wait_for_k3s_ready "$node" 180
      
      # Wait a bit for additional initialization 
      sleep 15
      
      # Uncordon with strict validation before proceeding
      log_operation_step "Uncordoning" "master node $node"
      if ! uncordon_node_with_validation "$node" 15 10; then
        if [[ "$FORCE" != "true" ]]; then
          log_error "Failed to uncordon master node $node. Aborting."
          NODES=("${original_nodes[@]}")
          return 1
        fi
        log_warn "Continuing despite uncordon failure for master node $node due to --force flag"
      fi
      
      # Validate this node is healthy before continuing to next node
      log_operation_step "Validating" "node $node before continuing"
      if ! validate_cluster "basic"; then
        if [[ "$FORCE" != "true" ]]; then
          log_error "Cluster validation failed after processing $node. Aborting."
          NODES=("${original_nodes[@]}")
          return 1
        fi
        log_warn "Continuing despite validation failure after processing $node due to --force flag"
      fi
    done
  else
    log_info "No master nodes to process"
  fi
  
  # Restore original nodes array
  NODES=("${original_nodes[@]}")
  
  # Final verification and cleanup
  log_subsection "Post-Operation Verification and Cleanup"
  
  # Only run final validation if we processed more than one node
  # (to avoid redundant validation with the per-node validation above)
  if [[ ${#original_nodes[@]} -gt 1 ]]; then
    # Verify cluster health after all operations
    log_operation_step "Final Cluster Validation" "after all $operation_name"
    validate_cluster || {
      log_warn "Cluster validation after $operation_name showed issues. Check cluster state."
    }
  fi

  # Ensure all nodes are uncordoned
  log_operation_step "Ensuring Uncordoned Nodes" "after $operation_type operations"
  local uncordon_failures=0

  for node in "${original_nodes[@]}"; do
    # Check if node is cordoned
    if [[ "$DRY_RUN" != "true" ]]; then
      # Use our improved node finder function
      local kubectl_node=$(find_kubectl_node "$node")
      
      if [[ -n "$kubectl_node" ]]; then
        log_info "Using $kubectl_node to check cordon status of $node"
        
        # Check if node is cordoned
        local is_cordoned=$(ssh_cmd_quiet "$kubectl_node" "kubectl get node $node -o jsonpath='{.spec.unschedulable}'" "$PROXMOX_USER")
        
        if [[ "$is_cordoned" == "true" ]]; then
          log_info "Node $node is cordoned, attempting to uncordon..."
          
          # Use improved uncordon function with retries
          if ! uncordon_node "$node" 5 15; then
            log_warn "Failed to uncordon node $node after multiple attempts"
            uncordon_failures=$((uncordon_failures+1))
          fi
        else
          log_info "Node $node is already schedulable"
        fi
      else
        log_warn "No working kubectl node found initially, waiting for services to initialize..."
        
        # If no kubectl node found, wait and try again with the node itself
        sleep 15
        
        if wait_for_k3s_ready "$node" 90; then
          log_info "Node $node now has k3s ready, checking if cordoned..."
          
          # Check if it's cordoned using the node itself
          local self_check=$(ssh_cmd_quiet "$node" "kubectl get node $node -o jsonpath='{.spec.unschedulable}'" "$PROXMOX_USER")
          
          if [[ "$self_check" == "true" ]]; then
            log_info "Node $node is cordoned, attempting self-uncordon..."
            if ssh_cmd_silent "$node" "kubectl uncordon $node" "$PROXMOX_USER"; then
              log_success "Node $node successfully self-uncordoned"
            else
              log_warn "Failed to self-uncordon node $node"
              uncordon_failures=$((uncordon_failures+1))
            fi
          else
            log_info "Node $node appears to be already schedulable"
          fi
        else
          log_warn "K3s service did not become ready on $node"
          uncordon_failures=$((uncordon_failures+1))
        fi
      fi
    else
      log_info "[DRY RUN] Would ensure node $node is uncordoned"
    fi
  done

  # Clean up old items based on retention policy
  log_operation_step "Cleaning Up Old $operation_name" "based on retention policy"
  if [[ "$operation_type" == "snapshot" ]]; then
    clean_old_snapshots
  else
    clean_old_backups
  fi
  
  if [[ $uncordon_failures -gt 0 ]]; then
    log_warn "$uncordon_failures nodes may still be cordoned. Manual verification recommended."
  fi
  
  log_success "Cluster $operation_name created successfully with name: $operation_label"
  return 0
}

# Wrapper function for backward compatibility - snapshots
function snapshot_cluster() {
  local from_interactive="$1"  # Pass this flag to the unified function
  create_cluster_point_in_time "snapshot" "$from_interactive"
  return $?
}

# Wrapper function for backward compatibility - backups
function backup_cluster() {
  local from_interactive="$1"  # Pass this flag to the unified function
  create_cluster_point_in_time "backup" "$from_interactive"
  return $?
}

# Clean up old backups based on retention policy
function clean_old_backups() {
  if [[ "$RETENTION_COUNT" -le 0 ]]; then
    log_info "Backup retention disabled, skipping cleanup"
    return 0
  fi
  
  log_info "Cleaning up old backups based on retention policy (keep: $RETENTION_COUNT)..."
  
  if [[ "$DRY_RUN" == "true" ]]; then
    log_info "[DRY RUN] Would clean up old backups"
    return 0
  fi
  
  # Process each Proxmox host
  for proxmox_host in "${PROXMOX_HOSTS[@]}"; do
    log_info "Checking backups on $proxmox_host..."
    
    # Get list of backup VZDUMPs for each node
    for node in "${NODES[@]}"; do
      local vm_id=$(yq -r ".node_details.$node.proxmox_vmid // \"\"" "$CONFIG_FILE")
      
      if [[ -z "$vm_id" ]]; then
        continue
      fi
      
      # Find backups for this VM
      local backups=$(ssh_cmd_quiet "$proxmox_host" "find /var/lib/vz/dump -name \"vzdump-qemu-${vm_id}-*.vma*\" -o -name \"vzdump-qemu-${vm_id}-*.tgz*\" | sort -r" "$PROXMOX_USER")
      
      if [[ -z "$backups" ]]; then
        log_info "No backups found for VM $vm_id on $proxmox_host"
        continue
      fi
      
      # Keep only the most recent backups based on retention policy
      local count=0
      local to_delete=""
      
      while read -r backup; do
        ((count++))
        if [[ $count -gt $RETENTION_COUNT ]]; then
          to_delete="$to_delete $backup"
        fi
      done <<< "$backups"
      
      # Delete old backups
      if [[ -n "$to_delete" ]]; then
        log_info "Deleting old backups for VM $vm_id: $to_delete"
        for backup in $to_delete; do
          ssh_cmd_silent "$proxmox_host" "rm -f $backup" "$PROXMOX_USER"
        done
      else
        log_info "No old backups to delete for VM $vm_id (count: $count, retention: $RETENTION_COUNT)"
      fi
    done
  done
  
  return 0
}

# Clean up old snapshots based on retention policy
function clean_old_snapshots() {
  if [[ "$RETENTION_COUNT" -le 0 ]]; then
    log_info "Snapshot retention disabled, skipping cleanup"
    return 0
  fi
  
  log_info "Cleaning up old snapshots based on retention policy (keep: $RETENTION_COUNT)..."
  
  if [[ "$DRY_RUN" == "true" ]]; then
    log_info "[DRY RUN] Would clean up old snapshots"
    return 0
  fi
  
  # Process each node
  for node in "${NODES[@]}"; do
    local vm_id=$(yq -r ".node_details.$node.proxmox_vmid // \"\"" "$CONFIG_FILE")
    local proxmox_host=$(yq -r ".node_details.$node.proxmox_host // \"\"" "$CONFIG_FILE")
    
    if [[ -z "$vm_id" || -z "$proxmox_host" ]]; then
      continue
    fi
    
    log_info "Checking snapshots for VM $vm_id on $proxmox_host..."
    
    # Get list of snapshots for this VM
    local snapshots=$(ssh_cmd_quiet "$proxmox_host" "qm listsnapshot $vm_id | grep -v current | grep -v Name | awk '{print \$1}' | grep -E '^${BACKUP_PREFIX}-[0-9]{8}-[0-9]{6}\$' | sort -r" "$PROXMOX_USER")
    
    if [[ -z "$snapshots" ]]; then
      log_info "No snapshots found for VM $vm_id matching pattern ${BACKUP_PREFIX}-*"
      continue
    fi
    
    # Keep only the most recent snapshots based on retention policy
    local count=0
    local to_delete=""
    
    while read -r snapshot; do
      ((count++))
      if [[ $count -gt $RETENTION_COUNT ]]; then
        to_delete="$to_delete $snapshot"
      fi
    done <<< "$snapshots"
    
    # Delete old snapshots
    if [[ -n "$to_delete" ]]; then
      log_info "Deleting old snapshots for VM $vm_id: $to_delete"
      for snapshot in $to_delete; do
        log_info "Deleting snapshot $snapshot..."
        ssh_cmd_silent "$proxmox_host" "qm delsnapshot $vm_id $snapshot" "$PROXMOX_USER"
      done
    else
      log_info "No old snapshots to delete for VM $vm_id (count: $count, retention: $RETENTION_COUNT)"
    fi
  done
  
  # Clean up old etcd snapshots as well
  clean_old_etcd_snapshots
  
  return 0
}

# Clean up old etcd snapshots based on retention policy
function clean_old_etcd_snapshots() {
  local first_node="${NODES[0]}"
  
  # Check if node is running k3s server (has etcd)
  local is_server=$(ssh_cmd_quiet "$first_node" "systemctl is-active k3s.service >/dev/null 2>&1 && echo 'true' || echo 'false'" "$PROXMOX_USER")
  
  if [[ "$is_server" != "true" ]]; then
    # Try to find another node running server
    for node in "${NODES[@]}"; do
      if [[ "$node" != "$first_node" ]]; then
        is_server=$(ssh_cmd_quiet "$node" "systemctl is-active k3s.service >/dev/null 2>&1 && echo 'true' || echo 'false'" "$PROXMOX_USER")
        if [[ "$is_server" == "true" ]]; then
          first_node="$node"
          break
        fi
      fi
    done
    
    if [[ "$is_server" != "true" ]]; then
      log_info "No node running k3s server found, skipping etcd snapshot cleanup"
      return 0
    fi
  fi
  
  log_info "Cleaning up old etcd snapshots on $first_node..."
  
  # List etcd snapshots
  local snapshots=$(ssh_cmd_quiet "$first_node" "ls -1 /var/lib/rancher/k3s/server/db/snapshots/ | grep -E '^${BACKUP_PREFIX}-[0-9]{8}-[0-9]{6}-etcd' | sort -r" "$PROXMOX_USER")
  
  if [[ -z "$snapshots" ]]; then
    log_info "No etcd snapshots found matching pattern ${BACKUP_PREFIX}-*-etcd"
    return 0
  fi
  
  # Keep only the most recent snapshots based on retention policy
  local count=0
  local to_delete=""
  
  while read -r snapshot; do
    ((count++))
    if [[ $count -gt $RETENTION_COUNT ]]; then
      to_delete="$to_delete $snapshot"
    fi
  done <<< "$snapshots"
  
  # Delete old snapshots
  if [[ -n "$to_delete" ]]; then
    log_info "Deleting old etcd snapshots: $to_delete"
    for snapshot in $to_delete; do
      ssh_cmd_silent "$first_node" "rm -f /var/lib/rancher/k3s/server/db/snapshots/$snapshot" "root"
    done
  else
    log_info "No old etcd snapshots to delete (count: $count, retention: $RETENTION_COUNT)"
  fi
  
  return 0
}