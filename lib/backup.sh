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

# Create backups of all VMs in the cluster
function backup_cluster() {
  log_section "Backing up Cluster"
  
  # Validate cluster before backup
  validate_cluster || {
    if [[ "$FORCE" != "true" ]]; then
      log_error "Cluster validation failed. Use --force to backup anyway."
      return 1
    fi
    log_warn "Continuing backup despite validation failure due to --force flag"
  }
  
  # Create etcd snapshot first
  local etcd_snapshot_name="${BACKUP_NAME}-etcd"
  create_etcd_snapshot "$etcd_snapshot_name" || {
    if [[ "$FORCE" != "true" ]]; then
      log_error "Failed to create etcd snapshot. Use --force to continue anyway."
      return 1
    fi
    log_warn "Continuing backup despite etcd snapshot failure due to --force flag"
  }
  
  # Get VM IDs and Proxmox hosts for each node
  declare -A node_vms
  declare -A node_hosts
  
  for node in "${NODES[@]}"; do
    local vm_id=$(yq -r ".node_details.$node.proxmox_vmid // \"\"" "$CONFIG_FILE")
    local proxmox_host=$(yq -r ".node_details.$node.proxmox_host // \"\"" "$CONFIG_FILE")
    
    if [[ -z "$vm_id" || -z "$proxmox_host" ]]; then
      log_error "Could not find VM ID or Proxmox host for node $node in config"
      if [[ "$FORCE" != "true" ]]; then
        return 1
      fi
      continue
    fi
    
    node_vms[$node]="$vm_id"
    node_hosts[$node]="$proxmox_host"
  done
  
  # Backup each node's VM
  for node in "${NODES[@]}"; do
    local vm_id="${node_vms[$node]}"
    local proxmox_host="${node_hosts[$node]}"
    
    if [[ -z "$vm_id" || -z "$proxmox_host" ]]; then
      continue
    fi
    
    log_info "Backing up VM for node $node (VM ID: $vm_id on $proxmox_host)..."
    
    if [[ "$DRY_RUN" == "true" ]]; then
      log_info "[DRY RUN] Would backup VM $vm_id on $proxmox_host"
      continue
    fi
    
    # Create backup
    local backup_cmd="vzdump $vm_id --compress --mode snapshot"
    
    local backup_storage=$(yq -r ".backup.storage // \"\"" "$CONFIG_FILE")
    if [[ -z "$backup_storage" ]]; then
      log_warn "No backup storage specified in config. Using default Proxmox storage."
      backup_storage="$PROXMOX_STORAGE"
    fi

    if [[ -n "$backup_storage" ]]; then
      backup_cmd="$backup_cmd --storage $backup_storage"
    fi
    
    backup_cmd="$backup_cmd --description \"K3s cluster backup - Node: $node - Timestamp: $TIMESTAMP - Etcd: $etcd_snapshot_name\""
    
    log_info "Running backup command: $backup_cmd"
    local backup_result=$(ssh_cmd_capture "$proxmox_host" "$backup_cmd" "$PROXMOX_USER")
    local exit_code=$?
    
    if [[ $exit_code -ne 0 ]]; then
      log_error "Failed to backup VM $vm_id: $backup_result"
      if [[ "$FORCE" != "true" ]]; then
        return 1
      fi
    else
      log_success "VM $vm_id backup completed successfully"
    fi
  done
  
  # Clean up old backups based on retention policy
  clean_old_backups
  
  log_success "Cluster backup completed successfully"
  return 0
}

# Create snapshots of all VMs in the cluster
function snapshot_cluster() {
  log_section "Creating Cluster Snapshots"
  
  # Asking about snapshotting the entire cluster is handled in run_interactive_mode
  local snapshot_all_nodes=true
  
  # In non-interactive mode, or if called from run_interactive_mode after node selection
  # we should just proceed with whatever NODES contains
  if [[ "$INTERACTIVE" == "true" && -z "$1" ]]; then
    # Only if directly called in interactive mode and no argument provided
    # (we'll use $1 as a flag to indicate we've been called from run_interactive_mode)
    original_nodes=("${NODES[@]}")
  fi
  
  # Validate cluster before snapshot (only once at the beginning)
  log_subsection "Pre-Snapshot Validation"
  validate_cluster || {
    if [[ "$FORCE" != "true" ]]; then
      log_error "Cluster validation failed. Use --force to snapshot anyway."
      return 1
    fi
    log_warn "Continuing snapshot creation despite validation failure due to --force flag"
  }
  
  # Ask for snapshot name or use default
  log_subsection "Snapshot Configuration"
  local snapshot_specific_name="auto"
  local custom_description=""
  local snapshot_name="untitled"
  if [[ "$INTERACTIVE" == "true" ]]; then
    local name_accepted=false
    
    while [[ "$name_accepted" != "true" ]]; do
      read -p "Enter a specific name for this snapshot (leave empty for 'auto'): " snapshot_input_name
      if [[ -z "$snapshot_input_name" ]]; then
        snapshot_specific_name="auto"
        name_accepted=true
      else
        snapshot_specific_name="$snapshot_input_name"
        
        # Format and validate the snapshot name
        local raw_timestamp=$(date +"%Y%m%d-%H%M%S")
        local expected_name="${snapshot_specific_name}-${CLUSTER_NAME}-${raw_timestamp}"
        local formatted_name=$(format_snapshot_name "$snapshot_specific_name" "$CLUSTER_NAME" "$raw_timestamp")
        
        if [[ "$formatted_name" != "$expected_name" ]]; then
          log_info "Snapshot name adjusted for compatibility: '$formatted_name'"
          # Ask user to confirm the adjusted name
          read -p "Use this snapshot name? (y/n): " confirm_name
          if [[ "$confirm_name" == "y" ]]; then
            snapshot_name="$formatted_name"
            name_accepted=true
          fi
          # If user says no, the loop will continue and ask for a new name
        else
          snapshot_name="$formatted_name"
          name_accepted=true
        fi
      fi
    done
    
    # Ask for custom description
    read -p "Enter a custom description for this snapshot (optional): " custom_description_input
    if [[ -n "$custom_description_input" ]]; then
      custom_description="$custom_description_input"
    fi
  fi
  
  # Create etcd snapshot name from validated snapshot name
  local etcd_snapshot_name="etcd-${snapshot_name}"
  
  log_info "Creating snapshot with name: $snapshot_name"
  
  # Create etcd snapshot first
  log_subsection "Creating etcd Snapshot"
  create_etcd_snapshot "$etcd_snapshot_name" || {
    if [[ "$FORCE" != "true" ]]; then
      log_error "Failed to create etcd snapshot. Use --force to continue anyway."
      return 1
    fi
    log_warn "Continuing snapshot creation despite etcd snapshot failure due to --force flag"
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
  if [[ "$snapshot_all_nodes" == "true" ]]; then
    original_nodes=("${NODES[@]}")
  fi
  
  # Process worker nodes first
  if [[ ${#worker_nodes[@]} -gt 0 ]]; then
    log_subsection "Processing Worker Nodes"
    NODES=("${worker_nodes[@]}")
    
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
    log_operation_step "Creating Snapshots" "worker nodes"
    for node in "${worker_nodes[@]}"; do
      log_info "Creating snapshot for worker node $node..."
      create_vm_snapshot "$node" "$snapshot_name" "$etcd_snapshot_name" "$custom_description" || {
        if [[ "$FORCE" != "true" ]]; then
          log_error "Failed to create snapshot for worker node $node. Aborting."
          NODES=("${original_nodes[@]}")
          return 1
        fi
        log_warn "Continuing despite snapshot failure for worker node $node due to --force flag"
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
    log_info "No worker nodes to process"
  fi
  
  # Process master nodes one at a time
  if [[ ${#master_nodes[@]} -gt 0 ]]; then
    log_subsection "Processing Master Nodes"
    for node in "${master_nodes[@]}"; do
      log_info "Processing master node $node..."
      
      # Set NODES to just this master node for processing
      NODES=("$node")
      
      # Shutdown this master node
      log_operation_step "Shutting Down" "master node $node"
      shutdown_node "true" || {
        if [[ "$FORCE" != "true" ]]; then
          log_error "Failed to shutdown master node $node. Performing cleanup..."
          cleanup_node "$node" "snapshot" "$etcd_snapshot_name"
          NODES=("${original_nodes[@]}")
          return 1
        fi
        log_warn "Continuing despite master node shutdown failure due to --force flag"
      }
      
      # Create snapshot for this master node
      log_operation_step "Creating Snapshot" "master node $node"
      create_vm_snapshot "$node" "$snapshot_name" "$etcd_snapshot_name" "$custom_description" || {
        if [[ "$FORCE" != "true" ]]; then
          log_error "Failed to create snapshot for master node $node. Aborting."
          NODES=("${original_nodes[@]}")
          return 1
        fi
        log_warn "Continuing despite snapshot failure for master node $node due to --force flag"
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
  log_subsection "Post-Snapshot Verification and Cleanup"
  
  # Only run final validation if we processed more than one node
  # (to avoid redundant validation with the per-node validation above)
  if [[ ${#original_nodes[@]} -gt 1 ]]; then
    # Verify cluster health after all snapshots
    log_operation_step "Final Cluster Validation" "after all snapshots"
    validate_cluster || {
      log_warn "Cluster validation after snapshots showed issues. Check cluster state."
    }
  fi

  # Ensure all nodes are uncordoned
  log_operation_step "Ensuring Uncordoned Nodes" "after snapshot operations"
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

  # Clean up old snapshots based on retention policy
  log_operation_step "Cleaning Up Old Snapshots" "based on retention policy"
  clean_old_snapshots
  
  if [[ $uncordon_failures -gt 0 ]]; then
    log_warn "$uncordon_failures nodes may still be cordoned. Manual verification recommended."
  fi
  
  log_success "Cluster snapshots created successfully with name: $snapshot_name"
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