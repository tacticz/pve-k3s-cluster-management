#!/bin/bash
# restore.sh - Restoration module
#
# This module is part of the k3s-cluster-management
# It provides functions for restoring the k3s cluster:
# - Restoring from etcd snapshots
# - Restoring from VM backups/snapshots
# - Coordinating multi-node restores

# Restore etcd from snapshot
function restore_etcd() {
  local snapshot_name="$1"
  local first_node="${NODES[0]}"
  
  log_info "Restoring etcd from snapshot $snapshot_name on $first_node..."
  
  if [[ "$DRY_RUN" == "true" ]]; then
    log_info "[DRY RUN] Would restore etcd from snapshot $snapshot_name"
    return 0
  fi
  
  # Check if node is running k3s server (has etcd)
  local is_server=$(ssh root@$first_node "systemctl is-active k3s.service >/dev/null 2>&1 && echo 'true' || echo 'false'" 2>/dev/null)
  
  if [[ "$is_server" != "true" ]]; then
    log_error "Node $first_node is not running k3s server, cannot restore etcd"
    return 1
  fi
  
  # Verify snapshot exists
  local snapshot_exists=$(ssh root@$first_node "ls -la /var/lib/rancher/k3s/server/db/snapshots/$snapshot_name* 2>/dev/null" 2>/dev/null)
  
  if [[ -z "$snapshot_exists" ]]; then
    log_error "Snapshot $snapshot_name not found on $first_node"
    
    # List available snapshots
    log_info "Available snapshots:"
    ssh root@$first_node "ls -la /var/lib/rancher/k3s/server/db/snapshots/" 2>/dev/null
    
    return 1
  fi
  
  # Before restoring, we need to stop k3s on all nodes
  log_info "Stopping k3s on all nodes before restore..."
  
  for node in "${NODES[@]}"; do
    log_info "Stopping k3s on $node..."
    ssh root@$node "systemctl stop k3s.service k3s-agent.service 2>/dev/null" &>/dev/null
  done
  
  sleep 5 # Give k3s time to stop
  
  # Check if k3s is stopped on all nodes
  for node in "${NODES[@]}"; do
    local status=$(ssh root@$node "systemctl is-active k3s.service k3s-agent.service 2>/dev/null" 2>/dev/null)
    if [[ "$status" == "active" ]]; then
      log_warn "k3s is still running on $node. Attempting to force stop..."
      ssh root@$node "systemctl stop k3s.service k3s-agent.service 2>/dev/null" &>/dev/null
      sleep 2
    fi
  done
  
  # Start with a clean data directory on the first node
  log_info "Preparing data directory on $first_node..."
  ssh root@$first_node "mkdir -p /var/lib/rancher/k3s/server/db/etcd-restore" &>/dev/null
  
  # Restore etcd from snapshot
  log_info "Restoring etcd from snapshot $snapshot_name..."
  local restore_result=$(ssh root@$first_node "K3S_CLUSTER_INIT=true k3s server \
    --cluster-reset \
    --cluster-reset-restore-path=/var/lib/rancher/k3s/server/db/snapshots/$snapshot_name \
    --data-dir=/var/lib/rancher/k3s/server/db/etcd-restore" 2>&1)
  
  local exit_code=$?
  
  if [[ $exit_code -ne 0 ]]; then
    log_error "Failed to restore etcd: $restore_result"
    return 1
  fi
  
  log_success "Etcd restored from snapshot $snapshot_name"
  
  # Restart k3s with the restored data directory
  log_info "Restarting k3s with restored data..."
  ssh root@$first_node "systemctl start k3s.service" &>/dev/null
  
  # Wait for k3s to start
  log_info "Waiting for k3s to start on $first_node..."
  local timeout=180
  local count=0
  while [[ $count -lt $timeout ]]; do
    local status=$(ssh root@$first_node "systemctl is-active k3s.service" 2>/dev/null)
    if [[ "$status" == "active" ]]; then
      log_success "k3s started on $first_node"
      break
    fi
    
    sleep 5
    ((count+=5))
    log_info "Still waiting for k3s to start... (${count}s/${timeout}s)"
  done
  
  if [[ $count -ge $timeout ]]; then
    log_error "Timed out waiting for k3s to start"
    return 1
  fi
  
  # Start k3s on other server nodes
  for node in "${NODES[@]}"; do
    if [[ "$node" != "$first_node" ]]; then
      log_info "Starting k3s on $node..."
      ssh root@$node "systemctl start k3s.service k3s-agent.service 2>/dev/null" &>/dev/null
    fi
  done
  
  log_success "Etcd restore completed successfully"
  return 0
}

# Find the backup file for a specific VM
function find_vm_backup() {
  local vm_id="$1"
  local proxmox_host="$2"
  local backup_name="$3"
  
  # If backup_name is specified, look for that specific backup
  if [[ -n "$backup_name" ]]; then
    log_info "Looking for backup $backup_name for VM $vm_id..."
    local backup_file=$(ssh ${PROXMOX_USER}@$proxmox_host "find /var/lib/vz/dump -name \"vzdump-qemu-${vm_id}-*${backup_name}*\" | sort -r | head -1" 2>/dev/null)
    
    if [[ -n "$backup_file" ]]; then
      echo "$backup_file"
      return 0
    fi
    
    log_warn "Specific backup $backup_name not found for VM $vm_id"
  fi
  
  # Otherwise, get the most recent backup
  log_info "Looking for most recent backup for VM $vm_id..."
  local backup_file=$(ssh ${PROXMOX_USER}@$proxmox_host "find /var/lib/vz/dump -name \"vzdump-qemu-${vm_id}-*.vma*\" -o -name \"vzdump-qemu-${vm_id}-*.tgz*\" | sort -r | head -1" 2>/dev/null)
  
  if [[ -n "$backup_file" ]]; then
    echo "$backup_file"
    return 0
  fi
  
  log_error "No backup found for VM $vm_id"
  return 1
}

# Find the snapshot for a specific VM
function find_vm_snapshot() {
  local vm_id="$1"
  local proxmox_host="$2"
  local snapshot_name="$3"
  
  # If snapshot_name is specified, check if it exists
  if [[ -n "$snapshot_name" ]]; then
    log_info "Checking if snapshot $snapshot_name exists for VM $vm_id..."
    local snapshot_exists=$(ssh ${PROXMOX_USER}@$proxmox_host "qm listsnapshot $vm_id | grep \"^$snapshot_name \"" 2>/dev/null)
    
    if [[ -n "$snapshot_exists" ]]; then
      echo "$snapshot_name"
      return 0
    fi
    
    log_warn "Specific snapshot $snapshot_name not found for VM $vm_id"
  fi
  
  # Otherwise, get the most recent k3s-backup snapshot
  log_info "Looking for most recent k3s-backup snapshot for VM $vm_id..."
  local snapshot=$(ssh ${PROXMOX_USER}@$proxmox_host "qm listsnapshot $vm_id | grep -E '^${BACKUP_PREFIX}-[0-9]{8}-[0-9]{6} ' | sort -r | head -1 | awk '{print \$1}'" 2>/dev/null)
  
  if [[ -n "$snapshot" ]]; then
    echo "$snapshot"
    return 0
  fi
  
  log_error "No suitable snapshot found for VM $vm_id"
  return 1
}

# Extract etcd snapshot info from backup description
function extract_etcd_snapshot_from_backup() {
  local vm_id="$1"
  local proxmox_host="$2"
  local backup_file="$3"
  
  log_info "Extracting etcd snapshot info from backup description..."
  
  # Get backup description
  local description=$(ssh ${PROXMOX_USER}@$proxmox_host "vzdump --list file=\"$backup_file\" | grep 'Description:' | cut -d ':' -f2-" 2>/dev/null)
  
  if [[ -z "$description" ]]; then
    log_warn "No description found for backup $backup_file"
    return 1
  fi
  
  # Extract etcd snapshot name from description
  local etcd_snapshot=$(echo "$description" | grep -o "Etcd: [^ ]*" | cut -d ' ' -f2)
  
  if [[ -z "$etcd_snapshot" ]]; then
    log_warn "No etcd snapshot information found in backup description"
    return 1
  fi
  
  echo "$etcd_snapshot"
  return 0
}

# Extract etcd snapshot info from snapshot description
function extract_etcd_snapshot_from_snapshot() {
  local vm_id="$1"
  local proxmox_host="$2"
  local snapshot_name="$3"
  
  log_info "Extracting etcd snapshot info from snapshot description..."
  
  # Get snapshot description
  local description=$(ssh ${PROXMOX_USER}@$proxmox_host "qm listsnapshot $vm_id | grep \"^$snapshot_name \" | sed 's/^[^ ]* *[^ ]* *//' | grep 'Etcd:'" 2>/dev/null)
  
  if [[ -z "$description" ]]; then
    log_warn "No etcd info found in snapshot $snapshot_name description"
    return 1
  fi
  
  # Extract etcd snapshot name from description
  local etcd_snapshot=$(echo "$description" | grep -o "Etcd: [^ ]*" | cut -d ' ' -f2)
  
  if [[ -z "$etcd_snapshot" ]]; then
    log_warn "No etcd snapshot information found in snapshot description"
    return 1
  fi
  
  echo "$etcd_snapshot"
  return 0
}

# Restore a VM from backup
function restore_vm_from_backup() {
  local node="$1"
  local backup_file="$2"
  
  # Get node details from config
  local vm_id=$(yq -r ".node_details.$node.proxmox_vmid // \"\"" "$CONFIG_FILE")
  local proxmox_host=$(yq -r ".node_details.$node.proxmox_host // \"\"" "$CONFIG_FILE")
  
  if [[ -z "$vm_id" || -z "$proxmox_host" ]]; then
    log_error "Could not find VM ID or Proxmox host for node $node in config"
    return 1
  fi
  
  log_info "Restoring VM $vm_id from backup $backup_file..."
  
  if [[ "$DRY_RUN" == "true" ]]; then
    log_info "[DRY RUN] Would restore VM $vm_id from backup $backup_file"
    return 0
  fi
  
  # Check if VM is running
  local vm_status=$(ssh ${PROXMOX_USER}@$proxmox_host "qm status $vm_id" 2>/dev/null | grep status | awk '{print $2}')
  
  if [[ "$vm_status" == "running" ]]; then
    log_info "VM $vm_id is running, stopping it first..."
    ssh ${PROXMOX_USER}@$proxmox_host "qm stop $vm_id" &>/dev/null
    sleep 5
  fi
  
  # Restore VM from backup
  local restore_cmd="qmrestore $backup_file $vm_id --force"
  log_info "Running restore command: $restore_cmd"
  
  local restore_result=$(ssh ${PROXMOX_USER}@$proxmox_host "$restore_cmd" 2>&1)
  local exit_code=$?
  
  if [[ $exit_code -ne 0 ]]; then
    log_error "Failed to restore VM $vm_id: $restore_result"
    return 1
  fi
  
  log_success "VM $vm_id restored successfully from backup"
  return 0
}

# Restore a VM from snapshot
function restore_vm_from_snapshot() {
  local node="$1"
  local snapshot_name="$2"
  
  # Get node details from config
  local vm_id=$(yq -r ".node_details.$node.proxmox_vmid // \"\"" "$CONFIG_FILE")
  local proxmox_host=$(yq -r ".node_details.$node.proxmox_host // \"\"" "$CONFIG_FILE")
  
  if [[ -z "$vm_id" || -z "$proxmox_host" ]]; then
    log_error "Could not find VM ID or Proxmox host for node $node in config"
    return 1
  fi
  
  log_info "Restoring VM $vm_id to snapshot $snapshot_name..."
  
  if [[ "$DRY_RUN" == "true" ]]; then
    log_info "[DRY RUN] Would restore VM $vm_id to snapshot $snapshot_name"
    return 0
  fi
  
  # Check if VM is running
  local vm_status=$(ssh ${PROXMOX_USER}@$proxmox_host "qm status $vm_id" 2>/dev/null | grep status | awk '{print $2}')
  
  if [[ "$vm_status" == "running" ]]; then
    log_info "VM $vm_id is running, stopping it first..."
    ssh ${PROXMOX_USER}@$proxmox_host "qm stop $vm_id" &>/dev/null
    sleep 5
  fi
  
  # Rollback to snapshot
  local rollback_result=$(ssh ${PROXMOX_USER}@$proxmox_host "qm rollback $vm_id $snapshot_name" 2>&1)
  local exit_code=$?
  
  if [[ $exit_code -ne 0 ]]; then
    log_error "Failed to rollback VM $vm_id to snapshot $snapshot_name: $rollback_result"
    return 1
  fi
  
  log_success "VM $vm_id rolled back to snapshot $snapshot_name successfully"
  return 0
}

# Restore a cluster from snapshot or backup
function restore_cluster() {
  if [[ ${#NODES[@]} -eq 0 ]]; then
    log_error "No nodes specified for restoration"
    return 1
  fi
  
  log_section "Restoring Cluster"
  
  # Check if specific backup/snapshot provided
  local restore_point="$1"
  
  if [[ -z "$restore_point" ]] && [[ "$INTERACTIVE" == "true" ]]; then
    read -p "Enter backup/snapshot name to restore (leave empty for most recent): " restore_point
  fi
  
  # Get etcd snapshot to use for restoration
  local etcd_snapshot=""
  
  # First node to handle
  local first_node="${NODES[0]}"
  
  # Get VM details for first node
  local first_vm_id=$(yq -r ".node_details.$first_node.proxmox_vmid // \"\"" "$CONFIG_FILE")
  local first_proxmox_host=$(yq -r ".node_details.$first_node.proxmox_host // \"\"" "$CONFIG_FILE")
  
  if [[ -z "$first_vm_id" || -z "$first_proxmox_host" ]]; then
    log_error "Could not find VM ID or Proxmox host for node $first_node in config"
    return 1
  fi
  
  # Determine restoration type (backup or snapshot)
  local restore_type=""
  local specific_backup=""
  local specific_snapshot=""
  
  # Try to find the backup file first
  local backup_file=$(find_vm_backup "$first_vm_id" "$first_proxmox_host" "$restore_point")
  
  if [[ -n "$backup_file" ]]; then
    log_info "Found backup file: $backup_file"
    restore_type="backup"
    specific_backup="$backup_file"
    
    # Extract etcd snapshot from backup description
    etcd_snapshot=$(extract_etcd_snapshot_from_backup "$first_vm_id" "$first_proxmox_host" "$backup_file")
  else
    # If backup not found, try snapshot
    local snapshot_name=$(find_vm_snapshot "$first_vm_id" "$first_proxmox_host" "$restore_point")
    
    if [[ -n "$snapshot_name" ]]; then
      log_info "Found snapshot: $snapshot_name"
      restore_type="snapshot"
      specific_snapshot="$snapshot_name"
      
      # Extract etcd snapshot from snapshot description
      etcd_snapshot=$(extract_etcd_snapshot_from_snapshot "$first_vm_id" "$first_proxmox_host" "$snapshot_name")
    else
      log_error "Could not find backup or snapshot for restoration"
      return 1
    fi
  fi
  
  # If using a snapshot or backup with etcd info, check if the etcd snapshot exists
  if [[ -n "$etcd_snapshot" ]]; then
    log_info "Found associated etcd snapshot: $etcd_snapshot"
    
    # Find a node that has this etcd snapshot
    local etcd_node=""
    for node in "${NODES[@]}"; do
      local snapshot_exists=$(ssh root@$node "ls -la /var/lib/rancher/k3s/server/db/snapshots/$etcd_snapshot* 2>/dev/null" 2>/dev/null)
      
      if [[ -n "$snapshot_exists" ]]; then
        etcd_node="$node"
        log_info "Node $etcd_node has the etcd snapshot $etcd_snapshot"
        break
      fi
    done
    
    if [[ -z "$etcd_node" ]]; then
      log_warn "Could not find etcd snapshot $etcd_snapshot on any node"
      
      if [[ "$INTERACTIVE" == "true" ]]; then
        read -p "Continue without etcd snapshot restoration? (y/n): " continue_response
        if [[ "$continue_response" != "y" ]]; then
          log_error "Aborting restoration as requested"
          return 1
        fi
      elif [[ "$FORCE" != "true" ]]; then
        log_error "Could not find etcd snapshot and --force not specified. Aborting."
        return 1
      fi
    fi
  else
    log_warn "No etcd snapshot information found in backup/snapshot"
    
    if [[ "$INTERACTIVE" == "true" ]]; then
      read -p "Continue without etcd snapshot restoration? (y/n): " continue_response
      if [[ "$continue_response" != "y" ]]; then
        log_error "Aborting restoration as requested"
        return 1
      fi
      
      # Ask if user wants to specify an etcd snapshot manually
      read -p "Specify etcd snapshot name manually? (y/n): " specify_etcd
      if [[ "$specify_etcd" == "y" ]]; then
        read -p "Enter etcd snapshot name: " etcd_snapshot
        
        # Find a node that has this etcd snapshot
        for node in "${NODES[@]}"; do
          local snapshot_exists=$(ssh root@$node "ls -la /var/lib/rancher/k3s/server/db/snapshots/$etcd_snapshot* 2>/dev/null" 2>/dev/null)
          
          if [[ -n "$snapshot_exists" ]]; then
            etcd_node="$node"
            log_info "Node $etcd_node has the etcd snapshot $etcd_snapshot"
            break
          fi
        done
        
        if [[ -z "$etcd_node" ]]; then
          log_warn "Could not find etcd snapshot $etcd_snapshot on any node"
        fi
      fi
    fi
  fi
  
  # Step 1: Restore etcd if we have a snapshot
  if [[ -n "$etcd_snapshot" && -n "$etcd_node" ]]; then
    log_info "Step 1: Restoring etcd from snapshot $etcd_snapshot"
    
    # Set first node to etcd node for restoration
    first_node="$etcd_node"
    NODES[0]="$etcd_node"
    
    if [[ "$DRY_RUN" != "true" ]]; then
      restore_etcd "$etcd_snapshot" || {
        if [[ "$FORCE" != "true" ]]; then
          log_error "Failed to restore etcd. Aborting cluster restoration."
          return 1
        fi
        log_warn "Continuing despite etcd restoration failure due to --force flag"
      }
    else
      log_info "[DRY RUN] Would restore etcd from snapshot $etcd_snapshot"
    fi
  else
    log_warn "Skipping etcd restoration (no suitable snapshot found)"
  fi
  
  # Step 2: Restore each VM
  log_info "Step 2: Restoring VMs"
  
  for node in "${NODES[@]}"; do
    log_info "Restoring VM for node $node..."
    
    # Get node details from config
    local vm_id=$(yq -r ".node_details.$node.proxmox_vmid // \"\"" "$CONFIG_FILE")
    local proxmox_host=$(yq -r ".node_details.$node.proxmox_host // \"\"" "$CONFIG_FILE")
    
    if [[ -z "$vm_id" || -z "$proxmox_host" ]]; then
      log_error "Could not find VM ID or Proxmox host for node $node in config"
      continue
    fi
    
    if [[ "$restore_type" == "backup" ]]; then
      # Find backup for this VM
      if [[ "$node" == "$first_node" && -n "$specific_backup" ]]; then
        # Use the already found backup for the first node
        local backup_file="$specific_backup"
      else
        # Find backup for other nodes
        local backup_file=$(find_vm_backup "$vm_id" "$proxmox_host" "$restore_point")
      fi
      
      if [[ -z "$backup_file" ]]; then
        log_error "Could not find backup for VM $vm_id"
        if [[ "$FORCE" != "true" ]]; then
          log_error "Aborting restoration for node $node"
          continue
        fi
        log_warn "Continuing despite backup not found due to --force flag"
      else
        # Restore VM from backup
        if [[ "$DRY_RUN" != "true" ]]; then
          if ! restore_vm_from_backup "$node" "$backup_file"; then
            if [[ "$FORCE" != "true" ]]; then
              log_error "Failed to restore VM for node $node. Aborting."
              continue
            fi
            log_warn "Continuing despite VM restoration failure due to --force flag"
          fi
        else
          log_info "[DRY RUN] Would restore VM $vm_id from backup $backup_file"
        fi
      fi
    elif [[ "$restore_type" == "snapshot" ]]; then
      # Find snapshot for this VM
      if [[ "$node" == "$first_node" && -n "$specific_snapshot" ]]; then
        # Use the already found snapshot for the first node
        local snapshot_name="$specific_snapshot"
      else
        # Find snapshot for other nodes
        local snapshot_name=$(find_vm_snapshot "$vm_id" "$proxmox_host" "$restore_point")
      fi
      
      if [[ -z "$snapshot_name" ]]; then
        log_error "Could not find snapshot for VM $vm_id"
        if [[ "$FORCE" != "true" ]]; then
          log_error "Aborting restoration for node $node"
          continue
        fi
        log_warn "Continuing despite snapshot not found due to --force flag"
      else
        # Restore VM from snapshot
        if [[ "$DRY_RUN" != "true" ]]; then
          if ! restore_vm_from_snapshot "$node" "$snapshot_name"; then
            if [[ "$FORCE" != "true" ]]; then
              log_error "Failed to restore VM for node $node. Aborting."
              continue
            fi
            log_warn "Continuing despite VM restoration failure due to --force flag"
          fi
        else
          log_info "[DRY RUN] Would restore VM $vm_id to snapshot $snapshot_name"
        fi
      fi
    fi
  done
  
  # Step 3: Start VMs
  log_info "Step 3: Starting VMs"
  
  for node in "${NODES[@]}"; do
    log_info "Starting VM for node $node..."
    
    # Get node details from config
    local vm_id=$(yq -r ".node_details.$node.proxmox_vmid // \"\"" "$CONFIG_FILE")
    local proxmox_host=$(yq -r ".node_details.$node.proxmox_host // \"\"" "$CONFIG_FILE")
    
    if [[ -z "$vm_id" || -z "$proxmox_host" ]]; then
      continue
    fi
    
    if [[ "$DRY_RUN" != "true" ]]; then
      ssh ${PROXMOX_USER}@$proxmox_host "qm start $vm_id" &>/dev/null
    else
      log_info "[DRY RUN] Would start VM $vm_id"
    fi
  done
  
  # Step 4: Wait for nodes to come online
  log_info "Step 4: Waiting for nodes to come online..."
  
  if [[ "$DRY_RUN" != "true" ]]; then
    local timeout=300
    local count=0
    
    while [[ $count -lt $timeout ]]; do
      local all_online=true
      
      for node in "${NODES[@]}"; do
        if ! ssh -o BatchMode=yes -o ConnectTimeout=5 root@$node "echo 'OK'" &>/dev/null; then
          all_online=false
          break
        fi
      done
      
      if [[ "$all_online" == "true" ]]; then
        log_success "All nodes are online"
        break
      fi
      
      sleep 10
      ((count+=10))
      log_info "Still waiting for nodes to come online... (${count}s/${timeout}s)"
    done
    
    if [[ $count -ge $timeout ]]; then
      log_warn "Timed out waiting for some nodes to come online"
    fi
  else
    log_info "[DRY RUN] Would wait for nodes to come online"
  fi
  
  # Step 5: Validate cluster health
  log_info "Step 5: Validating cluster health"
  
  if [[ "$DRY_RUN" != "true" ]]; then
    # Wait a bit for k3s services to start
    sleep 30
    
    validate_cluster || {
      log_warn "Cluster validation found issues after restoration"
    }
  else
    log_info "[DRY RUN] Would validate cluster health"
  fi
  
  log_success "Cluster restoration completed"
  return 0
}

# Interactive restoration wizard
function run_restore_wizard() {
  log_section "Interactive Restoration Wizard"
  
  # Step 1: Select restoration type
  echo "1. Restore entire cluster"
  echo "2. Restore individual node"
  echo "3. Restore etcd only"
  read -p "Select restoration type (1-3): " restore_type
  
  case "$restore_type" in
    1)
      # List available backup/snapshot points
      log_info "Listing available backups and snapshots..."
      
      # Get the first node to check for backups/snapshots
      local first_node="${NODES[0]}"
      
      if [[ -z "$first_node" ]]; then
        # Try to get nodes from config
        if [[ ${#CONFIG_NODES[@]} -gt 0 ]]; then
          first_node="${CONFIG_NODES[0]}"
        else
          log_error "No nodes configured. Please specify nodes in config or with --node option."
          return 1
        fi
      fi
      
      # Get node details from config
      local vm_id=$(yq -r ".node_details.$first_node.proxmox_vmid // \"\"" "$CONFIG_FILE")
      local proxmox_host=$(yq -r ".node_details.$first_node.proxmox_host // \"\"" "$CONFIG_FILE")
      
      if [[ -z "$vm_id" || -z "$proxmox_host" ]]; then
        log_error "Could not find VM ID or Proxmox host for node $first_node in config"
        return 1
      fi
      
      # List backups
      echo "Available backups:"
      ssh ${PROXMOX_USER}@$proxmox_host "find /var/lib/vz/dump -name \"vzdump-qemu-${vm_id}-*.vma*\" -o -name \"vzdump-qemu-${vm_id}-*.tgz*\" | sort -r | head -5" 2>/dev/null
      
      # List snapshots
      echo "Available snapshots:"
      ssh ${PROXMOX_USER}@$proxmox_host "qm listsnapshot $vm_id | grep -E '^${BACKUP_PREFIX}-[0-9]{8}-[0-9]{6} '" 2>/dev/null
      
      read -p "Enter backup/snapshot name to restore: " restore_point
      
      # Call restore_cluster with the selected point
      restore_cluster "$restore_point"
      ;;
    
    2)
      # List all nodes
      log_info "Available nodes:"
      for i in "${!NODES[@]}"; do
        echo "$((i+1)). ${NODES[$i]}"
      done
      
      read -p "Select node to restore (1-${#NODES[@]}): " node_index
      
      if [[ -z "$node_index" || ! "$node_index" =~ ^[0-9]+$ || "$node_index" -lt 1 || "$node_index" -gt "${#NODES[@]}" ]]; then
        log_error "Invalid node selection"
        return 1
      fi
      
      local selected_node="${NODES[$((node_index-1))]}"
      
      # Get node details from config
      local vm_id=$(yq -r ".node_details.$selected_node.proxmox_vmid // \"\"" "$CONFIG_FILE")
      local proxmox_host=$(yq -r ".node_details.$selected_node.proxmox_host // \"\"" "$CONFIG_FILE")
      
      if [[ -z "$vm_id" || -z "$proxmox_host" ]]; then
        log_error "Could not find VM ID or Proxmox host for node $selected_node in config"
        return 1
      fi
      
      # List available backups and snapshots for this node
      echo "Available backups for $selected_node:"
      ssh ${PROXMOX_USER}@$proxmox_host "find /var/lib/vz/dump -name \"vzdump-qemu-${vm_id}-*.vma*\" -o -name \"vzdump-qemu-${vm_id}-*.tgz*\" | sort -r | head -5" 2>/dev/null
      
      echo "Available snapshots for $selected_node:"
      ssh ${PROXMOX_USER}@$proxmox_host "qm listsnapshot $vm_id | grep -E '^${BACKUP_PREFIX}-[0-9]{8}-[0-9]{6} '" 2>/dev/null
      
      read -p "Restore from backup (b) or snapshot (s)? " restore_from
      
      if [[ "$restore_from" == "b" ]]; then
        read -p "Enter backup name or leave empty for most recent: " backup_name
        
        # Find backup
        local backup_file=$(find_vm_backup "$vm_id" "$proxmox_host" "$backup_name")
        
        if [[ -z "$backup_file" ]]; then
          log_error "Could not find backup for VM $vm_id"
          return 1
        fi
        
        # Restore VM from backup
        restore_vm_from_backup "$selected_node" "$backup_file"
        
        # Start VM
        log_info "Starting VM..."
        ssh ${PROXMOX_USER}@$proxmox_host "qm start $vm_id" &>/dev/null
        
      elif [[ "$restore_from" == "s" ]]; then
        read -p "Enter snapshot name or leave empty for most recent: " snapshot_name
        
        # Find snapshot
        local found_snapshot=$(find_vm_snapshot "$vm_id" "$proxmox_host" "$snapshot_name")
        
        if [[ -z "$found_snapshot" ]]; then
          log_error "Could not find snapshot for VM $vm_id"
          return 1
        fi
        
        # Restore VM from snapshot
        restore_vm_from_snapshot "$selected_node" "$found_snapshot"
        
        # Start VM
        log_info "Starting VM..."
        ssh ${PROXMOX_USER}@$proxmox_host "qm start $vm_id" &>/dev/null
        
      else
        log_error "Invalid option: $restore_from"
        return 1
      fi
      
      # Wait for node to be online
      log_info "Waiting for node $selected_node to come online..."
      local timeout=180
      local count=0
      while [[ $count -lt $timeout ]]; do
        if ssh -o BatchMode=yes -o ConnectTimeout=5 root@$selected_node "echo 'OK'" &>/dev/null; then
          log_success "Node $selected_node is now online"
          break
        fi
        
        sleep 5
        ((count+=5))
        log_info "Still waiting for $selected_node... (${count}s/${timeout}s)"
      done
      
      if [[ $count -ge $timeout ]]; then
        log_error "Timed out waiting for $selected_node to be reachable"
        return 1
      fi
      ;;
      
    3)
      # Restore etcd only
      log_info "Listing available etcd snapshots..."
      
      # Find a control plane node
      local control_node=""
      
      for node in "${NODES[@]}"; do
        local is_server=$(ssh root@$node "systemctl is-active k3s.service >/dev/null 2>&1 && echo 'true' || echo 'false'" 2>/dev/null)
        
        if [[ "$is_server" == "true" ]]; then
          control_node="$node"
          break
        fi
      done
      
      if [[ -z "$control_node" ]]; then
        log_error "Could not find a control plane node"
        return 1
      fi
      
      # List etcd snapshots
      echo "Available etcd snapshots on $control_node:"
      ssh root@$control_node "ls -la /var/lib/rancher/k3s/server/db/snapshots/" 2>/dev/null
      
      read -p "Enter etcd snapshot name to restore: " etcd_snapshot
      
      if [[ -z "$etcd_snapshot" ]]; then
        log_error "No etcd snapshot specified"
        return 1
      fi
      
      # Restore etcd from snapshot
      restore_etcd "$etcd_snapshot"
      ;;
      
    *)
      log_error "Invalid option: $restore_type"
      return 1
      ;;
  esac
  
  return 0
}
