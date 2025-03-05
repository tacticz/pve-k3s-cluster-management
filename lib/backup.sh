#!/bin/bash
# backup.sh - Backup operations module
#
# This module is part of the k3s-cluster-management
# It provides functions for backing up the k3s cluster:
# - Taking etcd snapshots
# - Creating Proxmox VM snapshots
# - Managing backup retention

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
  local is_server=$(ssh root@$first_node "systemctl is-active k3s.service >/dev/null 2>&1 && echo 'true' || echo 'false'" 2>/dev/null)
  
  if [[ "$is_server" != "true" ]]; then
    log_error "Node $first_node is not running k3s server, cannot create etcd snapshot"
    
    # Try to find another node running server
    for node in "${NODES[@]}"; do
      if [[ "$node" != "$first_node" ]]; then
        is_server=$(ssh root@$node "systemctl is-active k3s.service >/dev/null 2>&1 && echo 'true' || echo 'false'" 2>/dev/null)
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
  local snapshot_result=$(ssh root@$first_node "k3s etcd-snapshot save --name $snapshot_name" 2>&1)
  local exit_code=$?
  
  if [[ $exit_code -ne 0 ]]; then
    log_error "Failed to create etcd snapshot: $snapshot_result"
    return 1
  fi
  
  log_success "Etcd snapshot created successfully: $snapshot_name"
  
  # Get snapshot location
  local snapshot_location=$(ssh root@$first_node "ls -la /var/lib/rancher/k3s/server/db/snapshots/$snapshot_name* 2>/dev/null | tail -1" 2>/dev/null)
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
    
    if [[ -n "$PROXMOX_STORAGE" ]]; then
      backup_cmd="$backup_cmd --storage $PROXMOX_STORAGE"
    fi
    
    backup_cmd="$backup_cmd --description \"K3s cluster backup - Node: $node - Timestamp: $TIMESTAMP - Etcd: $etcd_snapshot_name\""
    
    log_info "Running backup command: $backup_cmd"
    local backup_result=$(ssh ${PROXMOX_USER}@$proxmox_host "$backup_cmd" 2>&1)
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
  
  # Validate cluster before snapshot
  validate_cluster || {
    if [[ "$FORCE" != "true" ]]; then
      log_error "Cluster validation failed. Use --force to snapshot anyway."
      return 1
    fi
    log_warn "Continuing snapshot creation despite validation failure due to --force flag"
  }
  
  # Create etcd snapshot first
  local etcd_snapshot_name="${BACKUP_NAME}-etcd"
  create_etcd_snapshot "$etcd_snapshot_name" || {
    if [[ "$FORCE" != "true" ]]; then
      log_error "Failed to create etcd snapshot. Use --force to continue anyway."
      return 1
    fi
    log_warn "Continuing snapshot creation despite etcd snapshot failure due to --force flag"
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
  
  # Create snapshot for each node's VM
  for node in "${NODES[@]}"; do
    local vm_id="${node_vms[$node]}"
    local proxmox_host="${node_hosts[$node]}"
    
    if [[ -z "$vm_id" || -z "$proxmox_host" ]]; then
      continue
    fi
    
    log_info "Creating snapshot for node $node (VM ID: $vm_id on $proxmox_host)..."
    
    if [[ "$DRY_RUN" == "true" ]]; then
      log_info "[DRY RUN] Would create snapshot for VM $vm_id on $proxmox_host"
      continue
    fi
    
    # Create VM snapshot
    local snapshot_name="${BACKUP_NAME}"
    local snapshot_desc="K3s cluster snapshot - Node: $node - Timestamp: $TIMESTAMP - Etcd: $etcd_snapshot_name"
    
    local snapshot_cmd="qm snapshot $vm_id $snapshot_name --description \"$snapshot_desc\""
    
    log_info "Running snapshot command: $snapshot_cmd"
    local snapshot_result=$(ssh ${PROXMOX_USER}@$proxmox_host "$snapshot_cmd" 2>&1)
    local exit_code=$?
    
    if [[ $exit_code -ne 0 ]]; then
      log_error "Failed to create snapshot for VM $vm_id: $snapshot_result"
      if [[ "$FORCE" != "true" ]]; then
        return 1
      fi
    else
      log_success "Snapshot for VM $vm_id created successfully"
    fi
  done
  
  # Clean up old snapshots based on retention policy
  clean_old_snapshots
  
  log_success "Cluster snapshots created successfully"
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
      local backups=$(ssh ${PROXMOX_USER}@$proxmox_host "find /var/lib/vz/dump -name \"vzdump-qemu-${vm_id}-*.vma*\" -o -name \"vzdump-qemu-${vm_id}-*.tgz*\" | sort -r" 2>/dev/null)
      
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
          ssh ${PROXMOX_USER}@$proxmox_host "rm -f $backup" &>/dev/null
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
    local snapshots=$(ssh ${PROXMOX_USER}@$proxmox_host "qm listsnapshot $vm_id | grep -v current | grep -v Name | awk '{print \$1}' | grep -E '^${BACKUP_PREFIX}-[0-9]{8}-[0-9]{6}\$' | sort -r" 2>/dev/null)
    
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
        ssh ${PROXMOX_USER}@$proxmox_host "qm delsnapshot $vm_id $snapshot" &>/dev/null
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
  local is_server=$(ssh root@$first_node "systemctl is-active k3s.service >/dev/null 2>&1 && echo 'true' || echo 'false'" 2>/dev/null)
  
  if [[ "$is_server" != "true" ]]; then
    # Try to find another node running server
    for node in "${NODES[@]}"; do
      if [[ "$node" != "$first_node" ]]; then
        is_server=$(ssh root@$node "systemctl is-active k3s.service >/dev/null 2>&1 && echo 'true' || echo 'false'" 2>/dev/null)
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
  local snapshots=$(ssh root@$first_node "ls -1 /var/lib/rancher/k3s/server/db/snapshots/ | grep -E '^${BACKUP_PREFIX}-[0-9]{8}-[0-9]{6}-etcd' | sort -r" 2>/dev/null)
  
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
      ssh root@$first_node "rm -f /var/lib/rancher/k3s/server/db/snapshots/$snapshot" &>/dev/null
    done
  else
    log_info "No old etcd snapshots to delete (count: $count, retention: $RETENTION_COUNT)"
  fi
  
  return 0
}
