#!/bin/bash
# node_ops.sh - Node operations module
#
# This module is part of the k3s-cluster-management
# It provides functions for managing k3s nodes:
# - Cordoning and draining
# - Shutting down nodes
# - Adding/removing nodes from the cluster

# Get all nodes in the cluster
function get_all_cluster_nodes() {
  local first_node="${NODES[0]}"
  
  # If no nodes are configured yet, try to get nodes from config file
  if [[ -z "$first_node" ]]; then
    # Return config nodes if available
    if [[ ${#CONFIG_NODES[@]} -gt 0 ]]; then
      echo "${CONFIG_NODES[@]}"
      return 0
    fi
    
    # Try to get nodes from one of the Proxmox hosts
    if [[ ${#PROXMOX_HOSTS[@]} -gt 0 ]]; then
      local proxmox_host="${PROXMOX_HOSTS[0]}"
      
      # Try to get VM list with 'k3s' in the name
      local vms=$(ssh_cmd_quiet "$proxmox_host" "qm list | grep k3s | awk '{print \$2}'" "$PROXMOX_USER")
      
      if [[ -n "$vms" ]]; then
        echo "$vms"
        return 0
      fi
    fi
    
    log_error "Could not determine cluster nodes"
    return 1
  fi
  
  # Get nodes from the cluster using kubectl
  local nodes=$(ssh_cmd_quiet "$first_node" "kubectl get nodes -o=jsonpath='{.items[*].metadata.name}'" "$PROXMOX_USER")
  
  if [[ -z "$nodes" ]]; then
    log_error "Could not get nodes from the cluster"
    return 1
  fi
  
  echo "$nodes"
  return 0
}

# Cordon a node (mark as unschedulable)
function cordon_node() {
  local node="$1"
  
  log_info "Cordoning node $node..."
  
  # Use any node that's not the target to run kubectl
  local kubectl_node=""
  for n in "${NODES[@]}"; do
    if [[ "$n" != "$node" ]]; then
      kubectl_node="$n"
      break
    fi
  done
  
  if [[ -z "$kubectl_node" ]]; then
    # If only one node, use it
    kubectl_node="${NODES[0]}"
  fi
  
  # Execute cordon command
  if [[ "$DRY_RUN" == "true" ]]; then
    log_info "[DRY RUN] Would cordon node $node"
    return 0
  fi
  
  local result=$(ssh_cmd_capture "$kubectl_node" "kubectl cordon $node" "$PROXMOX_USER")
  local exit_code=$?
  
  if [[ $exit_code -ne 0 ]]; then
    log_error "Failed to cordon node $node: $result"
    return 1
  fi
  
  log_success "Node $node cordoned successfully"
  return 0
}

# Uncordon a node (mark as schedulable)
function uncordon_node() {
  local node="$1"
  
  log_info "Uncordoning node $node..."
  
  # Use any node that's not the target to run kubectl
  local kubectl_node=""
  for n in "${NODES[@]}"; do
    if [[ "$n" != "$node" ]]; then
      kubectl_node="$n"
      break
    fi
  done
  
  if [[ -z "$kubectl_node" ]]; then
    # If only one node, use it
    kubectl_node="${NODES[0]}"
  fi
  
  # Execute uncordon command
  if [[ "$DRY_RUN" == "true" ]]; then
    log_info "[DRY RUN] Would uncordon node $node"
    return 0
  fi
  
  local result=$(ssh_cmd_capture "$kubectl_node" "kubectl uncordon $node" "$PROXMOX_USER")
  local exit_code=$?
  
  if [[ $exit_code -ne 0 ]]; then
    log_error "Failed to uncordon node $node: $result"
    return 1
  fi
  
  log_success "Node $node uncordoned successfully"
  return 0
}

# Drain a node (evict all pods)
function drain_node() {
  local node="$1"
  local force="${2:-false}"
  
  log_info "Draining node $node..."
  
  # Use any node that's not the target to run kubectl
  local kubectl_node=""
  for n in "${NODES[@]}"; do
    if [[ "$n" != "$node" ]]; then
      kubectl_node="$n"
      break
    fi
  done
  
  if [[ -z "$kubectl_node" ]]; then
    log_error "Need at least one other node to drain $node"
    return 1
  fi
  
  # Build drain command with appropriate options
  local drain_cmd="kubectl drain $node --ignore-daemonsets"
  
  if [[ "$force" == "true" ]]; then
    drain_cmd="$drain_cmd --force --delete-emptydir-data"
  fi
  
  # Execute drain command
  if [[ "$DRY_RUN" == "true" ]]; then
    log_info "[DRY RUN] Would drain node $node with command: $drain_cmd"
    return 0
  fi
  
  log_info "Running drain command: $drain_cmd"
  log_info "This may take some time, timeout set to $DRAINING_TIMEOUT seconds..."
  
  # Run with timeout
  local result=$(ssh_cmd_capture "$kubectl_node" "timeout $DRAINING_TIMEOUT $drain_cmd" "$PROXMOX_USER")
  local exit_code=$?
  
  if [[ $exit_code -ne 0 ]]; then
    # If timeout, ask user if they want to force drain
    if [[ $exit_code -eq 124 ]]; then # 124 is the exit code for timeout
      log_warn "Draining operation timed out after $DRAINING_TIMEOUT seconds"
      
      if [[ "$force" != "true" ]] && [[ "$FORCE" != "true" ]] && [[ "$INTERACTIVE" == "true" ]]; then
        read -p "Do you want to force drain with --force --delete-emptydir-data? (y/n): " force_response
        if [[ "$force_response" == "y" ]]; then
          drain_node "$node" "true"
          return $?
        fi
      elif [[ "$FORCE" == "true" ]]; then
        log_info "Force flag set, attempting force drain..."
        drain_node "$node" "true"
        return $?
      fi
    fi
    
    log_error "Failed to drain node $node: $result"
    return 1
  fi
  
  log_success "Node $node drained successfully"
  return 0
}

# Shutdown a node
function shutdown_node() {
  if [[ ${#NODES[@]} -eq 0 ]]; then
    log_error "No nodes specified to shutdown"
    return 1
  fi
  
  # Run pre-flight checks
  run_preflight_checks || return 1
  
  # Validate cluster health before doing anything
  validate_cluster || {
    if [[ "$FORCE" != "true" ]]; then
      log_error "Cluster validation failed. Use --force to continue anyway."
      return 1
    else
      log_warn "Continuing despite validation failure due to --force flag"
    fi
  }
  
  for node in "${NODES[@]}"; do
    log_section "Shutting down node $node"
    
    # Get node details from config
    local vm_id=$(yq -r ".node_details.$node.proxmox_vmid // \"\"" "$CONFIG_FILE")
    local proxmox_host=$(yq -r ".node_details.$node.proxmox_host // \"\"" "$CONFIG_FILE")
    
    if [[ -z "$vm_id" || -z "$proxmox_host" ]]; then
      log_error "Could not find VM ID or Proxmox host for node $node in config"
      continue
    fi
    
    # Check if node is part of control plane
    local is_control_plane=$(ssh_cmd_quiet "$node" "systemctl is-active k3s.service >/dev/null 2>&1 && echo 'true' || echo 'false'" "$PROXMOX_USER")
    
    if [[ "$is_control_plane" == "true" ]]; then
      log_info "Node $node is part of the control plane"
      
      # Check if we have enough control plane nodes
      local control_plane_count=0
      for n in "${NODES[@]}"; do
        if [[ "$n" != "$node" ]]; then
          local node_role=$(ssh_cmd_quiet "$n" "systemctl is-active k3s.service >/dev/null 2>&1 && echo 'control' || echo 'worker'" "$PROXMOX_USER")
          if [[ "$node_role" == "control" ]]; then
            ((control_plane_count++))
          fi
        fi
      done
      
      if [[ $control_plane_count -lt 1 ]]; then
        log_error "Cannot shutdown $node: At least one other control plane node must be available"
        continue
      fi
    fi
    
    # 1. Cordon the node
    cordon_node "$node" || {
      if [[ "$FORCE" != "true" ]]; then
        log_error "Failed to cordon node $node. Aborting shutdown."
        continue
      else
        log_warn "Continuing despite cordon failure due to --force flag"
      fi
    }
    
    # 2. Drain the node
    drain_node "$node" || {
      if [[ "$FORCE" != "true" ]]; then
        log_error "Failed to drain node $node. Aborting shutdown."
        # Uncordon the node since we're not continuing
        uncordon_node "$node"
        continue
      else
        log_warn "Continuing despite drain failure due to --force flag"
      fi
    }
    
    # 3. Stop k3s service on the node
    log_info "Stopping k3s service on $node..."
    if [[ "$DRY_RUN" != "true" ]]; then
      ssh_cmd "$node" "systemctl stop k3s.service k3s-agent.service 2>/dev/null" "root"
    else
      log_info "[DRY RUN] Would stop k3s service on $node"
    fi
    
    # 4. Shutdown the VM via Proxmox
    log_info "Shutting down VM $vm_id on $proxmox_host..."
    
    if [[ "$DRY_RUN" != "true" ]]; then
      local shutdown_result=$(ssh_cmd_capture "$proxmox_host" "qm shutdown $vm_id --timeout 180" "$PROXMOX_USER")
      local exit_code=$?
      
      if [[ $exit_code -ne 0 ]]; then
        log_error "Failed to shutdown VM $vm_id: $shutdown_result"
        
        if [[ "$FORCE" == "true" ]]; then
          log_warn "Force flag set, attempting to stop VM..."
          ssh_cmd_silent "$proxmox_host" "qm stop $vm_id" "$PROXMOX_USER"
        fi
      else
        log_success "VM $vm_id shutdown initiated"
      fi
    else
      log_info "[DRY RUN] Would shutdown VM $vm_id on $proxmox_host"
    fi
  done
  
  log_success "Node shutdown operations completed"
  return 0
}

# Start a previously shutdown node
function start_node() {
  local node="$1"
  
  # Get node details from config
  local vm_id=$(yq -r ".node_details.$node.proxmox_vmid // \"\"" "$CONFIG_FILE")
  local proxmox_host=$(yq -r ".node_details.$node.proxmox_host // \"\"" "$CONFIG_FILE")
  
  if [[ -z "$vm_id" || -z "$proxmox_host" ]]; then
    log_error "Could not find VM ID or Proxmox host for node $node in config"
    return 1
  fi
  
  log_info "Starting VM $vm_id on $proxmox_host..."
  
  if [[ "$DRY_RUN" != "true" ]]; then
    local start_result=$(ssh_cmd_capture "$proxmox_host" "qm start $vm_id" "$PROXMOX_USER")
    local exit_code=$?
    
    if [[ $exit_code -ne 0 ]]; then
      log_error "Failed to start VM $vm_id: $start_result"
      return 1
    fi
    
    log_success "VM $vm_id started successfully"
    
    # Wait for the node to be reachable
    log_info "Waiting for $node to be reachable..."
    local timeout=300
    local count=0
    while [[ $count -lt $timeout ]]; do
      if ssh -o BatchMode=yes -o ConnectTimeout=5 root@$node "echo 'OK'" &>/dev/null; then
        log_success "Node $node is now reachable"
        
        # Wait for k3s service to be up
        log_info "Waiting for k3s service to be up..."
        ssh_cmd_silent "$node" "timeout 60 bash -c 'until systemctl is-active k3s.service k3s-agent.service 2>/dev/null; do sleep 5; done'" "root"
        
        # Uncordon the node
        uncordon_node "$node"
        return 0
      fi
      
      sleep 5
      ((count+=5))
      log_info "Still waiting for $node... (${count}s/${timeout}s)"
    done
    
    log_error "Timed out waiting for $node to be reachable"
    return 1
  else
    log_info "[DRY RUN] Would start VM $vm_id on $proxmox_host"
    return 0
  fi
}

# Replace a node in the cluster
function replace_node() {
  if [[ ${#NODES[@]} -eq 0 ]]; then
    log_error "No node specified to replace"
    return 1
  fi
  
  # For node replacement, we only handle one node at a time
  local node="${NODES[0]}"
  
  log_section "Replacing node $node"
  
  # Run pre-flight checks
  run_preflight_checks || return 1
  
  # Validate cluster health before doing anything
  validate_cluster || {
    if [[ "$FORCE" != "true" ]]; then
      log_error "Cluster validation failed. Use --force to continue anyway."
      return 1
    else
      log_warn "Continuing despite validation failure due to --force flag"
    fi
  }
  
  # Get node details from config
  local vm_id=$(yq -r ".node_details.$node.proxmox_vmid // \"\"" "$CONFIG_FILE")
  local proxmox_host=$(yq -r ".node_details.$node.proxmox_host // \"\"" "$CONFIG_FILE")
  local node_ip=$(yq -r ".node_details.$node.ip // \"\"" "$CONFIG_FILE")
  local node_role=$(yq -r ".node_details.$node.role // \"worker\"" "$CONFIG_FILE")
  
  if [[ -z "$vm_id" || -z "$proxmox_host" || -z "$node_ip" ]]; then
    log_error "Could not find VM ID, Proxmox host, or IP for node $node in config"
    return 1
  fi
  
  # Step 1: Shutdown the node properly
  log_info "Step 1: Shutting down the node properly"
  shutdown_node || {
    if [[ "$FORCE" != "true" ]]; then
      log_error "Failed to properly shutdown node $node. Aborting replacement."
      return 1
    else
      log_warn "Continuing despite shutdown failure due to --force flag"
    fi
  }
  
  # Step 2: Take one final backup of the VM
  log_info "Step 2: Taking backup of VM $vm_id before replacement"
  if [[ "$DRY_RUN" != "true" ]]; then
    local backup_name="prereplace-${node}-${TIMESTAMP}"
    local backup_result=$(ssh_cmd_capture "$proxmox_host" "vzdump $vm_id --compress --mode snapshot --storage ${PROXMOX_STORAGE} --tmpdir /tmp --description 'Pre-replacement backup of $node'" "$PROXMOX_USER")
    
    if [[ $? -ne 0 ]]; then
      log_warn "Failed to take backup of VM $vm_id: $backup_result"
      if [[ "$FORCE" != "true" && "$INTERACTIVE" == "true" ]]; then
        read -p "Continue without backup? (y/n): " continue_response
        if [[ "$continue_response" != "y" ]]; then
          log_error "Aborting replacement as requested"
          return 1
        fi
      elif [[ "$FORCE" != "true" ]]; then
        log_error "Failed to take backup and --force not specified. Aborting."
        return 1
      fi
    else
      log_success "Backup of VM $vm_id completed successfully"
    fi
  else
    log_info "[DRY RUN] Would take backup of VM $vm_id before replacement"
  fi
  
  # Step 3: Remove the node from the cluster
  log_info "Step 3: Removing node $node from the k3s cluster"
  
  # Get a working node for kubectl operations
  local kubectl_node=""
  for n in "${NODES[@]}"; do
    if [[ "$n" != "$node" ]]; then
      kubectl_node="$n"
      break
    fi
  done
  
  if [[ -z "$kubectl_node" ]]; then
    log_error "No available node for kubectl operations"
    return 1
  fi
  
  if [[ "$DRY_RUN" != "true" ]]; then
    local delete_result=$(ssh_cmd_capture "$kubectl_node" "kubectl delete node $node" "$PROXMOX_USER")
    
    if [[ $? -ne 0 ]]; then
      log_error "Failed to remove node $node from cluster: $delete_result"
      if [[ "$FORCE" != "true" ]]; then
        log_error "Aborting replacement"
        return 1
      fi
    else
      log_success "Node $node removed from cluster"
    fi
  else
    log_info "[DRY RUN] Would remove node $node from cluster"
  fi
  
  # Step 4: Create new VM or reset existing one
  log_info "Step 4: Recreating or resetting the VM"
  
  # Interactive mode: ask for VM recreation or reset
  local recreate_vm="true"
  if [[ "$INTERACTIVE" == "true" ]]; then
    read -p "Do you want to recreate the VM from template (y) or reset the existing one (n)? (y/n): " recreate_response
    if [[ "$recreate_response" == "n" ]]; then
      recreate_vm="false"
    fi
  fi
  
  if [[ "$recreate_vm" == "true" ]]; then
    # Get template ID from configuration
    local template_id=$(yq -r ".proxmox.templates.${node_role} // \"\"" "$CONFIG_FILE")
    
    if [[ -z "$template_id" ]]; then
      log_error "Could not find template ID for role '$node_role' in config"
      return 1
    fi
    
    if [[ "$DRY_RUN" != "true" ]]; then
      # Delete old VM
      log_info "Deleting old VM $vm_id"
      ssh_cmd_silent "$proxmox_host" "qm destroy $vm_id --purge" "$PROXMOX_USER"
      
      # Clone from template
      log_info "Cloning new VM from template $template_id"
      local clone_result=$(ssh_cmd_capture "$proxmox_host" "qm clone $template_id $vm_id --name $node" "$PROXMOX_USER")
      
      if [[ $? -ne 0 ]]; then
        log_error "Failed to clone VM from template: $clone_result"
        return 1
      fi
      
      # Configure VM network with the same IP
      log_info "Configuring VM network with IP $node_ip"
      # This part depends on your VM configuration method - cloud-init, custom scripts, etc.
      # Example for cloud-init:
      ssh_cmd_silent "$proxmox_host" "qm set $vm_id --ipconfig0 ip=$node_ip/24,gw=10.0.7.1" "$PROXMOX_USER"
    else
      log_info "[DRY RUN] Would recreate VM $vm_id from template $template_id with IP $node_ip"
    fi
  else
    # Reset existing VM
    if [[ "$DRY_RUN" != "true" ]]; then
      log_info "Resetting existing VM $vm_id"
      # This part depends on how you want to reset the VM
      # Example: Roll back to a clean snapshot if available
      local snapshots=$(ssh_cmd_quiet "$proxmox_host" "qm listsnapshot $vm_id" "$PROXMOX_USER" | grep -i clean)
      
      if [[ -n "$snapshots" ]]; then
        local clean_snapshot=$(echo "$snapshots" | head -1 | awk '{print $1}')
        log_info "Rolling back to snapshot $clean_snapshot"
        ssh_cmd_silent "$proxmox_host" "qm rollback $vm_id $clean_snapshot" "$PROXMOX_USER"
      else
        log_warn "No clean snapshot found for VM $vm_id, just restarting"
      fi
    else
      log_info "[DRY RUN] Would reset VM $vm_id"
    fi
  fi
  
  # Step 5: Start the VM
  log_info "Step 5: Starting the VM"
  if [[ "$DRY_RUN" != "true" ]]; then
    ssh_cmd_silent "$proxmox_host" "qm start $vm_id" "$PROXMOX_USER"
    
    # Wait for the node to be reachable
    log_info "Waiting for $node to be reachable..."
    local timeout=300
    local count=0
    while [[ $count -lt $timeout ]]; do
      if ssh -o BatchMode=yes -o ConnectTimeout=5 root@$node "echo 'OK'" &>/dev/null; then
        log_success "Node $node is now reachable"
        break
      fi
      
      sleep 5
      ((count+=5))
      log_info "Still waiting for $node... (${count}s/${timeout}s)"
    done
    
    if [[ $count -ge $timeout ]]; then
      log_error "Timed out waiting for $node to be reachable"
      return 1
    fi
  else
    log_info "[DRY RUN] Would start VM $vm_id and wait for it to be reachable"
  fi
  
  # Step 6: Rejoin the node to the cluster
  log_info "Step 6: Rejoining the node to the k3s cluster"
  
  if [[ "$DRY_RUN" != "true" ]]; then
    # Get cluster token
    local token=$(ssh_cmd_quiet "$kubectl_node" "cat /var/lib/rancher/k3s/server/node-token" "$PROXMOX_USER")
    
    if [[ -z "$token" ]]; then
      log_error "Could not retrieve cluster token from $kubectl_node"
      return 1
    fi
    
    # Install k3s on the new node based on role
    if [[ "$node_role" == "master" || "$node_role" == "control" ]]; then
      log_info "Installing k3s server on $node"
      local install_cmd="curl -sfL https://get.k3s.io | sh -s - server --server https://${kubectl_node}:6443 --token ${token} --tls-san ${node_ip} --flannel-backend=wireguard-native --node-label=\"kubernetes.io/arch=amd64\""
      ssh_cmd_silent "$node" "$install_cmd" "root"
    else
      log_info "Installing k3s agent on $node"
      local install_cmd="curl -sfL https://get.k3s.io | sh -s - agent --server https://${kubectl_node}:6443 --token ${token} --node-label=\"kubernetes.io/arch=amd64\""
      ssh_cmd_silent "$node" "$install_cmd" "root"
    fi
    
    # Wait for the node to join the cluster
    log_info "Waiting for $node to join the cluster..."
    local timeout=300
    local count=0
    while [[ $count -lt $timeout ]]; do
      local node_status=$(ssh_cmd_quiet "$kubectl_node" "kubectl get node $node -o jsonpath='{.status.conditions[?(@.type==\"Ready\")].status}'" "$PROXMOX_USER")
      
      if [[ "$node_status" == "True" ]]; then
        log_success "Node $node has joined the cluster and is Ready"
        break
      fi
      
      sleep 5
      ((count+=5))
      log_info "Still waiting for $node to be Ready... (${count}s/${timeout}s)"
    done
    
    if [[ $count -ge $timeout ]]; then
      log_error "Timed out waiting for $node to join the cluster"
      return 1
    fi
  else
    log_info "[DRY RUN] Would rejoin $node to the k3s cluster"
  fi
  
  # Step 7: Configure CephFS mount on the node
  log_info "Step 7: Configuring CephFS mount on $node"
  
  if [[ "$DRY_RUN" != "true" ]]; then
    # Install required packages
    ssh_cmd_silent "$node" "apt update && apt install -y ceph-common ceph-fuse" "root"
    
    # Create Ceph config
    ssh_cmd_silent "$node" "mkdir -p /etc/ceph" "root"
    
    # Get Ceph key from an existing node
    local ceph_key=$(ssh_cmd_quiet "$kubectl_node" "cat /etc/ceph/ceph.keyring" "$PROXMOX_USER")
    
    if [[ -z "$ceph_key" ]]; then
      log_warn "Could not retrieve Ceph keyring from $kubectl_node"
    else
      ssh_cmd "$node" "cat > /etc/ceph/ceph.keyring << 'EOF'
$ceph_key
EOF
chmod 600 /etc/ceph/ceph.keyring" &>/dev/null
    fi
    
    # Get Ceph config
    local ceph_conf=$(ssh_cmd_quiet "$kubectl_node" "cat /etc/ceph/ceph.conf" "$PROXMOX_USER")
    
    if [[ -z "$ceph_conf" ]]; then
      log_warn "Could not retrieve Ceph config from $kubectl_node"
    else
      ssh_cmd "$node" "cat > /etc/ceph/ceph.conf << 'EOF'
$ceph_conf
EOF" &>/dev/null
    fi
    
    # Create mount point and update fstab
    ssh_cmd_silent "$node" "mkdir -p /mnt/pvecephfs-1-k3s" "root"
    
    # Check if fstab entry already exists
    local fstab_check=$(ssh_cmd_quiet "$node" "grep pvecephfs-1-k3s /etc/fstab" "$PROXMOX_USER")
    
    if [[ -z "$fstab_check" ]]; then
      ssh_cmd "$node" "cat >> /etc/fstab << 'EOF'
# Mount cluster shared cephfs
none /mnt/pvecephfs-1-k3s fuse.ceph ceph.id=admin,ceph.client_fs=pvecephfs-1-k3s,_netdev,defaults 0 0
EOF" &>/dev/null
    fi
    
    # Mount CephFS
    ssh_cmd_silent "$node" "mount /mnt/pvecephfs-1-k3s" "root"
    
    # Verify mount
    local mount_check=$(ssh_cmd_quiet "$node" "mount | grep pvecephfs-1-k3s" "$PROXMOX_USER")
    
    if [[ -z "$mount_check" ]]; then
      log_warn "CephFS mount not verified on $node"
    else
      log_success "CephFS mounted successfully on $node"
    fi
  else
    log_info "[DRY RUN] Would configure CephFS mount on $node"
  fi
  
  # Final validation
  log_info "Final validation of cluster health"
  if [[ "$DRY_RUN" != "true" ]]; then
    validate_cluster
  else
    log_info "[DRY RUN] Would validate cluster health"
  fi
  
  log_success "Node replacement completed successfully"
  return 0
}
