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
  
  # Find a kubectl node that's not the target
  local kubectl_node=""
  
  # First try nodes in the current NODES array
  for n in "${NODES[@]}"; do
    if [[ "$n" != "$node" ]]; then
      kubectl_node="$n"
      break
    fi
  done
  
  # If no node found in NODES array, try to get a node from the config
  if [[ -z "$kubectl_node" ]]; then
    # Get all nodes from config
    local all_nodes=$(yq -r '.nodes[]' "$CONFIG_FILE" 2>/dev/null)
    
    # Find a node that's not the target and is accessible
    for potential_node in $all_nodes; do
      if [[ "$potential_node" != "$node" ]]; then
        if ssh_cmd_quiet "$potential_node" "echo Connected" "$PROXMOX_USER" &>/dev/null; then
          kubectl_node="$potential_node"
          log_info "Using $kubectl_node to run kubectl commands"
          break
        fi
      fi
    done
  fi
  
  # If still no other node found, use the node itself as last resort
  if [[ -z "$kubectl_node" ]]; then
    kubectl_node="$node"
    log_warn "No other accessible node found, using $node itself for cordoning"
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

# Function to wait for k3s service to be ready on a node
function wait_for_k3s_ready() {
  local node="$1"
  local timeout="${2:-120}"  # 2 minute timeout
  
  log_info "Waiting for k3s service to be active on $node..."
  local count=0
  
  while [[ $count -lt $timeout ]]; do
    # Check ONLY k3s service, not k3s-agent
    local service_status=$(ssh_cmd_quiet "$node" "systemctl is-active k3s.service 2>/dev/null || echo 'inactive'" "$PROXMOX_USER")
    
    # Trim any whitespace or newlines
    service_status=$(echo "$service_status" | tr -d '[:space:]')
    
    if [[ "$service_status" == "active" ]]; then
      log_success "K3s service is active on $node"
      return 0
    fi
    
    sleep 5
    ((count+=5))
    log_info "Waiting for k3s service to become active... (${count}s/${timeout}s)"
    
    # Every 30 seconds, show more detailed diagnostics
    if (( count % 30 == 0 )); then
      log_info "Checking detailed status on $node after ${count}s..."
      ssh_cmd_quiet "$node" "systemctl status k3s.service | grep 'Active:'" "$PROXMOX_USER"
    fi
  done
  
  # Final check before giving up
  local final_status=$(ssh_cmd_quiet "$node" "systemctl is-active k3s.service 2>/dev/null || echo 'inactive'" "$PROXMOX_USER")
  final_status=$(echo "$final_status" | tr -d '[:space:]')
  
  if [[ "$final_status" == "active" ]]; then
    log_success "K3s service is active on $node after final check"
    return 0
  fi
  
  log_warn "Timed out waiting for k3s service to become active on $node"
  return 1
}

# Enhanced function to find the best node to run kubectl from
function find_kubectl_node() {
  local excluded_node="$1"
  local all_nodes=("${NODES[@]}")
  
  # Try to find a node in the current nodes array first
  for node in "${all_nodes[@]}"; do
    if [[ "$node" != "$excluded_node" ]]; then
      if ssh_cmd_silent "$node" "kubectl get nodes" "$PROXMOX_USER"; then
        echo "$node"
        return 0
      fi
    fi
  done
  
  # If no node found in the current array, try all nodes from config
  log_info "Looking for any available control plane node to run kubectl..."
  local config_nodes=$(yq -r '.nodes[]' "$CONFIG_FILE" 2>/dev/null)
  
  for node in $config_nodes; do
    if [[ "$node" != "$excluded_node" ]]; then
      if ssh_cmd_silent "$node" "kubectl get nodes" "$PROXMOX_USER"; then
        echo "$node"
        return 0
      fi
    fi
  done
  
  # As a last resort, try the node itself, but only if it's ready
  if ssh_cmd_silent "$excluded_node" "kubectl get nodes" "$PROXMOX_USER"; then
    echo "$excluded_node"
    return 0
  fi
  
  return 1
}

# Improved uncordon function with better node selection and retry mechanism
function uncordon_node() {
  local node="$1"
  local max_retries="${2:-3}"  # Allow multiple retries
  local retry_delay="${3:-10}"  # Seconds between retries
  
  log_info "Uncordoning node $node (with up to $max_retries retries)..."
  
  # Find a kubectl node that's not the target
  local kubectl_node=$(find_kubectl_node "$node")
  
  # If no node found, wait briefly and try again
  if [[ -z "$kubectl_node" ]]; then
    log_warn "No node available to run kubectl commands. Waiting 30 seconds before retrying..."
    sleep 30
    kubectl_node=$(find_kubectl_node "$node")
  fi
  
  # If still no node found, check if target node is ready to run kubectl itself
  if [[ -z "$kubectl_node" ]]; then
    log_info "Waiting for $node to be ready to run kubectl itself..."
    wait_for_k3s_ready "$node"
    kubectl_node="$node"
    log_info "Using $node itself for uncordoning"
  fi
  
  # Execute uncordon command with retries
  if [[ "$DRY_RUN" == "true" ]]; then
    log_info "[DRY RUN] Would uncordon node $node using $kubectl_node"
    return 0
  fi
  
  local retry=0
  local success=false
  
  while [[ $retry -lt $max_retries && "$success" != "true" ]]; do
    if [[ $retry -gt 0 ]]; then
      log_info "Retry $retry/$max_retries after $retry_delay seconds..."
      sleep $retry_delay
    fi
    
    log_info "Running uncordon command from $kubectl_node: kubectl uncordon $node"
    local result=$(ssh_cmd_capture "$kubectl_node" "kubectl uncordon $node" "$PROXMOX_USER")
    local exit_code=$?
    
    if [[ $exit_code -eq 0 ]]; then
      log_success "Node $node uncordoned successfully"
      success=true
      break
    else
      log_warn "Attempt $((retry+1)) failed to uncordon node $node: $result"
      ((retry++))
      
      # If we've failed multiple times, check if node status is cordoned
      # It might have already been uncordoned by another process
      if [[ $retry -gt 1 ]]; then
        local is_cordoned=$(ssh_cmd_quiet "$kubectl_node" "kubectl get node $node -o jsonpath='{.spec.unschedulable}'" "$PROXMOX_USER")
        
        if [[ "$is_cordoned" != "true" ]]; then
          log_info "Node $node appears to be already schedulable"
          success=true
          break
        fi
      fi
    fi
  done
  
  if [[ "$success" != "true" ]]; then
    log_error "Failed to uncordon node $node after $max_retries attempts"
    return 1
  fi
  
  return 0
}

# Enhanced uncordon function with validation and retries
function uncordon_node_with_validation() {
  local node="$1"
  local max_retries="${2:-12}"
  local initial_delay="${3:-10}"
  
  log_info "Uncordoning node $node with validation (max retries: $max_retries)..."
  
  # First make sure the node is Ready before attempting to uncordon
  log_info "Waiting for node $node to be in Ready state before uncordoning..."
  check_node_ready "$node" 300
  
  # Even if node isn't fully ready, proceed with uncordon attempts
  local retry=0
  local success=false
  local backoff=$initial_delay
  
  while [[ $retry -lt $max_retries && "$success" != "true" ]]; do
    if [[ $retry -gt 0 ]]; then
      log_info "Retry $retry/$max_retries after $backoff seconds delay..."
      sleep $backoff
      # Increase backoff for next attempt
      backoff=$((backoff * 2))
      if [[ $backoff -gt 120 ]]; then
        backoff=120
      fi
    fi
    
    # Find best kubectl node
    local kubectl_node=""
    for check_node in "${NODES[@]}"; do
      if [[ "$check_node" != "$node" ]] && ssh_cmd_silent "$check_node" "kubectl get nodes" "$PROXMOX_USER"; then
        kubectl_node="$check_node"
        break
      fi
    done
    
    if [[ -n "$kubectl_node" ]]; then
      log_info "Using $kubectl_node to uncordon $node"
      local cmd_result=$(ssh_cmd_capture "$kubectl_node" "kubectl uncordon $node" "$PROXMOX_USER")
      local exit_code=$?
      
      if [[ $exit_code -eq 0 ]]; then
        log_success "Node $node uncordon command succeeded"
        
        # Verify uncordon was successful
        sleep 2
        local is_still_cordoned=$(ssh_cmd_quiet "$kubectl_node" "kubectl get node $node -o jsonpath='{.spec.unschedulable}'" "$PROXMOX_USER")
        
        if [[ "$is_still_cordoned" != "true" ]]; then
          log_success "Node $node successfully uncordoned and verified"
          success=true
          break
        else
          log_warn "Node $node still appears cordoned despite successful command"
        fi
      else
        log_warn "Attempt $((retry+1)) failed to uncordon node $node: $cmd_result"
      fi
    else
      # Try self-uncordon if no other node available and node is Ready
      local self_ready=$(ssh_cmd_quiet "$node" "kubectl get node $node -o jsonpath='{.status.conditions[?(@.type==\"Ready\")].status}'" "$PROXMOX_USER")
      
      if [[ "$self_ready" == "True" ]]; then
        log_info "Node $node appears Ready, attempting self-uncordon"
        if ssh_cmd_silent "$node" "kubectl uncordon $node" "$PROXMOX_USER"; then
          log_success "Node $node successfully self-uncordoned"
          
          # Verify it worked
          sleep 2
          local self_check=$(ssh_cmd_quiet "$node" "kubectl get node $node -o jsonpath='{.spec.unschedulable}'" "$PROXMOX_USER")
          
          if [[ "$self_check" != "true" ]]; then
            log_success "Node $node self-uncordon verified"
            success=true
            break
          fi
        fi
      else
        log_info "Node $node not yet Ready for self-uncordon, waiting..."
      fi
    fi
    
    ((retry++))
  done
  
  if [[ "$success" != "true" ]]; then
    log_error "Failed to uncordon node $node after $max_retries attempts"
    return 1
  fi
  
  return 0
}

# Drain a node (evict all pods)
function drain_node() {
  local node="$1"
  local force="${2:-false}"
  
  log_info "Draining node $node..."
  
  # First try to use another node from the current NODES array
  local kubectl_node=""
  for n in "${NODES[@]}"; do
    if [[ "$n" != "$node" ]]; then
      kubectl_node="$n"
      break
    fi
  done
  
  # If no node found in NODES array, try to get a control plane node from the cluster
  if [[ -z "$kubectl_node" ]]; then
    log_info "Looking for another control plane node to run kubectl from..."
    
    # Get all nodes from config
    local all_nodes=$(yq -r '.nodes[]' "$CONFIG_FILE" 2>/dev/null)
    
    # Find a node that's not the target and is accessible
    for potential_node in $all_nodes; do
      if [[ "$potential_node" != "$node" ]]; then
        if ssh_cmd_quiet "$potential_node" "echo Connected" "$PROXMOX_USER" &>/dev/null; then
          kubectl_node="$potential_node"
          log_info "Using $kubectl_node to run kubectl commands"
          break
        fi
      fi
    done
  fi
  
  # If still no node found, we can't proceed
  if [[ -z "$kubectl_node" ]]; then
    log_error "Need at least one other accessible node to drain $node"
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
  
  # Define parameters
  local wait_for_shutdown="${1:-true}"
  
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
  
  local shutdown_success=true
  
  for node in "${NODES[@]}"; do
    log_section "Shutting down node $node"
    
    # Get node details from config
    local vm_id=$(yq -r ".node_details.$node.proxmox_vmid // \"\"" "$CONFIG_FILE")
    local proxmox_host=$(yq -r ".node_details.$node.proxmox_host // \"\"" "$CONFIG_FILE")
    
    if [[ -z "$vm_id" || -z "$proxmox_host" ]]; then
      log_error "Could not find VM ID or Proxmox host for node $node in config"
      shutdown_success=false
      continue
    fi
    
    # Check if node is part of control plane
    local is_control_plane=$(ssh_cmd_quiet "$node" "systemctl is-active k3s.service >/dev/null 2>&1 && echo 'true' || echo 'false'" "$PROXMOX_USER")
    
    if [[ "$is_control_plane" == "true" ]]; then
      log_info "Node $node is part of the control plane"
      
      # Get a working kubectl node to check control plane nodes
      local kubectl_node="$node"
      
      # Get all control plane nodes directly from the cluster
      local control_plane_nodes=$(ssh_cmd_quiet "$kubectl_node" "kubectl get nodes -l node-role.kubernetes.io/control-plane -o jsonpath='{.items[*].metadata.name}' | tr ' ' '\n' | grep -v $node" "$PROXMOX_USER")
      
      # If empty, also try the older master label
      if [[ -z "$control_plane_nodes" ]]; then
        control_plane_nodes=$(ssh_cmd_quiet "$kubectl_node" "kubectl get nodes -l node-role.kubernetes.io/master -o jsonpath='{.items[*].metadata.name}' | tr ' ' '\n' | grep -v $node" "$PROXMOX_USER")
      fi
      
      # Check if we found other control plane nodes
      if [[ -z "$control_plane_nodes" ]]; then
        log_error "Cannot shutdown $node: At least one other control plane node must be available"
        shutdown_success=false
        continue
      else
        log_info "Found other control plane nodes: $control_plane_nodes"
      fi
    fi
    
    # 1. Cordon the node
    cordon_node "$node" || {
      if [[ "$FORCE" != "true" ]]; then
        log_error "Failed to cordon node $node. Aborting shutdown."
        shutdown_success=false
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
        shutdown_success=false
        continue
      else
        log_warn "Continuing despite drain failure due to --force flag"
      fi
    }
    
    # 3. Stop k3s service on the node
    log_info "Stopping k3s service on $node..."
    if [[ "$DRY_RUN" != "true" ]]; then
      local k3s_stop_result=$(ssh_cmd_capture "$node" "systemctl stop k3s.service k3s-agent.service 2>/dev/null" "$PROXMOX_USER")
      local exit_code=$?
      
      if [[ $exit_code -ne 0 ]]; then
        log_warn "Failed to gracefully stop k3s on $node: $k3s_stop_result"
        
        if [[ "$FORCE" == "true" ]]; then
          log_warn "Attempting to force stop k3s processes on $node..."
          ssh_cmd_quiet "$node" "pkill -9 k3s 2>/dev/null" "$PROXMOX_USER"
          sleep 5
        fi
      fi
      
      # Verify k3s is stopped
      local k3s_status=$(ssh_cmd_quiet "$node" "systemctl is-active k3s.service k3s-agent.service 2>/dev/null || echo 'inactive'" "$PROXMOX_USER")
      
      if [[ "$k3s_status" == "active" ]]; then
        log_warn "k3s service is still running on $node despite stop attempt. VM shutdown might fail."
        
        if [[ "$FORCE" != "true" && "$INTERACTIVE" != "true" ]]; then
          log_error "Aborting. Use --force to continue anyway."
          # Uncordon the node since we're not continuing
          uncordon_node "$node"
          shutdown_success=false
          continue
        elif [[ "$INTERACTIVE" == "true" ]]; then
          if ! confirm "Continue with VM shutdown despite k3s service stop failure?"; then
            log_error "Shutdown operation cancelled by user."
            # Uncordon the node since we're not continuing
            uncordon_node "$node" 
            shutdown_success=false
            continue
          fi
        fi
      else
        log_success "k3s service stopped on $node"
      fi
    else
      log_info "[DRY RUN] Would stop k3s service on $node"
    fi
    
    # 4. Shutdown the VM via Proxmox
    log_info "Shutting down VM $vm_id on $proxmox_host..."

    if [[ "$DRY_RUN" != "true" ]]; then
      # More verbose logging
      log_info "Running command on $proxmox_host: qm shutdown $vm_id"
      
      # Use the unified ssh_cmd with capture mode for better error detection
      local shutdown_result=$(ssh_cmd "$proxmox_host" "qm shutdown $vm_id" "$PROXMOX_USER" "capture")
      local exit_code=$?
      
      # Log full result for debugging
      log_debug "Shutdown command result: exit_code=$exit_code, output='$shutdown_result'"
      
      if [[ $exit_code -ne 0 ]]; then
        log_warn "Failed to initiate shutdown of VM $vm_id: $shutdown_result"
        
        if [[ "$FORCE" == "true" ]]; then
          log_warn "Force flag set, attempting to stop VM..."
          ssh_cmd "$proxmox_host" "qm stop $vm_id" "$PROXMOX_USER" "quiet"
        elif [[ "$INTERACTIVE" == "true" ]]; then
          if confirm "Graceful shutdown failed. Force stop VM $vm_id?"; then
            log_info "Forcing VM $vm_id to stop..."
            ssh_cmd "$proxmox_host" "qm stop $vm_id" "$PROXMOX_USER" "quiet"
          else
            log_error "Cannot continue without stopping the VM. Aborting."
            # Try to uncordon the node
            uncordon_node "$node"
            shutdown_success=false
            continue
          fi
        else
          log_error "Failed to shutdown VM $vm_id and --force not specified"
          # Try to uncordon the node
          uncordon_node "$node"
          shutdown_success=false
          continue
        fi
      else
        log_success "VM $vm_id shutdown initiated"
      fi
      
      # Wait for VM to shutdown if requested
      if [[ "$wait_for_shutdown" == "true" ]]; then
        log_info "Waiting for VM $vm_id to shutdown..."
        local timeout=90
        local count=0
        
        while [[ $count -lt $timeout ]]; do
          # Query VM status with the unified ssh_cmd
          local vm_status=$(ssh_cmd "$proxmox_host" "qm status $vm_id | grep -o 'status: [a-z]*' | cut -d' ' -f2" "$PROXMOX_USER" "quiet")
          
          log_info "Current VM status: $vm_status"
          
          if [[ "$vm_status" == "stopped" ]]; then
            log_success "VM $vm_id is now stopped"
            break
          fi
          
          sleep 5
          ((count+=5))
          log_info "Still waiting for VM $vm_id to stop... (${count}s/${timeout}s)"
        done
        
        # Final check
        local final_status=$(ssh_cmd_quiet "$proxmox_host" "qm status $vm_id | grep -o \"status: [a-z]*\" | cut -d' ' -f2" "$PROXMOX_USER")
        log_debug "Final VM status check via ssh_cmd_quiet: '$final_status'"

        # Just for debugging - what happens if we try direct qm on local system
        local local_qm_exists=$(command -v qm &>/dev/null && echo "exists" || echo "missing")
        log_debug "Local qm command $local_qm_exists on $(hostname)"
        if [[ "$local_qm_exists" == "exists" ]]; then
          local direct_check=$(qm status $vm_id 2>/dev/null | grep -o "status: [a-z]*" | cut -d' ' -f2 || echo "failed")
          log_debug "Direct local qm status check result: '$direct_check'"
        fi

        if [[ "$final_status" != "stopped" ]]; then
          log_error "Timed out waiting for VM $vm_id to stop (final status: $final_status)"
          
          if [[ "$FORCE" == "true" || "$INTERACTIVE" == "true" ]]; then
            if [[ "$INTERACTIVE" != "true" ]] || confirm "Force stop VM $vm_id using 'qm stop'?"; then
              log_info "Forcing VM $vm_id to stop..."
              ssh_cmd_quiet "$proxmox_host" "qm stop $vm_id" "$PROXMOX_USER"
              
              # Wait a bit more for the forced stop
              local force_timeout=60
              local force_count=0
              
              while [[ $force_count -lt $force_timeout ]]; do
                local vm_status=$(ssh_cmd_quiet "$proxmox_host" "qm status $vm_id" "$PROXMOX_USER" | grep -o "status: [a-z]*" | cut -d' ' -f2)
                
                if [[ "$vm_status" == "stopped" ]]; then
                  log_success "VM $vm_id is now stopped (after forced stop)"
                  break
                fi
                
                sleep 5
                ((force_count+=5))
                log_info "Still waiting for VM $vm_id to stop after force... (${force_count}s/${force_timeout}s)"
              done
              
              if [[ $force_count -ge $force_timeout ]]; then
                log_error "Timed out waiting for VM $vm_id to stop even after forced shutdown"
                shutdown_success=false
                continue
              fi
            else
              log_error "User declined to force stop VM. Aborting."
              shutdown_success=false
              continue
            fi
          else
            log_error "Timed out waiting for VM $vm_id to stop"
            shutdown_success=false
            continue
          fi
        fi
      fi
    else
      log_info "[DRY RUN] Would shutdown VM $vm_id on $proxmox_host"
    fi
  done
  
  if [[ "$shutdown_success" == "true" ]]; then
    log_success "Node shutdown operations completed successfully"
    return 0
  else
    log_warn "Some nodes failed to shutdown properly"
    return 1
  fi
}

# Modify the start_node function to add proper waiting for k3s initialization
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
    local timeout=120
    local count=0
    local connection_success=false

    while [[ $count -lt $timeout ]]; do
      # Try multiple connection attempts before considering it a failure
      if ssh -o BatchMode=yes -o ConnectTimeout=3 -o StrictHostKeyChecking=no -p "$SSH_PORT" root@$node "echo 'Connected'" &>/dev/null; then
        log_success "Node $node is now reachable"
        connection_success=true
        break
      fi
      
      # Try with IP address if hostname resolution might be an issue
      local node_ip=$(yq -r ".node_details.$node.ip // \"\"" "$CONFIG_FILE")
      if [[ -n "$node_ip" ]] && ssh -o BatchMode=yes -o ConnectTimeout=3 -o StrictHostKeyChecking=no -p "$SSH_PORT" root@$node_ip "echo 'Connected'" &>/dev/null; then
        log_success "Node $node (IP: $node_ip) is now reachable"
        connection_success=true
        break
      fi
      
      sleep 5
      ((count+=5))
      log_info "Still waiting for $node... (${count}s/${timeout}s)"
      
      # Check VM status every 15 seconds to confirm it's running
      if (( count % 15 == 0 )); then
        local vm_status=$(ssh_cmd_quiet "$proxmox_host" "qm status $vm_id" "$PROXMOX_USER" | grep -o "status: [a-z]*" | cut -d' ' -f2)
        log_info "Current VM status: $vm_status"
      fi
    done
    
    if [[ "$connection_success" != "true" ]]; then
      log_error "Timed out waiting for $node to be reachable"
      return 1
    fi
    
    # Now wait for k3s to be fully initialized
    log_info "Waiting for k3s to initialize on $node..."
    wait_for_k3s_ready "$node" 180  # Allow 3 minutes for k3s to start up
  else
    log_info "[DRY RUN] Would start VM $vm_id on $proxmox_host"
    return 0
  fi
  
  return 0
}

# Cleanup node after a failed operation
function cleanup_node() {
  local node="$1"
  local operation="$2"  # snapshot, backup, replace, etc.
  local etcd_snapshot="$3"  # Optional etcd snapshot to clean up
  
  log_section "Cleaning up node $node after failed $operation"
  
  # 1. Check if node is cordoned
  local kubectl_node=""
  local all_nodes=$(yq -r '.nodes[]' "$CONFIG_FILE" 2>/dev/null)
  
  # Find another node that's accessible to run kubectl
  for potential_node in $all_nodes; do
    if [[ "$potential_node" != "$node" ]]; then
      if ssh_cmd_quiet "$potential_node" "echo Connected" "$PROXMOX_USER" &>/dev/null; then
        kubectl_node="$potential_node"
        break
      fi
    fi
  done
  
  if [[ -n "$kubectl_node" ]]; then
    # Check if node is cordoned
    local node_status=$(ssh_cmd_quiet "$kubectl_node" "kubectl get node $node -o jsonpath='{.spec.unschedulable}'" "$PROXMOX_USER")
    
    if [[ "$node_status" == "true" ]]; then
      log_info "Node $node is cordoned, uncordoning..."
      uncordon_node "$node"
    fi
  fi
  
  # 2. Check k3s service status and start if needed
  log_info "Checking k3s service status on $node..."
  local k3s_status=$(ssh_cmd_quiet "$node" "systemctl is-active k3s.service 2>/dev/null || echo 'inactive'" "$PROXMOX_USER")
  
  log_info "k3s service status: $k3s_status"
  
  # Always attempt to start the service unless it's already active
  if [[ "$k3s_status" != "active" ]]; then
    log_info "Starting k3s service on $node..."
    ssh_cmd_quiet "$node" "systemctl start k3s.service" "$PROXMOX_USER"
    
    # Wait for service to start
    local timeout=90
    local count=0
    while [[ $count -lt $timeout ]]; do
      k3s_status=$(ssh_cmd_quiet "$node" "systemctl is-active k3s.service 2>/dev/null || echo 'inactive'" "$PROXMOX_USER")
      
      if [[ "$k3s_status" == "active" ]]; then
        log_success "k3s service started on $node"
        break
      fi
      
      sleep 5
      ((count+=5))
      log_info "Waiting for k3s service to start... (${count}s/${timeout}s)"
    done
    
    if [[ $count -ge $timeout ]]; then
      log_error "Failed to start k3s service on $node"
    fi
  else
    log_info "k3s service is already active on $node"
  fi
  
  # Additional verification - check if node shows as Ready
  if [[ -n "$kubectl_node" ]]; then
    log_info "Waiting for node $node to be Ready..."
    local timeout=120
    local count=0
    
    while [[ $count -lt $timeout ]]; do
      local ready_status=$(ssh_cmd_quiet "$kubectl_node" "kubectl get node $node -o jsonpath='{.status.conditions[?(@.type==\"Ready\")].status}'" "$PROXMOX_USER")
      
      if [[ "$ready_status" == "True" ]]; then
        log_success "Node $node is now Ready"
        break
      fi
      
      sleep 10
      ((count+=10))
      log_info "Waiting for node $node to be Ready... (${count}s/${timeout}s)"
    done
    
    if [[ $count -ge $timeout ]]; then
      log_warn "Timed out waiting for node $node to be Ready"
    fi
  fi
  
  # 3. Clean up the etcd snapshot if provided
  if [[ -n "$etcd_snapshot" ]]; then
    log_info "Cleaning up etcd snapshot: $etcd_snapshot"
    
    # Find a suitable node to run the cleanup
    local etcd_node="$node"
    local is_server=$(ssh_cmd_quiet "$etcd_node" "systemctl is-active k3s.service >/dev/null 2>&1 && echo 'true' || echo 'false'" "$PROXMOX_USER")
    
    if [[ "$is_server" != "true" ]]; then
      # Try to find another server node
      for potential_node in $all_nodes; do
        if [[ "$potential_node" != "$node" ]]; then
          is_server=$(ssh_cmd_quiet "$potential_node" "systemctl is-active k3s.service >/dev/null 2>&1 && echo 'true' || echo 'false'" "$PROXMOX_USER")
          if [[ "$is_server" == "true" ]]; then
            etcd_node="$potential_node"
            break
          fi
        fi
      done
    fi
    
    # List all etcd snapshots and identify any that match the pattern
    log_info "Listing etcd snapshots matching pattern: $etcd_snapshot"
    local snapshot_files=$(ssh_cmd_quiet "$etcd_node" "ls -1 /var/lib/rancher/k3s/server/db/snapshots/ | grep '$etcd_snapshot'" "$PROXMOX_USER")
    
    if [[ -n "$snapshot_files" ]]; then
      log_info "Found matching etcd snapshot files:"
      echo "$snapshot_files"
      
      # Delete each matching file
      while read -r snapshot_file; do
        log_info "Deleting etcd snapshot file: $snapshot_file"
        ssh_cmd_quiet "$etcd_node" "rm -f /var/lib/rancher/k3s/server/db/snapshots/$snapshot_file" "$PROXMOX_USER"
      done <<< "$snapshot_files"
      
      log_success "Etcd snapshots cleaned up"
    else
      log_warn "No matching etcd snapshot files found for pattern: $etcd_snapshot"
    fi
  fi
  
  log_success "Cleanup completed"
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
    local timeout=120
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
