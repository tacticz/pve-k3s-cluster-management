#!/bin/bash
# validation.sh - Cluster validation functions
#
# This module is part of the k3s-cluster-management
# It provides functions for validating the health and accessibility
# of the k3s cluster and related components.

# Validate cluster health
function validate_cluster() {
  log_section "Validating Cluster Health"
  
  # Check validation level
  case "$VALIDATE_LEVEL" in
    basic)
      log_info "Performing basic validation"
      check_k3s_version
      check_nodes_status
      check_cluster_health
      ;;
    extended)
      log_info "Performing extended validation"
      check_k3s_version
      check_nodes_status
      check_cluster_health
      check_etcd_health
      check_storage_health
      ;;
    full)
      log_info "Performing full validation"
      check_k3s_version
      check_nodes_status
      check_cluster_health
      check_etcd_health
      check_storage_health
      check_network_health
      check_workload_health
      check_proxmox_connectivity
      ;;
    *)
      log_error "Unknown validation level: $VALIDATE_LEVEL"
      return 1
      ;;
  esac
  
  log_success "Cluster validation completed successfully"
  return 0
}

# Check K3s version on all nodes
function check_k3s_version() {
  log_info "Checking K3s version on all nodes..."
  
  local first_node_version=""
  local all_same=true
  
  for node in "${NODES[@]}"; do
    log_info "Checking version on $node..."
    
    # Get K3s version
    local version=$(ssh_cmd "$node" "k3s --version 2>/dev/null || echo 'Not installed'" "$PROXMOX_USER")
    
    # Clean debug output from the version string if DEBUG is true
    if [[ "$DEBUG" == "true" ]]; then
      version=$(echo "$version" | grep -v '\[DEBUG\]')
    fi

    if [[ "$version" == "Not installed" ]]; then
      log_error "K3s not installed on $node"
      return 1
    fi
    
    log_info "$node: $version"
    
    # Check if all nodes have the same version
    if [[ -z "$first_node_version" ]]; then
      first_node_version="$version"
    elif [[ "$version" != "$first_node_version" ]]; then
      all_same=false
    fi
  done
  
  if [[ "$all_same" != "true" ]]; then
    log_warn "Not all nodes are running the same K3s version"
  else
    log_success "All nodes are running $first_node_version"
  fi
  
  return 0
}

# Check status of all nodes
function check_nodes_status() {
  log_info "Checking node status..."
  
  # Use the first node to get cluster status
  local first_node="${NODES[0]}"
  
  # Get node status
  local nodes_status=$(ssh_cmd_quiet "$first_node" "kubectl get nodes -o wide" "$PROXMOX_USER")
  
  if [[ $? -ne 0 ]]; then
    log_error "Failed to get node status from $first_node"
    return 1
  fi
  
  log_info "Nodes status:"
  echo "$nodes_status"
  
  # Check for NotReady nodes
  if echo "$nodes_status" | grep -q "NotReady"; then
    log_warn "Some nodes are not ready"
    return 1
  fi
  
  log_success "All nodes are Ready"
  return 0
}

# Check if a node is in Ready state
function check_node_ready() {
  local node="$1"
  local timeout="${2:-300}"  # Default 5 minute timeout
  local remote_node="$3"  # Optional: another node to run kubectl from
  
  log_wait_sequence "node $node to be in Ready state" "$timeout"
  
  # First make sure k3s service is active
  if ! systemctl_is_active "$node" "k3s.service" 120; then
    log_warn "K3s service not active on $node"
    return 1
  fi
  
  # Now check node readiness state from another node if possible
  if [[ -z "$remote_node" ]]; then
    # Try to find another node to run kubectl from
    for check_node in "${NODES[@]}"; do
      if [[ "$check_node" != "$node" ]] && ssh_cmd_silent "$check_node" "kubectl get nodes" "$PROXMOX_USER"; then
        remote_node="$check_node"
        log_info "Using $remote_node to check $node status"
        break
      fi
    done
  fi
  
  # If no remote node found, try to use the node itself
  if [[ -z "$remote_node" ]]; then
    log_info "Using $node itself to check readiness"
    remote_node="$node"
  fi
  
  local count=0
  while [[ $count -lt $timeout ]]; do
    local ready_status=$(ssh_cmd_quiet "$remote_node" "kubectl get node $node -o jsonpath='{.status.conditions[?(@.type==\"Ready\")].status}'" "$PROXMOX_USER")
    
    if [[ "$ready_status" == "True" ]]; then
      log_success "Node $node is in Ready state"
      return 0
    fi
    
    sleep 10
    ((count+=10))
    
    if (( count % 30 == 0 )); then
      local node_status=$(ssh_cmd_quiet "$remote_node" "kubectl get node $node" "$PROXMOX_USER")
      log_info "Current node status after ${count}s: $node_status"
    fi
    
    log_info "Waiting for node $node to be Ready... (${count}s/${timeout}s)"
  done
  
  log_warn "Timed out waiting for node $node to be Ready"
  return 1
}

# Helper function to check if a systemd service is active
function systemctl_is_active() {
  local node="$1"
  local service="$2"
  local timeout="${3:-60}"
  
  log_info "Checking if $service is active on $node..."
  local count=0
  
  while [[ $count -lt $timeout ]]; do
    local status=$(ssh_cmd_quiet "$node" "systemctl is-active $service 2>/dev/null || echo 'inactive'" "$PROXMOX_USER")
    status=$(echo "$status" | tr -d '[:space:]')
    
    if [[ "$status" == "active" ]]; then
      log_success "$service is active on $node"
      return 0
    fi
    
    sleep 5
    ((count+=5))
    
    log_info "Waiting for $service to be active... (${count}s/${timeout}s)"
  done
  
  log_warn "$service is not active on $node after ${timeout}s"
  return 1
}

# Check overall cluster health
function check_cluster_health() {
  log_info "Checking cluster health..."
  
  # Use the first node to get cluster status
  local first_node="${NODES[0]}"
  
  # Get components status
  local components_status=$(ssh_cmd_quiet "$first_node" "kubectl get componentstatuses" "$PROXMOX_USER")
  
  # Clean debug output
  if [[ "$DEBUG" == "true" ]]; then
    components_status=$(echo "$components_status" | grep -v '\[DEBUG\]')
  fi

  if [[ $? -ne 0 ]]; then
    log_warn "Failed to get component status (this might be expected with newer Kubernetes versions)"
  else
    log_info "Component status:"
    echo "$components_status"
    
    # Check for unhealthy components - only check the STATUS column for non-Healthy values
    if echo "$components_status" | grep -v "NAME\|STATUS" | grep -v "Healthy"; then
      log_warn "Some components might not be healthy"
    else
      log_success "All components are healthy"
    fi
  fi
  
  # Check for pending pods - better filter for actual problem pods
  local pending_pods=$(ssh_cmd_quiet "$first_node" "kubectl get pods --all-namespaces | grep -v Running | grep -v Completed | grep -v NAME" "$PROXMOX_USER")
  
  # Clean debug output
  if [[ "$DEBUG" == "true" ]]; then
    pending_pods=$(echo "$pending_pods" | grep -v '\[DEBUG\]')
  fi

  if [[ -n "$pending_pods" ]]; then
    log_warn "Some pods are not in Running/Completed state:"
    echo "$pending_pods"
  else
    log_success "All pods are in Running/Completed state"
  fi
  
  return 0
}

# Check etcd health
function check_etcd_health() {
  if [[ "$VALIDATE_ETCD" != "true" ]]; then
    log_info "Skipping etcd health check"
    return 0
  fi
  
  log_info "Checking etcd health..."
  
  # Use the first node to check etcd
  local first_node="${NODES[0]}"
  
  # Get etcd status
  local etcd_health=$(ssh_cmd "$first_node" "k3s etcd-snapshot ls 2>/dev/null || echo 'Not available'" "$PROXMOX_USER")
  
  if [[ "$etcd_health" == "Not available" ]]; then
    log_warn "Could not check etcd snapshots, might not be using etcd"
    return 0
  fi
  
  log_info "Etcd snapshots:"
  echo "$etcd_health"
  
  # Check etcd endpoints
  local etcd_endpoints=$(ssh_cmd_quiet "$first_node" "kubectl get endpoints -n kube-system etcd-server-events -o yaml" "$PROXMOX_USER")
  
  if [[ $? -ne 0 ]]; then
    log_warn "Could not get etcd endpoints, checking k3s server service"
    
    # Check if k3s server is running
    local k3s_status=$(ssh_cmd_quiet "$first_node" "systemctl status k3s.service | grep Active:" "$PROXMOX_USER")
    
    if [[ $? -ne 0 ]] || ! echo "$k3s_status" | grep -q "active (running)"; then
      log_error "k3s server service is not running properly on $first_node"
      return 1
    else
      log_success "k3s server service is running on $first_node"
    fi
  else
    log_success "etcd endpoints found"
  fi
  
  return 0
}

# Check storage health
function check_storage_health() {
  if [[ "$VALIDATE_STORAGE" != "true" ]]; then
    log_info "Skipping storage health check"
    return 0
  fi
  
  log_info "Checking storage health..."
  
  # Use the first node to check storage
  local first_node="${NODES[0]}"
  
  # Check if CephFS is mounted
  local mount_check=$(ssh_cmd_quiet "$first_node" "mount | grep pvecephfs-1-k3s" "$PROXMOX_USER")
  
  if [[ -z "$mount_check" ]]; then
    log_error "CephFS not mounted on $first_node"
    return 1
  fi
  
  log_info "CephFS mount found on $first_node:"
  echo "$mount_check"
  
  # Check if the mount is writable
  local write_test=$(ssh_cmd_quiet "$first_node" "touch /mnt/pvecephfs-1-k3s/k3s-admin-write-test && echo 'OK' || echo 'FAIL'" "$PROXMOX_USER")
  
  if [[ "$write_test" != "OK" ]]; then
    log_error "CephFS mount is not writable on $first_node"
    return 1
  fi
  
  # Clean up test file
  ssh_cmd_silent "$first_node" "rm -f /mnt/pvecephfs-1-k3s/k3s-admin-write-test" "root"
    
  log_success "CephFS is mounted and writable on $first_node"
  
  # Check StorageClass (for PVCs)
  local storage_classes=$(ssh_cmd_quiet "$first_node" "kubectl get storageclasses" "$PROXMOX_USER")
  
  if [[ $? -ne 0 ]]; then
    log_warn "Could not get StorageClasses"
  else
    log_info "StorageClasses:"
    echo "$storage_classes"
  fi
  
  return 0
}

# Check network health
function check_network_health() {
  if [[ "$VALIDATE_NETWORK" != "true" ]]; then
    log_info "Skipping network health check"
    return 0
  fi
  
  log_info "Checking network health..."
  
  # Check connectivity between nodes
  for node in "${NODES[@]}"; do
    log_info "Checking connectivity from $node to other nodes..."
    
    for target in "${NODES[@]}"; do
      if [[ "$node" != "$target" ]]; then
        local ping_test=$(ssh_cmd_quiet "$node" "ping -c 1 -W 2 $target >/dev/null && echo 'OK' || echo 'FAIL'" "$PROXMOX_USER")
        
        if [[ "$ping_test" != "OK" ]]; then
          log_error "$node cannot ping $target"
          return 1
        fi
      fi
    done
    
    log_success "$node can ping all other nodes"
  done
  
  # Check pod network connectivity
  local first_node="${NODES[0]}"
  log_info "Checking pod network connectivity..."
  
  # Check k3s network configuration - this is more appropriate for k3s where flannel runs embedded
  local network_check=$(ssh_cmd_quiet "$first_node" "kubectl get nodes -o jsonpath='{.items[0].spec.podCIDR}'" "$PROXMOX_USER")
  
  if [[ -n "$network_check" ]]; then
    log_info "Pod CIDR configured: $network_check"
    log_success "Network configuration found"
  else
    # Try alternative method to check for network configuration
    local cni_config=$(ssh_cmd_quiet "$first_node" "ls -l /var/lib/rancher/k3s/agent/etc/cni/net.d/" "$PROXMOX_USER")
    
    if [[ -n "$cni_config" ]]; then
      log_info "CNI configuration found:"
      echo "$cni_config"
      log_success "Network configuration detected"
    else
      log_warn "Could not verify k3s network configuration"
    fi
  fi
  
  # Additional network validation: Check if cross-node pod communication is working
  log_info "Testing cross-node pod communication..."
  
  # Create a test pod on the first node if it doesn't exist
  local test_pod=$(ssh_cmd_quiet "$first_node" "kubectl get pod network-test 2>/dev/null" "$PROXMOX_USER")
  
  if [[ -z "$test_pod" || ! "$test_pod" =~ Running ]]; then
    local create_pod=$(ssh_cmd_quiet "$first_node" "kubectl run network-test --image=busybox --restart=Never --command -- sleep 300" "$PROXMOX_USER")
    log_info "Created test pod for network validation"
    
    # Wait for pod to be ready
    local timeout=60
    local count=0
    while [[ $count -lt $timeout ]]; do
      test_pod=$(ssh_cmd_quiet "$first_node" "kubectl get pod network-test -o jsonpath='{.status.phase}'" "$PROXMOX_USER")
      
      if [[ "$test_pod" == "Running" ]]; then
        log_success "Test pod is running"
        break
      fi
      
      sleep 2
      ((count+=2))
      log_info "Waiting for test pod... (${count}s/${timeout}s)"
    done
    
    if [[ $count -ge $timeout ]]; then
      log_warn "Timed out waiting for test pod to run"
      ssh_cmd_quiet "$first_node" "kubectl delete pod network-test --force" "$PROXMOX_USER"
      return 0
    fi
  fi
  
  # Check pod DNS resolution
  local dns_check=$(ssh_cmd_quiet "$first_node" "kubectl exec network-test -- nslookup kubernetes.default.svc.cluster.local" "$PROXMOX_USER")
  
  if [[ $? -eq 0 ]]; then
    log_success "Pod DNS resolution is working"
  else
    log_warn "Pod DNS resolution test failed"
  fi
  
  # Clean up test pod
  ssh_cmd_quiet "$first_node" "kubectl delete pod network-test --force" "$PROXMOX_USER"
  
  return 0
}

# Check workload health
function check_workload_health() {
  log_info "Checking workload health..."
  
  # Use the first node
  local first_node="${NODES[0]}"
  
  # Get namespaces
  local namespaces=$(ssh_cmd_quiet "$first_node" "kubectl get namespaces -o name | cut -d/ -f2" "$PROXMOX_USER")
  
  if [[ $? -ne 0 ]]; then
    log_error "Failed to get namespaces"
    return 1
  fi
  
  # Check deployments in each namespace
  for ns in $namespaces; do
    if [[ "$ns" != "kube-system" && "$ns" != "kube-public" && "$ns" != "kube-node-lease" ]]; then
      log_info "Checking deployments in namespace: $ns"
      
      local deployments=$(ssh_cmd_quiet "$first_node" "kubectl -n $ns get deployments" "$PROXMOX_USER")
      
      if [[ -n "$deployments" ]]; then
        echo "$deployments"
        
        # Check for deployments with unavailable replicas
        if echo "$deployments" | grep -v "AVAILABLE" | awk '{if ($3 != $4) print $0}' | grep -v "^$"; then
          log_warn "Some deployments in $ns have unavailable replicas"
        fi
      else
        log_info "No deployments found in namespace $ns"
      fi
    fi
  done
  
  return 0
}

# Check Proxmox connectivity
function check_proxmox_connectivity() {
  log_info "Checking Proxmox connectivity..."
  
  for host in "${PROXMOX_HOSTS[@]}"; do
    log_info "Checking connectivity to Proxmox host: $host"
    
    # Check if we can SSH to the Proxmox host
    local ssh_test=$(ssh_cmd_quiet "$host" "echo 'OK'" "$PROXMOX_USER" || echo "FAIL")
    
    if [[ "$ssh_test" != "OK" ]]; then
      log_error "Cannot SSH to Proxmox host $host"
      return 1
    fi
    
    # Check if pvesh command is available (Proxmox CLI)
    local pvesh_test=$(ssh_cmd_quiet "$host" "command -v pvesh >/dev/null && echo 'OK' || echo 'FAIL'" "$PROXMOX_USER")
    
    if [[ "$pvesh_test" != "OK" ]]; then
      log_error "pvesh command not available on $host"
      return 1
    fi
    
    log_success "Proxmox connectivity to $host verified"
  done
  
  return 0
}

# Pre-flight checks before running operations
function run_preflight_checks() {
  log_section "Running Pre-flight Checks"

  # Verify SSH connectivity to all hosts and handle host key verification
  log_info "Verifying SSH connectivity to hosts and Proxmox servers..."
  verify_ssh_hosts || return 1
  
  # Since verify_ssh_hosts already checked SSH connectivity to all nodes,
  # we can skip the redundant connectivity check and proceed to checking kubectl
  
  # Check kubectl availability on the first node
  local first_node="${NODES[0]}"
  log_info "Checking kubectl availability on $first_node..."

  # Test kubectl command with more comprehensive approach
  local kubectl_check=$(ssh_cmd "$first_node" "which kubectl || find /usr/local/bin -name kubectl | grep kubectl || echo 'NOT_FOUND'" "$PROXMOX_USER" "capture")

  if [[ "$kubectl_check" == "NOT_FOUND" ]]; then
    # Alternative check - k3s comes with kubectl built-in via the k3s binary
    local k3s_check=$(ssh_cmd "$first_node" "k3s kubectl get nodes" "$PROXMOX_USER" "quiet")
    
    if [[ $? -eq 0 ]]; then
      log_success "kubectl available via k3s command on $first_node"
    else
      log_error "kubectl not available on $first_node (neither native kubectl nor via k3s command)"
      return 1
    fi
  else
    log_success "kubectl found at: $kubectl_check"
  fi
  
  return 0
}
