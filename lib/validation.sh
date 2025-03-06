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

# Check overall cluster health
function check_cluster_health() {
  log_info "Checking cluster health..."
  
  # Use the first node to get cluster status
  local first_node="${NODES[0]}"
  
  # Get components status
  local components_status=$(ssh_cmd_quiet "$first_node" "kubectl get componentstatuses" "$PROXMOX_USER")
  
  if [[ $? -ne 0 ]]; then
    log_warn "Failed to get component status (this might be expected with newer Kubernetes versions)"
  else
    log_info "Component status:"
    echo "$components_status"
    
    # Check for unhealthy components
    if echo "$components_status" | grep -v "Healthy"; then
      log_warn "Some components might not be healthy"
    fi
  fi
  
  # Check for pending pods
  local pending_pods=$(ssh_cmd_quiet "$first_node" "kubectl get pods --all-namespaces | grep -v Running | grep -v Completed" "$PROXMOX_USER")
  
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
  
  # Check CNI status (Flannel WireGuard)
  local cni_check=$(ssh_cmd_quiet "$first_node" "kubectl -n kube-system get pods | grep flannel" "$PROXMOX_USER")
  
  if [[ -z "$cni_check" ]]; then
    log_warn "Could not find flannel pods"
  else
    log_info "Flannel pods:"
    echo "$cni_check"
    
    # Check if all flannel pods are running
    if echo "$cni_check" | grep -v "Running"; then
      log_warn "Some flannel pods are not running"
    else
      log_success "All flannel pods are running"
    fi
  fi
  
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
  
  # Check SSH connectivity to all nodes
  log_info "Checking SSH connectivity to all nodes..."
  
  for node in "${NODES[@]}"; do
    log_info "Testing SSH connection to $node..."
    
    ssh -o BatchMode=yes -o ConnectTimeout=5 root@$node "echo 'Connected'" &>/dev/null
    
    if [[ $? -ne 0 ]]; then
      log_error "Cannot SSH to $node"
      return 1
    fi
  done
  
  log_success "SSH connectivity to all nodes verified"
  
  # Check kubectl availability on the first node
  local first_node="${NODES[0]}"
  log_info "Checking kubectl availability on $first_node..."
  
  local kubectl_check=$(ssh_cmd_quiet "$first_node" "command -v kubectl >/dev/null && echo 'OK' || echo 'FAIL'" "$PROXMOX_USER")
  
  if [[ "$kubectl_check" != "OK" ]]; then
    log_error "kubectl not available on $first_node"
    return 1
  fi
  
  log_success "kubectl available on $first_node"
  
  return 0
}
