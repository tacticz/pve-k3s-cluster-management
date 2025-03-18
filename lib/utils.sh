#!/bin/bash
# utils.sh - Utility functions
#
# This module is part of the k3s-cluster-management
# It provides common utility functions used throughout the script.

# Log levels
LOG_ERROR=1
LOG_WARN=2
LOG_INFO=3
LOG_SUCCESS=4
LOG_DEBUG=5

# Terminal colors
RED='\033[0;31m'
YELLOW='\033[0;33m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# Logging function
function log() {
  local level=$1
  local message=$2
  local timestamp=$(date "+%Y-%m-%d %H:%M:%S")
  
  # Only log if not quiet mode or if it's an error/warning
  if [[ "$QUIET" != "true" || $level -le $LOG_WARN ]]; then
    case $level in
      $LOG_ERROR)
        echo -e "${timestamp} ${RED}[ERROR]${NC} $message" >&2
        ;;
      $LOG_WARN)
        echo -e "${timestamp} ${YELLOW}[WARN]${NC} $message" >&2
        ;;
      $LOG_INFO)
        echo -e "${timestamp} ${BLUE}[INFO]${NC} $message"
        ;;
      $LOG_SUCCESS)
        echo -e "${timestamp} ${GREEN}[SUCCESS]${NC} $message"
        ;;
      $LOG_DEBUG)
        if [[ "$DEBUG" == "true" ]]; then
          echo -e "${timestamp} ${CYAN}[DEBUG]${NC} $message"
        fi
        ;;
    esac
  fi
  
  # Also log to file if log file is defined
  if [[ -n "$LOG_FILE" ]]; then
    case $level in
      $LOG_ERROR)
        echo "${timestamp} [ERROR] $message" >> "$LOG_FILE"
        ;;
      $LOG_WARN)
        echo "${timestamp} [WARN] $message" >> "$LOG_FILE"
        ;;
      $LOG_INFO)
        echo "${timestamp} [INFO] $message" >> "$LOG_FILE"
        ;;
      $LOG_SUCCESS)
        echo "${timestamp} [SUCCESS] $message" >> "$LOG_FILE"
        ;;
      $LOG_DEBUG)
        if [[ "$DEBUG" == "true" ]]; then
          echo "${timestamp} [DEBUG] $message" >> "$LOG_FILE"
        fi
        ;;
    esac
  fi
}

# Log error message
function log_error() {
  log $LOG_ERROR "$1"
}

# Log warning message
function log_warn() {
  log $LOG_WARN "$1"
}

# Log info message
function log_info() {
  log $LOG_INFO "$1"
}

# Log success message
function log_success() {
  log $LOG_SUCCESS "$1"
}

# Log debug message
function log_debug() {
  log $LOG_DEBUG "$1"
}

# Print section header
function log_section() {
  local section_name=$1
  local divider=$(printf '%*s' 50 | tr ' ' '=')
  echo -e "\n${divider}"
  echo -e "${BLUE}${section_name}${NC}"
  echo -e "${divider}\n"
  
  if [[ -n "$LOG_FILE" ]]; then
    echo -e "\n${divider}" >> "$LOG_FILE"
    echo -e "${section_name}" >> "$LOG_FILE"
    echo -e "${divider}\n" >> "$LOG_FILE"
  fi
}

# Print subsection header (smaller than section header)
function log_subsection() {
  local subsection_name=$1
  local divider=$(printf '%*s' 40 | tr ' ' '-')
  echo -e "\n${divider}"
  echo -e "${CYAN}${subsection_name}${NC}"
  echo -e "${divider}"
  
  if [[ -n "$LOG_FILE" ]]; then
    echo -e "\n${divider}" >> "$LOG_FILE"
    echo -e "${subsection_name}" >> "$LOG_FILE"
    echo -e "${divider}" >> "$LOG_FILE"
  fi
}

# Print operation step (for major steps within a subsection)
function log_operation_step() {
  local operation=$1
  local target=$2
  
  echo -e "\n${YELLOW}▶ ${operation}${NC} ${target}"
  
  if [[ -n "$LOG_FILE" ]]; then
    echo -e "\n▶ ${operation} ${target}" >> "$LOG_FILE"
  fi
}

# Print wait sequence start
function log_wait_sequence() {
  local wait_for=$1
  local timeout=$2
  
  echo -e "\n${BLUE}⧖ Waiting for${NC} ${wait_for} ${BLUE}(timeout: ${timeout}s)${NC}"
  
  if [[ -n "$LOG_FILE" ]]; then
    echo -e "\n⧖ Waiting for ${wait_for} (timeout: ${timeout}s)" >> "$LOG_FILE"
  fi
}

# Confirm action with user
function confirm() {
  local message=$1
  local default=${2:-n}
  
  if [[ "$FORCE" == "true" ]]; then
    return 0
  fi
  
  local prompt
  if [[ $default == "y" ]]; then
    prompt="[Y/n]"
  else
    prompt="[y/N]"
  fi
  
  read -p "$message $prompt " response
  response=${response:-$default}
  
  if [[ $response =~ ^[Yy] ]]; then
    return 0
  else
    return 1
  fi
}

# Check if a command exists
function command_exists() {
  command -v "$1" &> /dev/null
}

# Ensure required commands are available
function check_required_commands() {
  local missing=false
  
  for cmd in ssh yq kubectl jq; do
    if ! command_exists "$cmd"; then
      log_error "Required command not found: $cmd"
      missing=true
    fi
  done
  
  if [[ "$missing" == "true" ]]; then
    log_error "Please install missing commands and try again"
    return 1
  fi
  
  return 0
}

# Improved host key management function
function add_host_key() {
  local host="$1"
  local port="${2:-$SSH_PORT}"
  
  log_info "Adding host key for $host to known_hosts file..."
  
  # Get IP address (if host is a hostname)
  local host_ip=""
  if [[ ! "$host" =~ ^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
    host_ip=$(getent hosts "$host" | awk '{print $1}' | head -1)
    log_debug "Resolved $host to IP: $host_ip"
  fi
  
  # Generate FQDN if it appears to be a short hostname
  local host_fqdn=""
  if [[ ! "$host" =~ \. && ! "$host" =~ ^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
    # Try to append domain from /etc/resolv.conf
    local domain=$(grep "^domain\|^search" /etc/resolv.conf | head -1 | awk '{print $2}')
    if [[ -n "$domain" ]]; then
      host_fqdn="${host}.${domain}"
      log_debug "Generated FQDN: $host_fqdn"
    fi
  fi
  
  # Create known_hosts file directory if it doesn't exist
  mkdir -p ~/.ssh
  touch ~/.ssh/known_hosts
  
  # Remove any existing entries for this host to avoid duplicates or old keys
  log_debug "Removing any existing host keys for $host"
  ssh-keygen -R "$host" >/dev/null 2>&1
  
  # Also remove entries for IP and FQDN if available
  if [[ -n "$host_ip" ]]; then
    log_debug "Removing any existing host keys for IP $host_ip"
    ssh-keygen -R "$host_ip" >/dev/null 2>&1
  fi
  
  if [[ -n "$host_fqdn" ]]; then
    log_debug "Removing any existing host keys for FQDN $host_fqdn"
    ssh-keygen -R "$host_fqdn" >/dev/null 2>&1
  fi
  
  # Use ssh-keyscan to get host keys and add them to known_hosts
  log_debug "Scanning for host keys (hostname: $host, port: $port)"
  if ssh-keyscan -p "$port" -H "$host" >> ~/.ssh/known_hosts 2>/dev/null; then
    log_success "Host key for $host added to known_hosts file"
  else
    log_error "Failed to add host key for $host"
    return 1
  fi
  
  # Additionally add IP and FQDN if available
  if [[ -n "$host_ip" ]]; then
    log_debug "Scanning for host keys (IP: $host_ip, port: $port)"
    if ssh-keyscan -p "$port" -H "$host_ip" >> ~/.ssh/known_hosts 2>/dev/null; then
      log_success "Host key for IP $host_ip added to known_hosts file"
    else
      log_warn "Failed to add host key for IP $host_ip"
    fi
  fi
  
  if [[ -n "$host_fqdn" ]]; then
    log_debug "Scanning for host keys (FQDN: $host_fqdn, port: $port)"
    if ssh-keyscan -p "$port" -H "$host_fqdn" >> ~/.ssh/known_hosts 2>/dev/null; then
      log_success "Host key for FQDN $host_fqdn added to known_hosts file"
    else
      log_warn "Failed to add host key for FQDN $host_fqdn"
    fi
  fi
  
  # Verify key was actually added by testing connection
  log_debug "Testing SSH connection to $host with new key"
  if ssh -o BatchMode=yes -o ConnectTimeout=5 -p "$port" root@$host "echo Connected" &>/dev/null; then
    log_success "SSH connection to $host verified successfully"
    return 0
  else
    log_error "SSH connection to $host still fails after adding host key"
    return 1
  fi
}

# Verify SSH host keys and ensure connections can be made without prompts
function verify_ssh_hosts() {
  log_section "Verifying SSH connectivity to hosts"
  
  # First, verify that we can locate all nodes and hosts
  log_debug "Nodes configured: ${NODES[*]}"
  log_debug "Proxmox hosts configured: ${PROXMOX_HOSTS[*]}"
  
  # Collect all hosts that need verification
  declare -a all_hosts=()
  
  # Add all nodes
  for node in "${NODES[@]}"; do
    log_debug "Adding node to verification list: $node"
    all_hosts+=("$node")
  done
  
  # Add all Proxmox hosts that aren't already in nodes list
  for host in "${PROXMOX_HOSTS[@]}"; do
    # Check if host is already in our list
    local already_included=false
    for existing in "${all_hosts[@]}"; do
      if [[ "$existing" == "$host" ]]; then
        already_included=true
        break
      fi
    done
    
    if [[ "$already_included" == "false" ]]; then
      log_debug "Adding Proxmox host to verification list: $host"
      all_hosts+=("$host")
    else
      log_debug "Proxmox host already in list: $host"
    fi
  done
  
  # Check for any node-specific Proxmox hosts that might not be in PROXMOX_HOSTS
  for node in "${NODES[@]}"; do
    local proxmox_host=$(yq -r ".node_details.$node.proxmox_host // \"\"" "$CONFIG_FILE")
    
    if [[ -n "$proxmox_host" ]]; then
      # Check if this host is already in our list
      local already_included=false
      for existing in "${all_hosts[@]}"; do
        if [[ "$existing" == "$proxmox_host" ]]; then
          already_included=true
          break
        fi
      done
      
      if [[ "$already_included" == "false" ]]; then
        log_debug "Adding node-specific Proxmox host to verification list: $proxmox_host"
        all_hosts+=("$proxmox_host")
      else
        log_debug "Node-specific Proxmox host already in list: $proxmox_host"
      fi
    fi
  done
  
  log_info "Hosts to verify: ${all_hosts[*]}"
  
  # Clear existing host keys if --force is used
  if [[ "$FORCE" == "true" ]]; then
    log_info "Force mode enabled, clearing any existing host keys..."
    
    for host in "${all_hosts[@]}"; do
      log_info "Removing any existing host keys for $host..."
      ssh-keygen -R "$host" >/dev/null 2>&1
      
      # Try with IP address if it's a hostname
      if [[ ! "$host" =~ ^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
        local ip=$(getent hosts "$host" | awk '{print $1}' | head -1)
        if [[ -n "$ip" ]]; then
          log_debug "Also removing keys for IP $ip"
          ssh-keygen -R "$ip" >/dev/null 2>&1
        fi
      fi
      
      # Try with FQDN if it looks like a short hostname
      if [[ ! "$host" =~ \. && ! "$host" =~ ^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
        # Try to append domain from /etc/resolv.conf
        local domain=$(grep "^domain\|^search" /etc/resolv.conf | head -1 | awk '{print $2}')
        if [[ -n "$domain" ]]; then
          local fqdn="${host}.${domain}"
          log_debug "Also removing keys for FQDN $fqdn"
          ssh-keygen -R "$fqdn" >/dev/null 2>&1
        fi
      fi
    done
  fi

  # Verify SSH connectivity to all hosts
  local all_verified=true
  
  for host in "${all_hosts[@]}"; do
    log_info "Verifying SSH connectivity to $host"
    
    # Try to connect without prompts
    if ssh -o BatchMode=yes -o ConnectTimeout=5 -p "$SSH_PORT" root@$host "echo Connected" &>/dev/null; then
      log_success "SSH connection to $host verified"
      continue
    fi
    
    log_warn "Cannot connect to $host without prompt, attempting to accept host key..."
    
    # Accept and add the host key
    if [[ "$FORCE" == "true" ]]; then
      log_info "Force mode enabled, automatically adding host key"
      if ! add_host_key "$host" "$SSH_PORT"; then
        log_error "Failed to add host key for $host"
        all_verified=false
        continue
      fi
    else
      # Interactive mode
      log_info "SSH connection requires host key verification for $host"
      if [[ "$INTERACTIVE" == "true" ]]; then
        if confirm "Accept and add host key for $host?"; then
          if ! add_host_key "$host" "$SSH_PORT"; then
            log_error "Failed to add host key for $host"
            all_verified=false
            continue
          fi
        else
          log_error "Host key verification rejected for $host"
          all_verified=false
          continue
        fi
      else
        log_error "Host key verification required for $host. Run in interactive mode or use --force"
        all_verified=false
        continue
      fi
    fi
    
    # Verify connection after adding the key
    if ! ssh -o BatchMode=yes -o ConnectTimeout=5 -p "$SSH_PORT" root@$host "echo Connected" &>/dev/null; then
      log_error "Failed to connect to $host despite adding host key"
      all_verified=false
    else
      log_success "Connection to $host verified after adding host key"
    fi
  done
  
  if [[ "$all_verified" == "true" ]]; then
    log_success "All SSH connections verified"
    return 0
  else
    log_error "Failed to verify SSH connectivity to all hosts"
    return 1
  fi
}

# Writes debug log to a file
function ssh_debug_log() {
  echo "$(date '+%Y-%m-%d %H:%M:%S') SSH_DEBUG: $1" >> /tmp/ssh_debug.log
}

# Enhanced SSH command executor with debugging
function ssh_cmd() {
  # Default values
  local target="$1"
  local cmd="$2"
  local user="${3:-root}"
  local mode="${4:-normal}"  # normal, silent, quiet, capture
  local port="${SSH_PORT:-22}"
  
  # Get local hostname for local execution detection
  local hostname=$(hostname -f)
  local short_hostname=$(hostname -s)
  
  # Debug logging
  ssh_debug_log "============= NEW SSH COMMAND ============="
  ssh_debug_log "Target: $target"
  ssh_debug_log "User: $user"
  ssh_debug_log "Mode: $mode"
  ssh_debug_log "Port: $port"
  ssh_debug_log "Command: $cmd"
  
  # Check if target is local host
  if [[ "$target" == "localhost" || "$target" == "127.0.0.1" || "$target" == "$hostname" || "$target" == "$short_hostname" ]]; then
    ssh_debug_log "Executing command locally"
    
    case "$mode" in
      silent)
        ssh_debug_log "Mode: silent (suppressing all output)"
        eval "$cmd" &>/dev/null
        local exit_code=$?
        ssh_debug_log "Exit code: $exit_code"
        return $exit_code
        ;;
      quiet)
        ssh_debug_log "Mode: quiet (suppressing stderr)"
        eval "$cmd" 2>/dev/null
        local exit_code=$?
        ssh_debug_log "Exit code: $exit_code"
        return $exit_code
        ;;
      capture)
        ssh_debug_log "Mode: capture (capturing all output)"
        local output=$(eval "$cmd" 2>&1)
        local exit_code=$?
        ssh_debug_log "Exit code: $exit_code"
        ssh_debug_log "Output: $output"
        echo "$output"
        return $exit_code
        ;;
      *)
        ssh_debug_log "Mode: normal"
        eval "$cmd"
        local exit_code=$?
        ssh_debug_log "Exit code: $exit_code"
        return $exit_code
        ;;
    esac
  else
    ssh_debug_log "Executing command via SSH to $target"
    
    # Debug connection
    ssh_debug_log "Testing connection first..."
    if ssh -o BatchMode=yes -o ConnectTimeout=2 -p "$port" "$user@$target" "echo TEST_CONNECTION_OK" &>/dev/null; then
      ssh_debug_log "Connection test successful"
    else
      ssh_debug_log "Connection test FAILED"
    fi
    
    # For special commands involving qm, add extra debugging
    if [[ "$cmd" == "qm "* || "$cmd" == *"qm "* ]]; then
      ssh_debug_log "IMPORTANT: Detected qm command: $cmd"
    fi
    
    # Construct the SSH command - this is where issues might occur
    local ssh_full_cmd="ssh -o BatchMode=yes -o ConnectTimeout=10 -p $port $user@$target \"$cmd\""
    ssh_debug_log "Full SSH command being executed: $ssh_full_cmd"
    
    case "$mode" in
      silent)
        ssh_debug_log "Mode: silent (suppressing all output)"
        local output=$(eval "$ssh_full_cmd" 2>&1)
        local exit_code=$?
        ssh_debug_log "Exit code: $exit_code"
        ssh_debug_log "Output (not shown to user): $output"
        return $exit_code
        ;;
      quiet)
        ssh_debug_log "Mode: quiet (suppressing stderr)"
        local output=$(eval "$ssh_full_cmd" 2>&1)
        local exit_code=$?
        ssh_debug_log "Exit code: $exit_code"
        ssh_debug_log "Output (partially shown to user): $output"
        echo "$output" | grep -v "^ssh:" | grep -v "^Warning:"
        return $exit_code
        ;;
      capture)
        ssh_debug_log "Mode: capture (capturing all output)"
        local output=$(eval "$ssh_full_cmd" 2>&1)
        local exit_code=$?
        ssh_debug_log "Exit code: $exit_code"
        ssh_debug_log "Output: $output"
        echo "$output"
        return $exit_code
        ;;
      *)
        ssh_debug_log "Mode: normal"
        local output=$(eval "$ssh_full_cmd" 2>&1)
        local exit_code=$?
        ssh_debug_log "Exit code: $exit_code"
        ssh_debug_log "Output: $output"
        echo "$output"
        return $exit_code
        ;;
    esac
  fi
}

# SSH wrapper - all output suppressed (both stdout and stderr)
function ssh_cmd_silent() {
  local target="$1"
  local cmd="$2"
  local user="${3:-root}"

  # Get local hostname
  local hostname=$(hostname -f)
  local short_hostname=$(hostname -s)
  
  # Check if target is local host
  if [[ "$target" == "localhost" || "$target" == "127.0.0.1" || "$target" == "$hostname" || "$target" == "$short_hostname" ]]; then
    log_debug "Executing command locally: $cmd"
    eval "$cmd" &>/dev/null
    return $?
  else
    log_debug "Executing command via SSH on $target: $cmd"
    ssh -o BatchMode=yes -p "$SSH_PORT" $user@$target "$cmd" &>/dev/null
    return $?
  fi
}

# SSH wrapper - errors suppressed
function ssh_cmd_quiet() {
  local target="$1"
  local cmd="$2"
  local user="${3:-root}"
  
  # Get local hostname
  local hostname=$(hostname -f)
  local short_hostname=$(hostname -s)
  
  # Check if target is local host
  if [[ "$target" == "localhost" || "$target" == "127.0.0.1" || "$target" == "$hostname" || "$target" == "$short_hostname" ]]; then
    log_debug "Executing command locally: $cmd"
    eval "$cmd" 2>/dev/null
    return $?
  else
    log_debug "Executing command via SSH on $target: $cmd"
    ssh -o BatchMode=yes -p "$SSH_PORT" $user@$target "$cmd" 2>/dev/null
    return $?
  fi
}

# SSH wrapper - errors captured with output
function ssh_cmd_capture() {
  local target="$1"
  local cmd="$2"
  local user="${3:-root}"
  
  # Get local hostname
  local hostname=$(hostname -f)
  local short_hostname=$(hostname -s)
  
  # Check if target is local host
  if [[ "$target" == "localhost" || "$target" == "127.0.0.1" || "$target" == "$hostname" || "$target" == "$short_hostname" ]]; then
    log_debug "Executing command locally: $cmd"
    eval "$cmd" 2>&1
    return $?
  else
    log_debug "Executing command via SSH on $target: $cmd"
    ssh -o BatchMode=yes -p "$SSH_PORT" $user@$target "$cmd" 2>&1
    return $?
  fi
}

# Run in interactive mode
function run_interactive_mode() {
  log_section "Interactive Mode"
  
  # Display main menu
  echo "K3s Cluster Admin - Interactive Mode"
  echo "1. Validate cluster health"
  echo "2. Shutdown node"
  echo "3. Start node"
  echo "4. Create backup"
  echo "5. Create snapshot"
  echo "6. Replace node"
  echo "7. Restore cluster"
  echo "8. Generate sample config"
  echo "9. Display version info"
  echo "0. Exit"
  
  read -p "Select an option (0-9): " option
  
  # Add this block RIGHT HERE to verify SSH hosts immediately after option selection
  # but before any option-specific operations
  case "$option" in
    1|2|3|4|5|6|7)
      # For options that require SSH, verify connectivity first
      verify_ssh_hosts || return 1
      ;;
  esac
  
  case "$option" in
    1)
      # Interactive validation
      echo "Validation levels:"
      echo "1. Basic validation"
      echo "2. Extended validation"
      echo "3. Full validation"
      read -p "Select validation level (1-3): " val_level
      
      case "$val_level" in
        1) VALIDATE_LEVEL="basic" ;;
        2) VALIDATE_LEVEL="extended" ;;
        3) VALIDATE_LEVEL="full" ;;
        *) log_error "Invalid option" && return 1 ;;
      esac
      
      validate_cluster
      ;;
    
    2)
      # Interactive shutdown
      if [[ ${#NODES[@]} -eq 0 ]]; then
        log_error "No nodes configured"
        return 1
      fi
      
      echo "Available nodes:"
      for i in "${!NODES[@]}"; do
        echo "$((i+1)). ${NODES[$i]}"
      done
      
      read -p "Select node to shutdown (1-${#NODES[@]}, or 'a' for all): " node_option
      
      if [[ "$node_option" == "a" ]]; then
        # Keep all nodes
        log_info "Will shutdown all nodes: ${NODES[*]}"
        
        if confirm "Are you sure you want to shutdown all nodes?"; then
          shutdown_node
        else
          log_info "Operation cancelled"
          return 0
        fi
      elif [[ "$node_option" =~ ^[0-9]+$ && "$node_option" -ge 1 && "$node_option" -le "${#NODES[@]}" ]]; then
        # Select specific node
        local selected_node="${NODES[$((node_option-1))]}"
        NODES=("$selected_node")
        
        log_info "Selected node: $selected_node"
        
        if confirm "Are you sure you want to shutdown node $selected_node?"; then
          shutdown_node
        else
          log_info "Operation cancelled"
          return 0
        fi
      else
        log_error "Invalid option: $node_option"
        return 1
      fi
      ;;
    
    3)
      # Interactive start node
      if [[ ${#NODES[@]} -eq 0 ]]; then
        log_error "No nodes configured"
        return 1
      fi
      
      echo "Available nodes:"
      for i in "${!NODES[@]}"; do
        echo "$((i+1)). ${NODES[$i]}"
      done
      
      read -p "Select node to start (1-${#NODES[@]}): " node_option
      
      if [[ "$node_option" =~ ^[0-9]+$ && "$node_option" -ge 1 && "$node_option" -le "${#NODES[@]}" ]]; then
        # Select specific node
        local selected_node="${NODES[$((node_option-1))]}"
        
        log_info "Selected node: $selected_node"
        
        if confirm "Are you sure you want to start node $selected_node?"; then
          start_node "$selected_node"
        else
          log_info "Operation cancelled"
          return 0
        fi
      else
        log_error "Invalid option: $node_option"
        return 1
      fi
      ;;
    
    4)
      # Interactive backup
      if confirm "Create backup of the entire cluster?"; then
        read -p "Enter retention count (leave empty for default: $DEFAULT_RETENTION_COUNT): " retention
        
        if [[ -n "$retention" ]]; then
          RETENTION_COUNT="$retention"
        fi
        
        backup_cluster
      else
        log_info "Backup cancelled"
        return 0
      fi
      ;;
    
    5)
      # Interactive snapshot
      log_section "Snapshot Configuration"
      
      if confirm "Create snapshot of the entire cluster?" "y"; then
        # All nodes will be snapshotted
        read -p "Enter retention count (leave empty for default: $DEFAULT_RETENTION_COUNT): " retention
        
        if [[ -n "$retention" ]]; then
          RETENTION_COUNT="$retention"
        fi
        
        snapshot_cluster
      else
        # Select specific nodes to snapshot
        log_subsection "Node Selection"
        echo "Available nodes:"
        for i in "${!NODES[@]}"; do
          echo "$((i+1)). ${NODES[$i]}"
        done
        
        # Get node selection
        local original_nodes=("${NODES[@]}")
        local selected_indices=()
        read -p "Enter node numbers to snapshot (comma-separated, e.g., 1,3,4): " node_selection
        
        # Parse selection
        IFS=',' read -ra selected_items <<< "$node_selection"
        for item in "${selected_items[@]}"; do
          # Trim any whitespace
          item=$(echo "$item" | tr -d '[:space:]')
          if [[ "$item" =~ ^[0-9]+$ && "$item" -ge 1 && "$item" -le "${#NODES[@]}" ]]; then
            selected_indices+=($((item-1)))
          else
            log_warn "Invalid selection: $item, ignoring"
          fi
        done
        
        if [[ ${#selected_indices[@]} -eq 0 ]]; then
          log_error "No valid nodes selected"
          return 1
        fi
        
        # Create new nodes array with only selected nodes
        local selected_nodes=()
        for idx in "${selected_indices[@]}"; do
          selected_nodes+=("${NODES[$idx]}")
        done
        
        # Replace NODES array with selected nodes
        NODES=("${selected_nodes[@]}")
        log_info "Selected nodes for snapshot: ${NODES[*]}"
        
        read -p "Enter retention count (leave empty for default: $DEFAULT_RETENTION_COUNT): " retention
        
        if [[ -n "$retention" ]]; then
          RETENTION_COUNT="$retention"
        fi
        
        # Capture the return value of snapshot_cluster
        snapshot_cluster "from_interactive"
        local snapshot_result=$?
        
        # Restore original nodes array
        NODES=("${original_nodes[@]}")
        
        # Return the result of snapshot_cluster
        return $snapshot_result
      fi
      ;;
    
    6)
      # Interactive replace
      if [[ ${#NODES[@]} -eq 0 ]]; then
        log_error "No nodes configured"
        return 1
      fi
      
      echo "Available nodes:"
      for i in "${!NODES[@]}"; do
        echo "$((i+1)). ${NODES[$i]}"
      done
      
      read -p "Select node to replace (1-${#NODES[@]}): " node_option
      
      if [[ "$node_option" =~ ^[0-9]+$ && "$node_option" -ge 1 && "$node_option" -le "${#NODES[@]}" ]]; then
        # Select specific node
        local selected_node="${NODES[$((node_option-1))]}"
        NODES=("$selected_node")
        
        log_info "Selected node: $selected_node"
        
        if confirm "Are you sure you want to replace node $selected_node?"; then
          replace_node
        else
          log_info "Operation cancelled"
          return 0
        fi
      else
        log_error "Invalid option: $node_option"
        return 1
      fi
      ;;
    
    7)
      # Interactive restore
      run_restore_wizard
      ;;
      
    8)
      # Generate sample config
      read -p "Enter config file name or path: " config_path
      
      if [[ -z "$config_path" ]]; then
        config_path="cluster-config-sample.yaml"
      fi
      
      if [[ -f "$config_path" ]]; then
        if ! confirm "File $config_path already exists. Overwrite?"; then
          log_info "Operation cancelled"
          return 0
        fi
      fi
      
      FORCE="true"
      generate_sample_config "$config_path" "true"  # Pass "true" to indicate interactive mode
      FORCE="false"
      ;;

    9)
      # Display version information
      cmd_version
      ;;

    0)
      log_info "Exiting interactive mode"
      return 0
      ;;
    
    *)
      log_error "Invalid option: $option"
      return 1
      ;;
  esac
  
  return 0
}

# Send notification
function send_notification() {
  local subject="$1"
  local message="$2"
  
  if [[ "$NOTIFY_ENABLED" != "true" ]]; then
    return 0
  fi
  
  if [[ -z "$NOTIFY_EMAIL" ]]; then
    log_warn "Notification enabled but no email address configured"
    return 1
  fi
  
  log_info "Sending notification to $NOTIFY_EMAIL"
  
  if command_exists mail; then
    echo "$message" | mail -s "$subject" "$NOTIFY_EMAIL"
    return $?
  else
    log_warn "mail command not found, cannot send notification"
    return 1
  fi
}

# Run a command with timeout
function run_with_timeout() {
  local timeout=$1
  local cmd=$2
  local message=$3
  
  log_info "$message"
  log_debug "Running command with $timeout second timeout: $cmd"
  
  timeout $timeout bash -c "$cmd"
  return $?
}

# Initialize logging
function init_logging() {
  # Set up log file if specified
  if [[ -n "$LOG_DIR" ]]; then
    mkdir -p "$LOG_DIR" &>/dev/null
    LOG_FILE="${LOG_DIR}/k3s-admin-${TIMESTAMP}.log"
    log_info "Logging to $LOG_FILE"
  fi
}
