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

# Add a host key to known_hosts file
function add_host_key() {
  local host="$1"
  local port="${2:-$SSH_PORT}"
  
  log_info "Adding host key for $host to known_hosts file..."
  
  # Use ssh-keyscan to get the host key and append it to known_hosts
  if ssh-keyscan -p "$port" -H "$host" >> ~/.ssh/known_hosts 2>/dev/null; then
    log_success "Host key for $host added to known_hosts file"
    return 0
  else
    log_error "Failed to add host key for $host"
    return 1
  fi
}

# Verify SSH host keys and ensure connections can be made without prompts
function verify_ssh_hosts() {
  log_section "Verifying SSH connectivity to hosts"
  
  # Clear existing host keys if --force is used
  if [[ "$FORCE" == "true" ]]; then
    log_info "Force mode enabled, clearing any existing host keys..."
    
    # Clear keys for nodes
    for node in "${NODES[@]}"; do
      log_info "Removing any existing host keys for $node..."
      ssh-keygen -R "$node" >/dev/null 2>&1
    done
    
    # Clear keys for Proxmox hosts
    for host in "${PROXMOX_HOSTS[@]}"; do
      log_info "Removing any existing host keys for $host..."
      ssh-keygen -R "$host" >/dev/null 2>&1
      # Also try with FQDN if different
      ssh-keygen -R "$host.ldv.corp" >/dev/null 2>&1
    done
  fi

  # First check connectivity to nodes
  log_info "Verifying connectivity to cluster nodes..."
  for node in "${NODES[@]}"; do
    log_info "Testing connection to node: $node"
    
    # Check if we can connect without prompts
    if ! ssh -o BatchMode=yes -o ConnectTimeout=5 -p "$SSH_PORT" root@$node "echo Connected" &>/dev/null; then
      log_warn "Cannot connect to $node without prompt, attempting to accept host key..."
      
      # Accept and permanently add the host key
      if [[ "$FORCE" == "true" ]]; then
        log_info "Force mode enabled, automatically adding node key"
        add_host_key "$node" "$SSH_PORT"
      else
        # Interactive mode
        log_info "SSH connection requires host key verification for node $node"
        log_info "To continue non-interactively in the future, use the --force flag"
        
        if [[ "$INTERACTIVE" == "true" ]]; then
          if confirm "Accept and add host key for node $node?"; then
            add_host_key "$node" "$SSH_PORT"
          else
            log_error "Host key verification rejected, cannot continue"
            return 1
          fi
        else
          log_error "Host key verification required for node $node. Run in interactive mode or use --force"
          return 1
        fi
      fi

      # Verify connection after adding the key
      local result=0
      if ! ssh -o BatchMode=yes -o ConnectTimeout=5 -p "$SSH_PORT" root@$node "echo Connected" &>/dev/null; then
        log_error "Failed to connect to $node despite adding host key"
        result=1
      else
        log_success "Connection to $node verified after adding host key"
      fi

      return $result
    else
      log_success "Connection to $node verified"
    fi
  done
  
  # Then check connectivity to Proxmox hosts
  log_info "Verifying connectivity to Proxmox hosts..."
  for host in "${PROXMOX_HOSTS[@]}"; do
    log_info "Testing connection to Proxmox host: $host"
    
    # Skip if host is one of the nodes we already verified
    local already_verified=false
    for node in "${NODES[@]}"; do
      if [[ "$host" == "$node" ]]; then
        already_verified=true
        break
      fi
    done
    
    if [[ "$already_verified" == "true" ]]; then
      log_info "Host $host already verified as a node"
      continue
    fi
    
    # Check if we can connect without prompts
    if ! ssh -o BatchMode=yes -o ConnectTimeout=5 -p "$SSH_PORT" root@$host "echo Connected" &>/dev/null; then
      log_warn "Cannot connect to $host without prompt, attempting to accept host key..."
      
      # Accept and permanently add the host key
      if [[ "$FORCE" == "true" ]]; then
        log_info "Force mode enabled, automatically adding host key"
        add_host_key "$host" "$SSH_PORT"
      else
        # Interactive mode
        log_info "SSH connection requires host key verification for host $host"
        log_info "To continue non-interactively in the future, use the --force flag"
        
        if [[ "$INTERACTIVE" == "true" ]]; then
          if confirm "Accept and add host key for host $host?"; then
            add_host_key "$host" "$SSH_PORT"
          else
            log_error "Host key verification rejected, cannot continue"
            return 1
          fi
        else
          log_error "Host key verification required for host $host. Run in interactive mode or use --force"
          return 1
        fi
      fi

      # Verify connection after adding the key
      local result=0
      if ! ssh -o BatchMode=yes -o ConnectTimeout=5 -p "$SSH_PORT" root@$host "echo Connected" &>/dev/null; then
        log_error "Failed to connect to $host despite adding host key"
        result=1
      else
        log_success "Connection to $host verified after adding host key"
      fi

      return $result
    else
      log_success "Connection to $host verified"
    fi
  done
  
  log_success "All SSH connections verified"
  return 0
}

# Unified SSH command executor with flexible output options
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
  
  # More detailed debug information
  log_debug "SSH command details:"
  log_debug "  Target: $target"
  log_debug "  User: $user"
  log_debug "  Mode: $mode"
  log_debug "  Port: $port"
  log_debug "  Command: $cmd"
  
  # Check if target is local host
  if [[ "$target" == "localhost" || "$target" == "127.0.0.1" || "$target" == "$hostname" || "$target" == "$short_hostname" ]]; then
    log_debug "Executing command locally"
    
    case "$mode" in
      silent)
        eval "$cmd" &>/dev/null
        return $?
        ;;
      quiet)
        eval "$cmd" 2>/dev/null
        return $?
        ;;
      capture)
        eval "$cmd" 2>&1
        return $?
        ;;
      *)
        eval "$cmd"
        return $?
        ;;
    esac
  else
    # Additional debug info for SSH connection
    log_debug "Executing command via SSH to $target:$port as $user"
    
    # Properly quote and escape the command to prevent shell interpretation issues
    # Use single quotes to prevent local shell expansion
    local escaped_cmd=$(printf "%q" "$cmd")
    
    case "$mode" in
      silent)
        ssh -o BatchMode=yes -o ConnectTimeout=10 -p "$port" $user@$target "$escaped_cmd" &>/dev/null
        return $?
        ;;
      quiet)
        ssh -o BatchMode=yes -o ConnectTimeout=10 -p "$port" $user@$target "$escaped_cmd" 2>/dev/null
        return $?
        ;;
      capture)
        ssh -o BatchMode=yes -o ConnectTimeout=10 -p "$port" $user@$target "$escaped_cmd" 2>&1
        return $?
        ;;
      *)
        ssh -o BatchMode=yes -o ConnectTimeout=10 -p "$port" $user@$target "$escaped_cmd"
        return $?
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
      if confirm "Create snapshot of the entire cluster?" "y"; then
        # All nodes will be snapshotted
        read -p "Enter retention count (leave empty for default: $DEFAULT_RETENTION_COUNT): " retention
        
        if [[ -n "$retention" ]]; then
          RETENTION_COUNT="$retention"
        fi
        
        snapshot_cluster
      else
        # Select specific nodes to snapshot
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
        snapshot_cluster
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
