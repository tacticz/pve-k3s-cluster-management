#!/bin/bash
# utils.sh - Utility functions
#
# This module is part of the k3s-cluster-management
# It provides common utility functions used throughout the script.
#
# Author: S-tor + claude.ai
# Date: February 2025
# Version: 0.1.0

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
  echo "9. Exit"
  
  read -p "Select an option (1-9): " option
  
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
      if confirm "Create snapshot of the entire cluster?"; then
        read -p "Enter retention count (leave empty for default: $DEFAULT_RETENTION_COUNT): " retention
        
        if [[ -n "$retention" ]]; then
          RETENTION_COUNT="$retention"
        fi
        
        snapshot_cluster
      else
        log_info "Snapshot creation cancelled"
        return 0
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
      read -p "Enter output file path: " config_path
      
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
      generate_sample_config "$config_path"
      FORCE="false"
      ;;
    
    9)
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
