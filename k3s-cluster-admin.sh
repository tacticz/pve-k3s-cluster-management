#!/bin/bash
# k3s-cluster-admin.sh - K3s Cluster Administration Script
# 
# This module is part of the k3s-cluster-management
# It provides utilities for managing a k3s cluster running on Proxmox VMs:
# - Safe node shutdown
# - Cluster backups (Proxmox VM snapshots + etcd)
# - Node replacement
#
# Usage: ./k3s-cluster-admin.sh [options] <command>
#
# Author: S-tor + claude.ai
# Date: February 2025
# Version: 0.1.0

set -eo pipefail

# Script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Load modules
source "${SCRIPT_DIR}/lib/config.sh"
source "${SCRIPT_DIR}/lib/validation.sh"
source "${SCRIPT_DIR}/lib/node_ops.sh"
source "${SCRIPT_DIR}/lib/backup.sh"
source "${SCRIPT_DIR}/lib/restore.sh"
source "${SCRIPT_DIR}/lib/utils.sh"

# Default configuration values
DEFAULT_CONFIG_FILE="${SCRIPT_DIR}/conf/cluster-config.yaml"
DEFAULT_RETENTION_COUNT=5
DEFAULT_DRAINING_TIMEOUT=300  # 5 minutes
DEFAULT_OPERATION_TIMEOUT=600 # 10 minutes
DEFAULT_VALIDATE_LEVEL="basic" # basic, extended, full

# Command functions
function cmd_help() {
  cat <<EOF
K3s Cluster Admin Script

Usage: 
  $(basename "$0") [options] <command>

Commands:
  shutdown       Safely shutdown a node VM
  backup         Create a backup of the entire cluster
  snapshot       Create a snapshot of the entire cluster
  replace        Replace a node in the cluster
  validate       Validate cluster health
  help           Show this help message

Options:
  -c, --config FILE    Path to configuration file (default: ${DEFAULT_CONFIG_FILE})
  -n, --node NODE      Target node (hostname or IP)
  -a, --all-nodes      Apply operation to all nodes sequentially
  -f, --force          Skip confirmations
  -r, --retention N    Number of snapshots/backups to keep (default: ${DEFAULT_RETENTION_COUNT})
  -i, --interactive    Run in interactive mode
  -v, --validate LEVEL Validation level: basic, extended, full (default: ${DEFAULT_VALIDATE_LEVEL})
  -h, --help           Show this help message
  -d, --dry-run        Show what would be done without doing it

Examples:
  $(basename "$0") --node k3s-node1 shutdown
  $(basename "$0") --config custom-config.yaml backup
  $(basename "$0") --interactive replace
  $(basename "$0") snapshot --all-nodes --retention 10

For more details, see the documentation.
EOF
}

function parse_args() {
  # Command line option parsing
  while [[ $# -gt 0 ]]; do
    case "$1" in
      -c|--config)
        CONFIG_FILE="$2"
        shift 2
        ;;
      -n|--node)
        TARGET_NODE="$2"
        shift 2
        ;;
      -a|--all-nodes)
        ALL_NODES=true
        shift
        ;;
      -f|--force)
        FORCE=true
        shift
        ;;
      -r|--retention)
        RETENTION_COUNT="$2"
        shift 2
        ;;
      -i|--interactive)
        INTERACTIVE=true
        shift
        ;;
      -v|--validate)
        VALIDATE_LEVEL="$2"
        shift 2
        ;;
      -h|--help)
        cmd_help
        exit 0
        ;;
      -d|--dry-run)
        DRY_RUN=true
        shift
        ;;
      shutdown|backup|snapshot|replace|validate|help)
        COMMAND="$1"
        shift
        ;;
      *)
        echo "Error: Unknown option $1"
        cmd_help
        exit 1
        ;;
    esac
  done

  # Set defaults if not specified
  CONFIG_FILE="${CONFIG_FILE:-$DEFAULT_CONFIG_FILE}"
  RETENTION_COUNT="${RETENTION_COUNT:-$DEFAULT_RETENTION_COUNT}"
  VALIDATE_LEVEL="${VALIDATE_LEVEL:-$DEFAULT_VALIDATE_LEVEL}"
  FORCE="${FORCE:-false}"
  ALL_NODES="${ALL_NODES:-false}"
  INTERACTIVE="${INTERACTIVE:-false}"
  DRY_RUN="${DRY_RUN:-false}"
}

function main() {
  parse_args "$@"

  # Set command to help if none provided
  COMMAND="${COMMAND:-help}"

  # Load configuration
  if [[ -f "$CONFIG_FILE" ]]; then
    log_info "Loading configuration from $CONFIG_FILE"
    load_config "$CONFIG_FILE"
  elif [[ "$CONFIG_FILE" != "$DEFAULT_CONFIG_FILE" ]]; then
    log_error "Configuration file $CONFIG_FILE not found."
    exit 1
  fi

  # Combine configuration priorities (CLI > Config file > Defaults)
  merge_config
  
  # Print configuration in verbose mode
  if [[ "$VERBOSE" == "true" ]]; then
    print_config
  fi

  # Check for interactive mode
  if [[ "$INTERACTIVE" == "true" ]]; then
    run_interactive_mode
    exit 0
  fi

  # Run the specified command
  case "$COMMAND" in
    help)
      cmd_help
      ;;
    validate)
      validate_cluster
      ;;
    shutdown)
      shutdown_node
      ;;
    backup)
      backup_cluster
      ;;
    snapshot)
      snapshot_cluster
      ;;
    replace)
      replace_node
      ;;
    *)
      log_error "Unknown command: $COMMAND"
      cmd_help
      exit 1
      ;;
  esac
}

# Run the script
main "$@"
