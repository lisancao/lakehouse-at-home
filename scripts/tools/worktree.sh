#!/usr/bin/env bash
#
# Git worktree helper for lakehouse-stack
# Creates worktrees with shared jars and copied configs
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

usage() {
    cat << EOF
Usage: $(basename "$0") <command> [options]

Commands:
  add <name> [branch]    Create a new worktree
                         - name: directory name (created as sibling to repo)
                         - branch: existing branch or new branch name (default: new branch from current)

  list                   List all worktrees

  remove <name>          Remove a worktree

  ports <name> <offset>  Adjust ports in a worktree's docker-compose files
                         - offset: number to add to all ports (e.g., 100)

Examples:
  $(basename "$0") add hotfix feature/hotfix-123
  $(basename "$0") add experiment                    # Creates new branch 'experiment'
  $(basename "$0") ports hotfix 100                  # Shift ports by +100
  $(basename "$0") remove hotfix

EOF
    exit 1
}

log() { echo -e "${GREEN}[worktree]${NC} $1"; }
warn() { echo -e "${YELLOW}[worktree]${NC} $1"; }
error() { echo -e "${RED}[worktree]${NC} $1" >&2; }

cmd_add() {
    local name="${1:-}"
    local branch="${2:-}"

    if [[ -z "$name" ]]; then
        error "Name required"
        usage
    fi

    local worktree_path="$(dirname "$REPO_ROOT")/lakehouse-$name"

    if [[ -d "$worktree_path" ]]; then
        error "Directory already exists: $worktree_path"
        exit 1
    fi

    # Determine branch strategy
    if [[ -z "$branch" ]]; then
        # Create new branch with the given name
        branch="$name"
        log "Creating worktree with new branch '$branch'..."
        git worktree add -b "$branch" "$worktree_path"
    elif git show-ref --verify --quiet "refs/heads/$branch" 2>/dev/null; then
        # Existing local branch
        log "Creating worktree for existing branch '$branch'..."
        git worktree add "$worktree_path" "$branch"
    elif git show-ref --verify --quiet "refs/remotes/origin/$branch" 2>/dev/null; then
        # Remote branch - create tracking branch
        log "Creating worktree tracking 'origin/$branch'..."
        git worktree add --track -b "$branch" "$worktree_path" "origin/$branch"
    else
        # New branch
        log "Creating worktree with new branch '$branch'..."
        git worktree add -b "$branch" "$worktree_path"
    fi

    log "Setting up shared resources..."

    # Remove jars directory and symlink to main repo's jars
    if [[ -d "$worktree_path/jars" ]]; then
        rm -rf "$worktree_path/jars"
    fi
    ln -s "$REPO_ROOT/jars" "$worktree_path/jars"
    log "  ✓ Symlinked jars/ (saves ~1GB)"

    # Copy .env if it exists
    if [[ -f "$REPO_ROOT/.env" ]]; then
        cp "$REPO_ROOT/.env" "$worktree_path/.env"
        log "  ✓ Copied .env"
    else
        warn "  ⚠ No .env found - copy from .env.example manually"
    fi

    # Copy spark configs if they exist
    for conf in spark-defaults.conf spark-defaults-uc.conf; do
        if [[ -f "$REPO_ROOT/config/spark/$conf" ]]; then
            cp "$REPO_ROOT/config/spark/$conf" "$worktree_path/config/spark/$conf"
            log "  ✓ Copied config/spark/$conf"
        fi
    done

    # Copy unity-catalog config if it exists
    if [[ -f "$REPO_ROOT/config/unity-catalog/server.properties" ]]; then
        cp "$REPO_ROOT/config/unity-catalog/server.properties" "$worktree_path/config/unity-catalog/server.properties"
        log "  ✓ Copied config/unity-catalog/server.properties"
    fi

    echo ""
    log "Worktree created at: ${BLUE}$worktree_path${NC}"
    echo ""
    echo "Next steps:"
    echo "  cd $worktree_path"
    echo ""
    echo "To run services in parallel with main repo, adjust ports:"
    echo "  $0 ports $name 100"
    echo ""
}

cmd_list() {
    log "Worktrees:"
    git worktree list
}

cmd_remove() {
    local name="${1:-}"

    if [[ -z "$name" ]]; then
        error "Name required"
        usage
    fi

    local worktree_path="$(dirname "$REPO_ROOT")/lakehouse-$name"

    if [[ ! -d "$worktree_path" ]]; then
        error "Worktree not found: $worktree_path"
        exit 1
    fi

    log "Removing worktree: $worktree_path"
    git worktree remove "$worktree_path" --force
    log "Done"
}

cmd_ports() {
    local name="${1:-}"
    local offset="${2:-}"

    if [[ -z "$name" || -z "$offset" ]]; then
        error "Name and offset required"
        usage
    fi

    local worktree_path="$(dirname "$REPO_ROOT")/lakehouse-$name"

    if [[ ! -d "$worktree_path" ]]; then
        error "Worktree not found: $worktree_path"
        exit 1
    fi

    log "Adjusting ports in $worktree_path by +$offset..."

    # Port mappings to adjust (host:container format, we only change host)
    # Format: file:original_host_port
    local -a port_mappings=(
        "docker-compose.yml:7077"
        "docker-compose.yml:8080"
        "docker-compose-spark41.yml:7078"
        "docker-compose-spark41.yml:8082"
        "docker-compose-kafka.yml:9092"
        "docker-compose-kafka.yml:2181"
        "docker-compose-unity-catalog.yml:8081"
        "docker-compose-airflow.yml:8085"
    )

    for mapping in "${port_mappings[@]}"; do
        local file="${mapping%%:*}"
        local port="${mapping##*:}"
        local new_port=$((port + offset))
        local filepath="$worktree_path/$file"

        if [[ -f "$filepath" ]]; then
            # Replace host port in "HOST:CONTAINER" mappings
            if grep -q "\"$port:" "$filepath" 2>/dev/null; then
                sed -i "s/\"$port:/\"$new_port:/g" "$filepath"
                log "  $file: $port → $new_port"
            fi
        fi
    done

    # Update .env SPARK_MASTER_PORT if present
    local env_file="$worktree_path/.env"
    if [[ -f "$env_file" ]] && grep -q "SPARK_MASTER_PORT" "$env_file"; then
        local current_port=$(grep "SPARK_MASTER_PORT" "$env_file" | cut -d= -f2)
        local new_port=$((current_port + offset))
        sed -i "s/SPARK_MASTER_PORT=.*/SPARK_MASTER_PORT=$new_port/" "$env_file"
        log "  .env SPARK_MASTER_PORT: $current_port → $new_port"
    fi

    echo ""
    log "Ports adjusted. New port summary:"
    echo "  Spark 4.0:  $((7077 + offset)) (UI: $((8080 + offset)))"
    echo "  Spark 4.1:  $((7078 + offset)) (UI: $((8082 + offset)))"
    echo "  Kafka:      $((9092 + offset))"
    echo "  Zookeeper:  $((2181 + offset))"
    echo "  Unity Cat:  $((8081 + offset))"
    echo "  Airflow:    $((8085 + offset))"
    echo ""
}

# Main
case "${1:-}" in
    add)    shift; cmd_add "$@" ;;
    list)   cmd_list ;;
    remove) shift; cmd_remove "$@" ;;
    ports)  shift; cmd_ports "$@" ;;
    *)      usage ;;
esac
