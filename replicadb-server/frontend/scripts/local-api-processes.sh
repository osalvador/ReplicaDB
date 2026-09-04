#!/usr/bin/env bash

replicadb_process_list() {
    ps -axo pid=,command=
}

replicadb_process_cwd() {
    lsof -a -p "$1" -d cwd -Fn 2>/dev/null | sed -n 's/^n//p'
}

replicadb_process_start() {
    ps -p "$1" -o lstart=
}

replicadb_process_children() {
    local parent_pid="$1"

    ps -axo pid=,ppid= | awk -v parent_pid="$parent_pid" '$2 == parent_pid { print $1 }'
}

replicadb_process_tree() {
    local parent_pid="$1"
    local child_pid

    while IFS= read -r child_pid; do
        [[ -n "$child_pid" ]] || continue
        replicadb_process_tree "$child_pid"
        printf '%s\n' "$child_pid"
    done < <(replicadb_process_children "$parent_pid")
}

replicadb_stop_process_tree() {
    local root_pid="$1"
    local process_tree
    local pid
    local remaining

    process_tree="$(replicadb_process_tree "$root_pid")"
    process_tree+=$'\n'"$root_pid"

    while IFS= read -r pid; do
        [[ -n "$pid" ]] || continue
        kill -TERM "$pid" 2>/dev/null || return 1
    done <<< "$process_tree"

    for _ in $(seq 1 10); do
        remaining=0
        while IFS= read -r pid; do
            [[ -n "$pid" ]] || continue
            if kill -0 "$pid" 2>/dev/null; then
                remaining=1
            fi
        done <<< "$process_tree"
        [[ "$remaining" == 0 ]] && return 0
        sleep 1
    done

    while IFS= read -r pid; do
        [[ -n "$pid" ]] || continue
        kill -KILL "$pid" 2>/dev/null || true
    done <<< "$process_tree"

    return 1
}

local_replica_api_processes() {
    local server_dir="$1"
    local process_line pid command cwd port started

    while IFS= read -r process_line; do
        process_line="${process_line#"${process_line%%[![:space:]]*}"}"
        [[ -n "$process_line" ]] || continue
        pid="${process_line%%[[:space:]]*}"
        command="${process_line#"$pid"}"
        command="${command#"${command%%[![:space:]]*}"}"
        [[ "$command" == *"org.replicadb.server.ReplicaDbServerApplication"* ]] || continue
        [[ "$command" == *"--spring.profiles.active=api"* ]] || continue
        cwd="$(replicadb_process_cwd "$pid")"
        [[ "$cwd" == "$server_dir" ]] || continue
        if [[ "$command" =~ --server\.port=([0-9]+) ]]; then
            port="${BASH_REMATCH[1]}"
        else
            port="unknown"
        fi
        started="$(replicadb_process_start "$pid" | sed 's/^[[:space:]]*//')"
        printf '%s\t%s\t%s\n' "$pid" "$port" "$started"
    done < <(replicadb_process_list)
}

local_replica_api_launchers() {
    local server_dir="$1"
    local process_line pid command cwd port started

    while IFS= read -r process_line; do
        process_line="${process_line#"${process_line%%[![:space:]]*}"}"
        [[ -n "$process_line" ]] || continue
        pid="${process_line%%[[:space:]]*}"
        command="${process_line#"$pid"}"
        command="${command#"${command%%[![:space:]]*}"}"
        [[ "$command" == *'spring-boot:run'* ]] || continue
        [[ "$command" == *"$server_dir/pom.xml"* ]] || continue
        [[ "$command" == *'-Dspring-boot.run.profiles=api'* ]] || continue
        cwd="$(replicadb_process_cwd "$pid")"
        [[ "$cwd" == "$server_dir" || "$cwd" == "${server_dir%/*}" ]] || continue
        if [[ "$command" =~ --server\.port=([0-9]+) ]]; then
            port="${BASH_REMATCH[1]}"
        else
            port="unknown"
        fi
        started="$(replicadb_process_start "$pid" | sed 's/^[[:space:]]*//')"
        printf '%s\t%s\t%s\n' "$pid" "$port" "$started"
    done < <(replicadb_process_list)
}

local_replica_frontend_processes() {
    local frontend_dir="$1"
    local process_line pid command cwd port started

    while IFS= read -r process_line; do
        process_line="${process_line#"${process_line%%[![:space:]]*}"}"
        [[ -n "$process_line" ]] || continue
        pid="${process_line%%[[:space:]]*}"
        command="${process_line#"$pid"}"
        command="${command#"${command%%[![:space:]]*}"}"
        cwd="$(replicadb_process_cwd "$pid")"
        [[ "$cwd" == "$frontend_dir" ]] || continue
        [[ "$command" == npm\ run\ dev* || "$command" == *"$frontend_dir/node_modules/.bin/vite"* ]] || continue
        if [[ "$command" =~ --port[[:space:]]+([0-9]+) ]]; then
            port="${BASH_REMATCH[1]}"
        else
            port="unknown"
        fi
        started="$(replicadb_process_start "$pid" | sed 's/^[[:space:]]*//')"
        printf '%s\t%s\t%s\n' "$pid" "$port" "$started"
    done < <(replicadb_process_list)
}

local_replica_managed_processes() {
    local server_dir="$1"
    local frontend_dir="$2"
    local process_line

    while IFS= read -r process_line; do
        [[ -n "$process_line" ]] || continue
        printf 'api\t%s\n' "$process_line"
    done < <(local_replica_api_processes "$server_dir")

    while IFS= read -r process_line; do
        [[ -n "$process_line" ]] || continue
        printf 'api-launcher\t%s\n' "$process_line"
    done < <(local_replica_api_launchers "$server_dir")

    while IFS= read -r process_line; do
        [[ -n "$process_line" ]] || continue
        printf 'frontend\t%s\n' "$process_line"
    done < <(local_replica_frontend_processes "$frontend_dir")
}

replicadb_stop_managed_processes() {
    local processes="$1"
    local kind pid port started

    while IFS=$'\t' read -r kind pid port started; do
        [[ -n "$pid" ]] || continue
        if kill -0 "$pid" 2>/dev/null; then
            replicadb_stop_process_tree "$pid" || return 1
        fi
    done <<< "$processes"
}

replicadb_handle_existing_managed_processes() {
    local processes="$1"
    local answer
    local kind pid port started

    [[ -n "$processes" ]] || return 0

    printf '%s\n' 'Existing local ReplicaDB managed processes were found:' >&2
    while IFS=$'\t' read -r kind pid port started; do
        [[ -n "$pid" ]] || continue
        printf '  %s PID %s, port %s, started %s\n' "$kind" "$pid" "$port" "$started" >&2
    done <<< "$processes"

    if [[ ! -t 0 || ! -t 1 ]]; then
        printf '%s\n' 'Cannot ask for confirmation without an interactive terminal; stopping startup.' >&2
        return 1
    fi

    read -r -p 'Stop these local ReplicaDB processes and continue? [y/N] ' answer
    case "$answer" in
        y|Y|yes|YES|Yes)
            if ! replicadb_stop_managed_processes "$processes"; then
                printf '%s\n' 'Could not stop every existing local ReplicaDB process.' >&2
                return 1
            fi
            printf '%s\n' 'Existing local ReplicaDB processes stopped.' >&2
            ;;
        *)
            printf '%s\n' 'Startup cancelled; existing local ReplicaDB processes were left running.' >&2
            return 1
            ;;
    esac
}

replicadb_stop_processes() {
    local processes="$1"
    local pid port started
    local remaining

    while IFS=$'\t' read -r pid port started; do
        [[ -n "$pid" ]] || continue
        kill "$pid" 2>/dev/null || return 1
    done <<< "$processes"

    for _ in $(seq 1 10); do
        remaining=0
        while IFS=$'\t' read -r pid port started; do
            [[ -n "$pid" ]] || continue
            if kill -0 "$pid" 2>/dev/null; then
                remaining=1
            fi
        done <<< "$processes"
        [[ "$remaining" == 0 ]] && return 0
        sleep 1
    done

    return 1
}

replicadb_handle_existing_processes() {
    local processes="$1"
    local answer
    local pid port started

    [[ -n "$processes" ]] || return 0

    printf '%s\n' 'Existing local ReplicaDB API instances were found:' >&2
    while IFS=$'\t' read -r pid port started; do
        [[ -n "$pid" ]] || continue
        printf '  PID %s, port %s, started %s\n' "$pid" "$port" "$started" >&2
    done <<< "$processes"

    if [[ ! -t 0 || ! -t 1 ]]; then
        printf '%s\n' 'Cannot ask for confirmation without an interactive terminal; stopping startup.' >&2
        return 1
    fi

    read -r -p 'Stop these local API instances and continue? [y/N] ' answer
    case "$answer" in
        y|Y|yes|YES|Yes)
            if ! replicadb_stop_processes "$processes"; then
                printf '%s\n' 'Could not stop every existing local ReplicaDB API instance.' >&2
                return 1
            fi
            printf '%s\n' 'Existing local ReplicaDB API instances stopped.' >&2
            ;;
        *)
            printf '%s\n' 'Startup cancelled; existing local ReplicaDB API instances were left running.' >&2
            return 1
            ;;
    esac
}
