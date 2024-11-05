#!/bin/sh

SSH_MASTER_SOCKET_DIR=$(pwd)
REPO_USER="${1:-'brc'}"
REPO_STRATUM0='cvmfs0-psu0.galaxyproject.org'
SSH_HOST='cvmfs0-psu0'
SSH_MASTER_SOCKET="${SSH_MASTER_SOCKET_DIR}/ssh-tunnel-${REPO_USER}-${REPO_STRATUM0}.sock"
#SSH_PROXYJUMP='nate@uniport.bx.psu.edu'

if [ ! -S "$SSH_MASTER_SOCKET" ]; then
    set -x
    #ssh -M -S "$SSH_MASTER_SOCKET" -Nfn -o ControlPersist=168h -o "ProxyJump=${SSH_PROXYJUMP}" -l "$REPO_USER" "$REPO_STRATUM0"
    ssh -M -S "$SSH_MASTER_SOCKET" -Nfn -o ControlPersist=168h -l "$REPO_USER" "$SSH_HOST"
    { set +x; } 2>/dev/null
else
    set -x
    ssh -S "$SSH_MASTER_SOCKET" -O exit -l "$REPO_USER" "$SSH_HOST"
    { set +x; } 2>/dev/null
    rm -f "$SSH_MASTER_SOCKET"
fi
