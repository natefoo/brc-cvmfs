#!/usr/bin/env bash
set -euo pipefail

# Set this variable to 'true' to publish on successful installation
: ${PUBLISH:=false}
: ${DEVMODE:=false}
: ${DEBUG:=$DEVMODE}

# Set by Jenkins if we're running in Jenkins
#: ${WORKSPACE:="$(mktemp -d -t brc.work.XXXXXX)"}
: ${WORKSPACE:="$(pwd)"}
: ${JOB_NAME:='none'}
: ${BUILD_NUMBER:='none'}

WORKDIR="${WORKSPACE}/${BUILD_NUMBER}"
# for cvmfs-fuse.conf
export WORKDIR

ASSEMBLY_LIST_URL='https://hgdownload.soe.ucsc.edu/hubs/BRC/assemblyList.json'

INDEXER_LIST=(
    bowtie1
    bowtie2
    bwa-mem
    bwa-mem2
)
# 2bit???

declare -rA INDEXER_LOC_LIST=(
    ["bowtie1"]="bowtie_indices.loc"
    ["bowtie2"]="bowtie2_indices.loc"
    ["bwa-mem"]="bwa_mem_index.loc"
    ["bwa-mem2"]="bwa_mem2_index.loc"
)

declare -rA INDEXER_DM_LIST=(
    ["fetch"]="devteam/data_manager_fetch_genome_dbkeys_all_fasta"
)

DM_CONFIGS="${WORKDIR}/dm_configs"

declare -A ASSEMBLY_LIST
declare -A ASSEMBLY_NAMES
MISSING_ASSEMBLY_LIST=()
MISSING_INDEX_LIST=()
# the list is small, just install everything
#DM_INSTALL_LIST=()

REPO='sandbox.galaxyproject.org'
REPO_USER='sandbox'
REPO_STRATUM0='cvmfs0-psu0.galaxyproject.org'

GALAXY_CLONE_URL='https://github.com/galaxyproject/galaxy.git'
GALAXY_CLONE_BRANCH='dev'

GALAXY_URL='http://localhost:8080'
GALAXY_ADMIN_API_KEY='c0ffee'
GALAXY_USER_API_KEY=

EPHEMERIS="git+https://github.com/mvdbeek/ephemeris.git@dm_parameters#egg_name=ephemeris"
GALAXY_MAINTENANCE_SCRIPTS="git+https://github.com/mvdbeek/galaxy-maintenance-scripts.git@avoid_galaxy_app#egg_name=galaxy-maintenance-scripts"

SSH_MASTER_SOCKET_DIR="${HOME}/.cache/brc"

# Should be set by Jenkins, so the default here is for development
: ${GIT_COMMIT:=$(git rev-parse HEAD)}

# Set to true to perform everything on the Jenkins worker and copy results to the Stratum 0 for publish, instead of
# performing everything directly on the Stratum 0. Requires preinstallation/preconfiguration of CVMFS and for
# fuse-overlayfs to be installed on Jenkins workers.
USE_LOCAL_OVERLAYFS=true

#
# Ensure that everything is defined for set -u
#

SSH_MASTER_SOCKET=
OVERLAYFS_UPPER=
OVERLAYFS_LOWER=
OVERLAYFS_WORK=
OVERLAYFS_MOUNT=
EPHEMERIS_BIN=
GALAXY_MAINTENANCE_SCRIPTS_BIN=

SSH_MASTER_UP=false
CVMFS_TRANSACTION_UP=false
LOCAL_CVMFS_MOUNTED=false
LOCAL_OVERLAYFS_MOUNTED=false
GALAXY_UP=false


function trap_handler() {
    { set +x; } 2>/dev/null
    # return to original dir
    while popd 2>/dev/null; do :; done || true
    #$IMPORT_CONTAINER_UP && stop_import_container
    #clean_preconfigured_container
    $GALAXY_UP && stop_galaxy
    $LOCAL_CVMFS_MOUNTED && unmount_overlay
    # $LOCAL_OVERLAYFS_MOUNTED does not need to be checked here since if it's true, $LOCAL_CVMFS_MOUNTED must be true
    $CVMFS_TRANSACTION_UP && abort_transaction
    $DEVMODE || clean_workspace
    # definitely don't want to do this in devmode, probably not in regular either
    #[ -n "$WORKSPACE" ] && log_exec rm -rf "$WORKSPACE"
    $SSH_MASTER_UP && [ -n "$REMOTE_WORKDIR" ] && exec_on rm -rf "$REMOTE_WORKDIR"
    $SSH_MASTER_UP && stop_ssh_control
    return 0
}
trap "trap_handler" SIGTERM SIGINT ERR EXIT


function log() {
    [ -t 0 ] && echo -e '\033[1;32m#' "$@" '\033[0m' || echo '#' "$@"
}


function log_error() {
    [ -t 0 ] && echo -e '\033[0;31mERROR:' "$@" '\033[0m' || echo 'ERROR:' "$@"
}


function log_debug() {
    if $DEBUG; then echo "####" "$@"; fi
}


function log_exec() {
    local rc
    if $USE_LOCAL_OVERLAYFS && ! $SSH_MASTER_UP; then
        set -x
        eval "$@"
    else
        set -x
        "$@"
    fi
    { rc=$?; set +x; } 2>/dev/null
    return $rc
}


function log_exit_error() {
    log_error "$@"
    exit 1
}


function log_exit() {
    echo "$@"
    exit 0
}


function exec_on() {
    if $USE_LOCAL_OVERLAYFS && ! $SSH_MASTER_UP; then
        log_exec "$@"
    else
        log_exec ssh -S "$SSH_MASTER_SOCKET" -l "$REPO_USER" "$REPO_STRATUM0" -- "$@"
    fi
}


function copy_to() {
    local file="$1"
    if $USE_LOCAL_OVERLAYFS && ! $SSH_MASTER_UP; then
        log_exec cp "$file" "${WORKDIR}/${file##*}"
    else
        log_exec scp -o "ControlPath=$SSH_MASTER_SOCKET" "$file" "${REPO_USER}@${REPO_STRATUM0}:${REMOTE_WORKDIR}/${file##*/}"
    fi
}


function fetch_assembly_list() {
    local line a
    # prefer refSeq IDs to genBank per Hiram
    local assembly_list_file="$(basename "$ASSEMBLY_LIST_URL")"
    if [ ! -f "$assembly_list_file" ] || ! $DEVMODE; then
        log_exec curl -O "$ASSEMBLY_LIST_URL"
    fi
    #ASSEMBLY_LIST=( $(jq -cr '.data[] | (.refSeq // .genBank)' "$assembly_list_file" | LC_ALL=C sort) )
    while read line; do
        # this forking is slow but otherwise we have to parse json in bash
        IFS=$'\n'; a=($(echo "$line" | jq -r '.[]')); unset IFS
        ASSEMBLY_LIST[${a[0]}]="${a[1]}"
        ASSEMBLY_NAMES[${a[0]}]="${a[2]}"
    done < <(jq -cr '.data[] | [.asmId, (.refSeq // .genBank), .comName]' "$assembly_list_file")
    log "Total ${#ASSEMBLY_LIST[@]} assemblies at UCSC"
}


function detect_changes() {
    local asm_id dbkey
    local loc
    for asm_id in "${!ASSEMBLY_LIST[@]}"; do
        dbkey="${ASSEMBLY_LIST[$asm_id]}"
        #log_debug "Checking assembly: ${assembly}"
        loc="/cvmfs/${REPO}/config/all_fasta.loc"
        if [ ! -f "$loc" ] || ! grep -Eq "^${dbkey}\s+" "$loc"; then
            log "Missing assembly: ${dbkey} (dbkey: $asm_id)"
            MISSING_ASSEMBLY_LIST+=("$asm_id")
            #DM_INSTALL_LIST+=("fetch")
        else
            for indexer in "${INDEXER_LIST[@]}"; do
                loc="/cvmfs/${REPO}/config/${INDEXER_LOC_LIST[$indexer]}"
                if [ ! -f "$loc" ] || grep -Eq "^${dbkey}\s+" "$loc"; then
                    log "Missing index: ${asm_id}/${indexer} (dbkey: $dbkey)"
                    MISSING_INDEX_LIST+=("${asm_id}/${indexer}")
                    #DM_INSTALL_LIST+=("$indexer")
                fi
            done
        fi
    done
}


function set_repo_vars() {
    CONTAINER_NAME="brc-${REPO_USER}-${BUILD_NUMBER}"
    if $USE_LOCAL_OVERLAYFS; then
        OVERLAYFS_LOWER="${WORKDIR}/lower"
        OVERLAYFS_UPPER="${WORKDIR}/upper"
        OVERLAYFS_WORK="${WORKDIR}/work"
        OVERLAYFS_MOUNT="${WORKDIR}/mount"
        CVMFS_CACHE="${WORKDIR}/cvmfs-cache"
    else
        OVERLAYFS_UPPER="/var/spool/cvmfs/${REPO}/scratch/current"
        OVERLAYFS_LOWER="/var/spool/cvmfs/${REPO}/rdonly"
        OVERLAYFS_MOUNT="/cvmfs/${REPO}"
    fi
}


function setup_ephemeris() {
    # Sets global $EPHEMERIS_BIN
    EPHEMERIS_BIN="${WORKDIR}/ephemeris/bin"
    if [ ! -d "$EPHEMERIS_BIN" ] || ! $DEVMODE; then
        log "Setting up Ephemeris"
        log_exec python3 -m venv "$(dirname "$EPHEMERIS_BIN")"
        log_exec "${EPHEMERIS_BIN}/pip" install --upgrade pip wheel
        log_exec "${EPHEMERIS_BIN}/pip" install --index-url https://wheels.galaxyproject.org/simple/ \
            --extra-index-url https://pypi.org/simple/ "${EPHEMERIS:=ephemeris}"
    fi
}


function setup_galaxy_maintenance_scripts() {
    # Sets global $GALAXY_MAINTENANCE_SCRIPTS
    GALAXY_MAINTENANCE_SCRIPTS_BIN="${WORKDIR}/galaxy-maintenance-scripts/bin"
    if [ ! -d "$GALAXY_MAINTENANCE_SCRIPTS_BIN" ] || ! $DEVMODE; then
        log "Setting up Galaxy Maintenance Scripts"
        log_exec python3 -m venv "$(dirname "$GALAXY_MAINTENANCE_SCRIPTS_BIN")"
        log_exec "${GALAXY_MAINTENANCE_SCRIPTS_BIN}/pip" install --upgrade pip wheel
        log_exec "${GALAXY_MAINTENANCE_SCRIPTS_BIN}/pip" install --index-url https://wheels.galaxyproject.org/simple/ \
            --extra-index-url https://pypi.org/simple/ "$GALAXY_MAINTENANCE_SCRIPTS"
    fi
}


function verify_cvmfs_revision() {
    log "Verifying that CVMFS Client and Stratum 0 are in sync"
    local cvmfs_io_sock="${WORKDIR}/cvmfs-cache/${REPO}/cvmfs_io.${REPO}"
    local stratum0_published_url="http://${REPO_STRATUM0}/cvmfs/${REPO}/.cvmfspublished"
    local client_rev=$(cvmfs_talk -p "$cvmfs_io_sock" revision)
    local stratum0_rev=$(curl -s "$stratum0_published_url" | awk -F '^--$' '{print $1} NF>1{exit}' | grep '^S' | sed 's/^S//')
    if [ -z "$client_rev" ]; then
        log_exit_error "Failed to detect client revision"
    elif [ -z "$stratum0_rev" ]; then
        log_exit_error "Failed to detect Stratum 0 revision"
    elif [ "$client_rev" -ne "$stratum0_rev" ]; then
        log_exit_error "Client revision '${client_rev}' does not match Stratum 0 revision '${stratum0_rev}'"
    fi

    log "${REPO} is revision ${client_rev}"
}


function verify_cvmfs_revision_priv() {
    log "Verifying that CVMFS Client and Stratum 0 are in sync"
    pushd "/cvmfs/${REPO}" >/dev/null ; popd >/dev/null
    local stratum0_published_url="http://${REPO_STRATUM0}/cvmfs/${REPO}/.cvmfspublished"
    local client_rev=$(cvmfs_config stat "$REPO" | awk 'FNR==1 {for(i=1;i<=NF;i++) {if($i == "REVISION") {next}}} {print $i}')
    local stratum0_rev=$(curl -s "$stratum0_published_url" | awk -F '^--$' '{print $1} NF>1{exit}' | grep '^S' | sed 's/^S//')

    if [ -z "$client_rev" ]; then
        log_exit_error "Failed to detect client revision"
    elif [ -z "$stratum0_rev" ]; then
        log_exit_error "Failed to detect Stratum 0 revision"
    elif [ "$client_rev" -ne "$stratum0_rev" ]; then
        log_exit_error "Client revision '${client_rev}' does not match Stratum 0 revision '${stratum0_rev}'"
    fi

    log "${REPO} is revision ${client_rev}"
}


function mount_overlay() {
    log "Mounting OverlayFS/CVMFS"
    log_debug "\$JOB_NAME: ${JOB_NAME}, \$WORKSPACE: ${WORKSPACE}, \$BUILD_NUMBER: ${BUILD_NUMBER}"
    log_exec mkdir -p "$OVERLAYFS_LOWER" "$OVERLAYFS_UPPER" "$OVERLAYFS_WORK" "$OVERLAYFS_MOUNT" "$CVMFS_CACHE"
    #log_exec cvmfs2 -o config=cvmfs-fuse.conf,allow_root "$REPO" "$OVERLAYFS_LOWER"
    log_exec cvmfs2 -o config=cvmfs-fuse.conf "$REPO" "$OVERLAYFS_LOWER"
    LOCAL_CVMFS_MOUNTED=true
    verify_cvmfs_revision
    #log_exec fuse-overlayfs \
    #    -o "lowerdir=${OVERLAYFS_LOWER},upperdir=${OVERLAYFS_UPPER},workdir=${OVERLAYFS_WORK},allow_root" \
    #    "$OVERLAYFS_MOUNT"
    log_exec fuse-overlayfs \
        -o "lowerdir=${OVERLAYFS_LOWER},upperdir=${OVERLAYFS_UPPER},workdir=${OVERLAYFS_WORK}" \
        "$OVERLAYFS_MOUNT"
    LOCAL_OVERLAYFS_MOUNTED=true
}


function unmount_overlay() {
    log "Unmounting OverlayFS/CVMFS"
    if $LOCAL_OVERLAYFS_MOUNTED; then
        log_exec fusermount -u "$OVERLAYFS_MOUNT"
        LOCAL_OVERLAYFS_MOUNTED=false
    fi
    # DEBUG: what is holding this?
    log_exec fuser -v "$OVERLAYFS_LOWER" || true
    # Attempt to kill anything still accessing lower so unmount doesn't fail
    log_exec fuser -v -k "$OVERLAYFS_LOWER" || true
    log_exec fusermount -u "$OVERLAYFS_LOWER"
    LOCAL_CVMFS_MOUNTED=false
}


function start_ssh_control() {
    log "Starting SSH control connection to Stratum 0"
    SSH_MASTER_SOCKET="${SSH_MASTER_SOCKET_DIR}/ssh-tunnel-${REPO_USER}-${REPO_STRATUM0}.sock"
    log_exec mkdir -p "$SSH_MASTER_SOCKET_DIR"
    log_exec ssh -M -S "$SSH_MASTER_SOCKET" -Nfn -l "$REPO_USER" "$REPO_STRATUM0"
    SSH_MASTER_UP=true
}


function stop_ssh_control() {
    log "Stopping SSH control connection to Stratum 0"
    log_exec ssh -S "$SSH_MASTER_SOCKET" -O exit -l "$REPO_USER" "$REPO_STRATUM0"
    rm -f "$SSH_MASTER_SOCKET"
    SSH_MASTER_UP=false
}


function begin_transaction() {
    # $1 >= 0 number of seconds to retry opening transaction for
    local max_wait="${1:--1}"
    local start=$(date +%s)
    local elapsed='-1'
    local sleep='4'
    local max_sleep='60'
    log "Opening transaction on $REPO"
    while ! exec_on cvmfs_server transaction "$REPO"; do
        log "Failed to open CVMFS transaction on ${REPO}"
        if [ "$max_wait" -eq -1 ]; then
            log_exit_error 'Transaction open retry disabled, giving up!'
        elif [ "$elapsed" -ge "$max_wait" ]; then
            log_exit_error "Time waited (${elapsed}s) exceeds limit (${max_wait}s), giving up!"
        fi
        log "Will retry in ${sleep}s"
        sleep $sleep
        [ $sleep -ne $max_sleep ] && let sleep="${sleep}*2"
        [ $sleep -gt $max_sleep ] && sleep="$max_sleep"
        let elapsed="$(date +%s)-${start}"
    done
    CVMFS_TRANSACTION_UP=true
}


function abort_transaction() {
    log "Aborting transaction on $REPO"
    exec_on cvmfs_server abort -f "$REPO"
    CVMFS_TRANSACTION_UP=false
}


function publish_transaction() {
    log "Publishing transaction on $REPO"
    exec_on "cvmfs_server publish -a 'idc-${GIT_COMMIT:0:7}.${DM_STAGE}' -m 'Automated data installation for commit ${GIT_COMMIT}' ${REPO}"
    CVMFS_TRANSACTION_UP=false
}


function create_workdir() {
    # Sets global $WORKDIR
    log "Creating local workdir"
    WORKDIR=$(log_exec mktemp -d -t idc.work.XXXXXX)
}


function setup_galaxy() {
    local galaxy="${WORKDIR}/galaxy"
    log "Setting up Galaxy"
    if [ ! -d "$galaxy" ] || ! $DEVMODE; then
        log_exec git clone -b "$GALAXY_CLONE_BRANCH" --depth=1 "$GALAXY_CLONE_URL" "$galaxy"
    fi
    log_exec cp galaxy.yml "${galaxy}/config/galaxy.yml"
    if [ ! -d "${galaxy}/.venv" ] || ! $DEVMODE; then
        pushd "$galaxy"
        log_exec sh ./scripts/common_startup.sh --skip-client-build
        popd
    fi
}


function run_galaxy() {
    local galaxy="${WORKDIR}/galaxy"
    log "Starting Galaxy"
    pushd "$galaxy"
    . ./.venv/bin/activate
    log_exec ./.venv/bin/galaxyctl start
    deactivate
    GALAXY_UP=true
    popd
    #wait_for_cvmfs_sync
    wait_for_galaxy
}


function create_brc_user() {
    # Sets global $GALAXY_USER_API_KEY
    GALAXY_USER_API_KEY="$(${EPHEMERIS_BIN}/python3 ./create-galaxy-user.py)"
}


#function wait_for_cvmfs_sync() {
#    # TODO merge with verify_cvmfs_revision() used by build side
#    # TODO: could avoid the hardcoding by using ansible but the output is harder to process
#    local stratum0_published_url="http://${REPO_STRATUM0}/cvmfs/${REPO}/.cvmfspublished"
#    while true; do
#        # ensure it's mounted
#        ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no -l rocky -i ~/.ssh/id_rsa_idc_jetstream2_cvmfs idc-build ls /cvmfs/${REPO} >/dev/null
#        local client_rev=$(ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no -l rocky -i ~/.ssh/id_rsa_idc_jetstream2_cvmfs idc-build sudo cvmfs_talk -i ${REPO} revision)
#        local stratum0_rev=$(curl -s "$stratum0_published_url" | awk -F '^--$' '{print $1} NF>1{exit}' | grep '^S' | sed 's/^S//')
#        if [ "$client_rev" -eq "$stratum0_rev" ]; then
#            log "${REPO} is revision ${client_rev}"
#            break
#        else
#            log_debug "Builder client revision '${client_rev}' does not match Stratum 0 revision '${stratum0_rev}'"
#            sleep 60
#        fi
#    done
#}


function wait_for_galaxy() {
    log "Waiting for Galaxy"
    log_exec "${EPHEMERIS_BIN}/galaxy-wait" -v -g "$GALAXY_URL" --timeout 180 || {
        log_error "Timed out waiting for Galaxy"
        #exec_on journalctl -u galaxy-gunicorn
        #log_debug "response from ${IMPORT_GALAXY_URL}";
        curl -s "$GALAXY_URL";
        log_exit_error "Terminating build due to previous errors"
    }
}


function stop_galaxy() {
    local galaxy="${WORKDIR}/galaxy"
    log "Stopping Build Galaxy"
    pushd "$galaxy"
    log_exec ./.venv/bin/galaxyctl shutdown
    GALAXY_UP=false
    popd
}


function install_data_managers() {
    local dm_install_list dm a
    log "Generating Data Manager tool list"
    #log_exec _idc-data-managers-to-tools
    #IFS=$'\n'; dm_install_list=($(sort <<<"${DM_INSTALL_LIST[*]}" | uniq)); unset IFS
    log "Installing Data Managers"
    . "${EPHEMERIS_BIN}/activate"
    #for dm in "${dm_install_list[@]}"; do
    for dm_repo in "${INDEXER_DM_LIST[@]}"; do
        #dm_repo="${INDEXER_DM_LIST[$dm]}"
        readarray -td/ a <<<"$dm_repo"
        log_exec shed-tools install -g "$GALAXY_URL" -a "$GALAXY_ADMIN_API_KEY" \
            --skip_install_resolver_dependencies \
            --skip_install_repository_dependencies \
            --owner "${a[0]}" \
            --name "${a[1]}"
    done
    deactivate
}


function generate_data_manager_tasks() {
    local asm_id dbkey name url
    #local assembly_list_file="$(basename "$ASSEMBLY_LIST_URL")"
    log "Generating Data Manager tasks"
    mkdir -p "$DM_CONFIGS"
    for asm_id in "${MISSING_ASSEMBLY_LIST[@]}"; do
        # FIXME: hardcoded DM tool id
        dbkey=${ASSEMBLY_LIST[$asm_id]}
        name=${ASSEMBLY_NAMES[$asm_id]}
        url="$(ucsc_fasta_url "$asm_id" "$dbkey")"
        cat >"${DM_CONFIGS}/fetch-${asm_id}.yaml" <<EOF
data_managers:
  - id: toolshed.g2.bx.psu.edu/repos/devteam/data_manager_fetch_genome_dbkeys_all_fasta/data_manager_fetch_genome_all_fasta_dbkey/0.0.4
    params:
      - 'dbkey_source|dbkey_source_selector': 'new'
      - 'dbkey_source|dbkey': '$dbkey'
      - 'dbkey_source|dbkey_name': 'BRC: $name'
      - 'sequence_name': '$name'
      - 'reference_source|reference_source_selector': 'url'
      - 'reference_source|user_url': '$url'
EOF
    done
}


function ucsc_fasta_url() {
    local asm_id="$1"
    local dbkey="$2"
    case "$asm_id" in
        GC?_*)
            echo "https://hgdownload.soe.ucsc.edu/hubs/${dbkey:0:3}/${dbkey:4:3}/${dbkey:7:3}/${dbkey:10:3}/${dbkey}/${dbkey}.fa.gz"
            ;;
        *)
            echo "https://hgdownload.soe.ucsc.edu/goldenPath/${asm_id}/bigZips/${asm_id}.fa.gz"
            ;;
    esac
}


function run_data_managers() {
    local dm_config run_id
    for dm_config in "${DM_CONFIGS}/"*; do
        run_id=$(basename $dm_config .yaml)
        log "Running Data Manager for: $run_id"
        log_exec "${EPHEMERIS_BIN}/run-data-managers" -g "$GALAXY_URL" -a "$GALAXY_USER_API_KEY" \
            --config "$dm_config" \
            --data-manager-mode bundle \
            --history-name "brc-${run_id}"
        # FIXME: testing
        break
    done
}


function update_tool_data_table_conf() {
    # update tool_data_table_conf.xml from repo
    if diff -q tool_data_table_conf.xml "${OVERLAYFS_MOUNT}/config/tool_data_table_conf.xml"; then
        log "Updating tool_data_table_conf.xml"
        mkdir -p "${OVERLAYFS_MOUNT}/config"
        log_exec cp tool_data_table_conf.xml "${OVERLAYFS_MOUNT}/config/tool_data_table_conf.xml"
    fi
}


function import_tool_data_bundles() {
    local dm_config run_id bundle_uri
    for dm_config in "${DM_CONFIGS}/"*; do
        run_id=$(basename $dm_config .yaml)
        log "Importing bundle for: '$run_id'"
        bundle_uri="$(log_exec ${EPHEMERIS_BIN}/python3 get-bundle-url.py --history-name "brc-${run_id}" --galaxy-api-key="$GALAXY_USER_API_KEY")"
        [ -n "$bundle_uri" ] || log_exit_error "Could not determine bundle URI!"
        log_debug "bundle URI is: $bundle_uri"
        # FIXME: this currently only works if you manually disable the IP allowlist check in
        # galaxy-maintenance-scripts/lib/python3.10/site-packages/galaxy/files/uris.py
        log_exec bwrap --bind / / \
            --dev-bind /dev /dev \
            --bind "$OVERLAYFS_MOUNT" "/cvmfs/${REPO}" -- \
                ${GALAXY_MAINTENANCE_SCRIPTS_BIN}/galaxy-import-data-bundle \
                    --tool-data-path "/cvmfs/${REPO}/data" \
                    --data-table-config-path "/cvmfs/${REPO}/config/tool_data_table_conf.xml" \
                    "$bundle_uri"
        break
    done
}


function show_logs() {
    local lines=
    if [ -n "${1:-}" ]; then
        lines="--tail ${1:-}"
        log_debug "tail ${lines} of server log";
    else
        log_debug "contents of server log";
    fi
    exec_on docker logs $lines "$CONTAINER_NAME"
}


function show_paths() {
    log "contents of OverlayFS upper mount (will be published)"
    exec_on tree "$OVERLAYFS_UPPER"
}


function check_for_repo_changes() {
    local lower=
    local changes=false
    log "Checking for changes to repo"
    show_paths
    for config in $(exec_on "compgen -G '${OVERLAYFS_UPPER}/config/*'"); do
        exec_on test -f "$config" || continue
        lower="${OVERLAYFS_LOWER}/config/${config##*/}"
        exec_on test -f "$lower" || lower=/dev/null
        exec_on diff -q "$lower" "$config" || { changes=true; exec_on diff -u "$lower" "$config" || true; }
    done
    if ! $changes; then
        log_exit_error "Terminating build: expected changes to ${OVERLAYFS_UPPER}/config/* not found!"
    fi
}


function clean_workspace() {
    log_exec rm -rf "${WORKSPACE}/${BUILD_NUMBER}"
}


function post_install() {
    log "Running post-installation tasks"
    exec_on "find '$OVERLAYFS_UPPER' -perm -u+r -not -perm -o+r -not -type l -print0 | xargs -0 --no-run-if-empty chmod go+r"
    exec_on "find '$OVERLAYFS_UPPER' -perm -u+rx -not -perm -o+rx -not -type l -print0 | xargs -0 --no-run-if-empty chmod go+rx"
}


function copy_upper_to_stratum0() {
    log "Copying changes to Stratum 0"
    set -x
    rsync -ah -e "ssh -o ControlPath=${SSH_MASTER_SOCKET}" --stats "${OVERLAYFS_UPPER}/" "${REPO_USER}@${REPO_STRATUM0}:/cvmfs/${REPO}"
    { rc=$?; set +x; } 2>/dev/null
    return $rc
}


function do_import_local() {
    mount_overlay
    # TODO: we could probably replace the import container with whatever cvmfsexec does to fake a mount
    if generate_import_tasks; then
        create_workdir
        prep_for_galaxy_run
        update_tool_data_table_conf
        run_import_container
        import_tool_data_bundles
        check_for_repo_changes
        stop_import_container
        clean_preconfigured_container
        post_install
    else
        log "Nothing to import"
        PUBLISH=false
    fi
    if $PUBLISH; then
        start_ssh_control
        begin_transaction 600
        copy_upper_to_stratum0
        publish_transaction
        stop_ssh_control
    fi
    unmount_overlay
}


function do_import_remote() {
    start_ssh_control
    create_remote_workdir
    setup_remote_ephemeris
    # from this point forward $EPHEMERIS_BIN refers to remote
    if generate_import_tasks; then
        setup_galaxy_maintenance_scripts "$WORKDIR" "$REMOTE_PYTHON"
        begin_transaction
        update_tool_data_table_conf
        import_tool_data_bundles
        check_for_repo_changes
        post_install
    else
        log "Nothing to import"
        PUBLISH=false
    fi
    $PUBLISH && publish_transaction || abort_transaction
    stop_ssh_control
}


function main() {
    set_repo_vars
    #verify_cvmfs_revision
    fetch_assembly_list
    mount_overlay
    detect_changes
    generate_data_manager_tasks
    if [ "${#MISSING_ASSEMBLY_LIST[@]}" -gt 0 ]; then
        setup_ephemeris
        setup_galaxy
        run_galaxy
        create_brc_user
        install_data_managers
        run_data_managers
        update_tool_data_table_conf
        setup_galaxy_maintenance_scripts
        import_tool_data_bundles
    elif [ "${#MISSING_ASSEMBLY_LIST[@]}" -gt 0 ]; then
        echo "NOT IMPLEMENTED"
        exit 1
    fi
    #if generate_data_manager_tasks; then
    #    run_build_galaxy
    #    wait_for_build_galaxy
    #    #install_data_managers
    #    run_data_managers
    #else
    #    log "Nothing to build, will check for unimported data"
    #fi
    #if $USE_LOCAL_OVERLAYFS; then
    #    do_import_local
    #else
    #    do_import_remote
    #fi
    #stop_build_galaxy
    #clean_workspace
    return 0
}


main
