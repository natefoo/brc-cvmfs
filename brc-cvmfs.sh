#!/usr/bin/env bash
set -euo pipefail

# Set this variable to 'true' to publish on successful installation
: ${PUBLISH:=false}
: ${DEVMODE:=false}
: ${DEBUG:=$DEVMODE}
: ${SHARED_ROOT:='/jetstream2/scratch/idc/brc'}


# Set by Jenkins if we're running in Jenkins
#: ${WORKSPACE:="$(mktemp -d -t brc.work.XXXXXX)"}
: ${WORKSPACE:="$(pwd)"}
: ${BUILD_NUMBER:='work'}

# Anything that will be used by GNU parallel (used for indexer DMs and importers) must be exported

WORKDIR="${WORKSPACE}/${BUILD_NUMBER}"
# for cvmfs-fuse.conf
export WORKDIR

REMOTE_WORKDIR="/tmp/brc"

# for import file streams
export TMPDIR="${WORKDIR}/tmp"
rm -rf "${TMPDIR}"
mkdir -p "${TMPDIR}"

HGDOWNLOAD='hgdownload2.soe.ucsc.edu'
ASSEMBLY_LIST_URL="https://${HGDOWNLOAD}/hubs/BRC/assemblyList.json"

SKIP_LIST_FILE='skip_list.txt'
declare -a SKIP_LIST

# could use the dict keys but this stays sorted
INDEXER_LIST=(
    bowtie1
    bowtie2
    bwa-mem
    bwa-mem2
    star
    hisat2
)
# Need annotations
#    snpeff

declare -rA INDEXER_LOC_LIST=(
    ['bowtie1']='bowtie_indices.loc'
    ['bowtie2']='bowtie2_indices.loc'
    ['bwa-mem']='bwa_mem_index.loc'
    ['bwa-mem2']='bwa_mem2_index.loc'
    ['star']='rnastar_index2x_versioned.loc'
    ['hisat2']='hisat2_indexes.loc'
)

declare -rA DM_LIST=(
    ['fetch']='devteam/data_manager_fetch_genome_dbkeys_all_fasta/data_manager_fetch_genome_all_fasta_dbkey'
    ['bowtie1']='iuc/data_manager_bowtie_index_builder/bowtie_index_builder_data_manager'
    ['bowtie2']='devteam/data_manager_bowtie2_index_builder/bowtie2_index_builder_data_manager'
    ['bwa-mem']='devteam/data_manager_bwa_mem_index_builder/bwa_mem_index_builder_data_manager'
    ['bwa-mem2']='iuc/data_manager_bwa_mem2_index_builder/bwa_mem2_index_builder_data_manager'
    ['star']='iuc/data_manager_star_index_builder/rna_star_index_builder_data_manager'
    ['hisat2']='iuc/data_manager_hisat2_index_builder/hisat2_index_builder_data_manager'
)

declare -A DM_TOOL_IDS

export DM_CONFIGS="${WORKDIR}/dm_configs"

declare -A ASSEMBLY_LIST
declare -A ASSEMBLY_NAMES
MISSING_ASSEMBLY_LIST=()
MISSING_INDEX_LIST=()
# the list is small, just install everything
#DM_INSTALL_LIST=()

export REPO='brc.galaxyproject.org'
export REPO_USER='brc'
export REPO_STRATUM0='cvmfs0-psu0.galaxyproject.org'

GALAXY_CLONE_URL='https://github.com/galaxyproject/galaxy.git'
GALAXY_CLONE_BRANCH='dev'

export GALAXY_URL='http://jetstream2:8080'
GALAXY_ADMIN_API_KEY='c0ffee'
export GALAXY_USER_API_KEY=

EPHEMERIS="git+https://github.com/mvdbeek/ephemeris.git@dm_parameters#egg_name=ephemeris"
GALAXY_MAINTENANCE_SCRIPTS="git+https://github.com/mvdbeek/galaxy-maintenance-scripts.git@avoid_galaxy_app#egg_name=galaxy-maintenance-scripts"

#SSH_MASTER_SOCKET_DIR="${HOME}/.cache/brc"
SSH_MASTER_SOCKET_DIR="$(pwd)"

# Should be set by Jenkins, so the default here is for development
: ${GIT_COMMIT:=$(git rev-parse HEAD)}

# Set to true to perform everything on the Jenkins worker and copy results to the Stratum 0 for publish, instead of
# performing everything directly on the Stratum 0. Requires preinstallation/preconfiguration of CVMFS and for
# fuse-overlayfs to be installed on Jenkins workers.
export USE_LOCAL_OVERLAYFS=false

#
# Ensure that everything is defined for set -u
#

export SSH_MASTER_SOCKET=
SSH_MASTER_SOCKET_CREATED=false
OVERLAYFS_UPPER=
OVERLAYFS_LOWER=
OVERLAYFS_WORK=
export OVERLAYFS_MOUNT=
export EPHEMERIS_BIN=
export GALAXY_MAINTENANCE_SCRIPTS_BIN=
export IMPORT_TMPDIR=

SSH_MASTER_UP=false
CVMFS_TRANSACTION_UP=false
LOCAL_CVMFS_MOUNTED=false
LOCAL_OVERLAYFS_MOUNTED=false
GALAXY_UP=false

NUM=0

while getopts ":1pn:" opt; do
    case "$opt" in
        1)
            NUM=1
            ;;
        n)
            NUM=$OPTARG
            ;;
        p)
            PUBLISH=true
            ;;
        *)
            echo "usage: $0 [-1 (one assembly only, vasili aka -n1)] [-n (num of assemblies to do)] [-p (publish)]"
            exit 1
            ;;
    esac
done


function trap_handler() {
    { set +x; } 2>/dev/null
    # return to original dir
    while popd 2>/dev/null; do :; done || true
    [ -z "${SKIP_LIST:-}" ] || printf "%s\n" "${SKIP_LIST[@]}" > "$SKIP_LIST_FILE"
    $GALAXY_UP && stop_galaxy
    $LOCAL_CVMFS_MOUNTED && unmount_overlay_cvmfs
    # $LOCAL_OVERLAYFS_MOUNTED does not need to be checked here since if it's true, $LOCAL_CVMFS_MOUNTED must be true
    $CVMFS_TRANSACTION_UP && abort_transaction
    #$DEVMODE || clean_workspace
    # definitely don't want to do this in devmode, probably not in regular either
    #[ -n "$WORKSPACE" ] && log_exec rm -rf "$WORKSPACE"
    #$SSH_MASTER_UP && [ -n "$REMOTE_WORKDIR" ] && exec_on rm -rf "$REMOTE_WORKDIR"
    $SSH_MASTER_UP && stop_ssh_control
    return 0
}
trap "trap_handler" SIGTERM SIGINT ERR EXIT


function log() {
    [ -t 0 ] && echo -e '\033[1;32m#' "$@" '\033[0m' || echo '#' "$@"
}
export -f log


function log_error() {
    [ -t 0 ] && echo -e '\033[0;31mERROR:' "$@" '\033[0m' || echo 'ERROR:' "$@"
}


function log_debug() {
    if $DEBUG; then echo "####" "$@"; fi
}
export -f log_debug


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
export -f log_exec


function log_exit_error() {
    log_error "$@"
    exit 1
}
export -f log_exit_error


function log_exit() {
    log "$@"
    exit 0
}


function exec_on() {
    if $USE_LOCAL_OVERLAYFS && ! $SSH_MASTER_UP; then
        log_exec "$@"
    else
        log_exec ssh -S "$SSH_MASTER_SOCKET" -l "$REPO_USER" "$REPO_STRATUM0" -- "$@"
    fi
}
export -f exec_on


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
        ASSEMBLY_NAMES[${a[0]}]="${a[2]} (${a[1]})"
    done < <(jq -cr '.data[] | [.asmId, (.refSeq // .genBank), .comName]' "$assembly_list_file")
    log "Total ${#ASSEMBLY_LIST[@]} assemblies at UCSC"
}


function detect_changes() {
    local asm_id dbkey skip do_skip loc
    readarray -t SKIP_LIST < "$SKIP_LIST_FILE"
    declare -p SKIP_LIST
    for asm_id in "${!ASSEMBLY_LIST[@]}"; do
        dbkey="${ASSEMBLY_LIST[$asm_id]}"
        do_skip=false
        for skip in "${SKIP_LIST[@]}"; do
            if [ "$asm_id" == "$skip" ]; then
                log "Skipping: ${asm_id} (dbkey: ${dbkey})"
                do_skip=true
                break
            fi
        done
        $do_skip && continue
        #log_debug "Checking assembly: ${assembly}"
        loc="${LOCAL_CVMFS_MOUNT}/config/all_fasta.loc"
        if [ ! -f "$loc" ] || ! grep -Eq "^${dbkey}\s+" "$loc"; then
            log "Missing assembly: ${asm_id} (dbkey: ${dbkey})"
            MISSING_ASSEMBLY_LIST+=("$asm_id")
            #DM_INSTALL_LIST+=("fetch")
        else
            for indexer in "${INDEXER_LIST[@]}"; do
                loc="${LOCAL_CVMFS_MOUNT}/config/${INDEXER_LOC_LIST[$indexer]}"
                if [ ! -f "$loc" ] || ! grep -Eq "^${dbkey}\s+" "$loc"; then
                    log "Missing index: ${asm_id}/${indexer} (dbkey: ${dbkey})"
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
        LOCAL_CVMFS_MOUNT="$OVERLAYFS_LOWER"
        CVMFS_CACHE="${WORKDIR}/cvmfs-cache"
        IMPORT_TMPDIR="${TMPDIR}"
    else
        OVERLAYFS_UPPER="/var/spool/cvmfs/${REPO}/scratch/current"
        OVERLAYFS_LOWER="/var/spool/cvmfs/${REPO}/rdonly"
        OVERLAYFS_MOUNT="/cvmfs/${REPO}"
        LOCAL_CVMFS_MOUNT="${WORKDIR}/lower"
        CVMFS_CACHE="${WORKDIR}/cvmfs-cache"
        IMPORT_TMPDIR="${OVERLAYFS_MOUNT}/data/_tmp"
    fi
}


function setup_ephemeris() {
    # Sets global $EPHEMERIS_BIN
    EPHEMERIS_BIN="${WORKDIR}/ephemeris/bin"
    if [ ! -d "$EPHEMERIS_BIN" ]; then
        log "Setting up Ephemeris"
        log_exec python3 -m venv "$(dirname "$EPHEMERIS_BIN")"
        log_exec "${EPHEMERIS_BIN}/pip" install --upgrade pip wheel
        log_exec "${EPHEMERIS_BIN}/pip" install --index-url https://wheels.galaxyproject.org/simple/ \
            --extra-index-url https://pypi.org/simple/ "${EPHEMERIS:=ephemeris}"
    fi
}


function setup_galaxy_maintenance_scripts() {
    # Sets global $GALAXY_MAINTENANCE_SCRIPTS
    if $USE_LOCAL_OVERLAYFS; then
        GALAXY_MAINTENANCE_SCRIPTS_BIN="${WORKDIR}/galaxy-maintenance-scripts/bin"
    else
        GALAXY_MAINTENANCE_SCRIPTS_BIN="${REMOTE_WORKDIR}/galaxy-maintenance-scripts/bin"
    fi
    if ! exec_on test -d "$GALAXY_MAINTENANCE_SCRIPTS_BIN"; then
        log "Setting up Galaxy Maintenance Scripts"
        exec_on python3 -m venv "$(dirname "$GALAXY_MAINTENANCE_SCRIPTS_BIN")"
        exec_on "${GALAXY_MAINTENANCE_SCRIPTS_BIN}/pip" install --upgrade pip wheel
        exec_on "${GALAXY_MAINTENANCE_SCRIPTS_BIN}/pip" install --index-url https://wheels.galaxyproject.org/simple/ \
            --extra-index-url https://pypi.org/simple/ "$GALAXY_MAINTENANCE_SCRIPTS"
    fi
}


function verify_cvmfs_revision() {
    log "Verifying that CVMFS Client and Stratum 0 are in sync"
    local cvmfs_io_sock="${WORKDIR}/cvmfs-cache/${REPO}/cvmfs_io.${REPO}"
    local stratum0_published_url="http://${REPO_STRATUM0}/cvmfs/${REPO}/.cvmfspublished"
    local client_rev=-1
    local stratum0_rev=0
    while [ "$client_rev" -ne "$stratum0_rev" ]; do
        log_exec cvmfs_talk -p "$cvmfs_io_sock" remount sync
        client_rev=$(cvmfs_talk -p "$cvmfs_io_sock" revision)
        stratum0_rev=$(curl -s "$stratum0_published_url" | awk -F '^--$' '{print $1} NF>1{exit}' | grep '^S' | sed 's/^S//')
        if [ -z "$client_rev" ]; then
            log_exit_error "Failed to detect client revision"
        elif [ -z "$stratum0_rev" ]; then
            log_exit_error "Failed to detect Stratum 0 revision"
        elif [ "$client_rev" -ne "$stratum0_rev" ]; then
            log_debug "Client revision '${client_rev}' does not match Stratum 0 revision '${stratum0_rev}'"
            sleep 20
        else
            log "${REPO} is revision ${client_rev}"
            break
        fi
    done
}


function mount_overlay_cvmfs() {
    mount_cvmfs
    mount_overlay
}


function unmount_overlay_cvmfs() {
    unmount_overlay
    unmount_cvmfs
}


function mount_cvmfs() {
    log "Mounting CVMFS"
    log_exec rm -rf "$LOCAL_CVMFS_MOUNT" "$CVMFS_CACHE"
    log_exec mkdir -p "$LOCAL_CVMFS_MOUNT" "$CVMFS_CACHE"
    #log_exec cvmfs2 -o config=cvmfs-fuse.conf,allow_root "$REPO" "$LOCAL_CVMFS_MOUNT"
    log_exec cvmfs2 -o config=cvmfs-fuse.conf "$REPO" "$LOCAL_CVMFS_MOUNT"
    LOCAL_CVMFS_MOUNTED=true
    verify_cvmfs_revision
}


function mount_overlay() {
    if $USE_LOCAL_OVERLAYFS; then
        log "Mounting OverlayFS"
        log_exec rm -rf "$OVERLAYFS_UPPER" "$OVERLAYFS_WORK" "$OVERLAYFS_MOUNT"
        log_exec mkdir -p "$OVERLAYFS_UPPER" "$OVERLAYFS_WORK" "$OVERLAYFS_MOUNT"
        #log_exec fuse-overlayfs \
        #    -o "lowerdir=${OVERLAYFS_LOWER},upperdir=${OVERLAYFS_UPPER},workdir=${OVERLAYFS_WORK},allow_root" \
        #    "$OVERLAYFS_MOUNT"
        log_exec fuse-overlayfs \
            -o "lowerdir=${OVERLAYFS_LOWER},upperdir=${OVERLAYFS_UPPER},workdir=${OVERLAYFS_WORK}" \
            "$OVERLAYFS_MOUNT"
        LOCAL_OVERLAYFS_MOUNTED=true
    fi
}


function clean_overlay() {
    # now that Galaxy uses this, CVMFS doesn't unmount cleanly, so we will try the sledgehammer instead
    #log "Remounting OverlayFS/CVMFS for cleaning"
    #unmount_overlay
    stop_galaxy
    unmount_overlay
    unmount_cvmfs
    log "Remounting CVMFS for clean"
    mount_cvmfs
    log "Remounting OverlayFS for clean"
    mount_overlay
    verify_cvmfs_revision
    run_galaxy
}


function unmount_overlay() {
    if $LOCAL_OVERLAYFS_MOUNTED; then
        log "Unmounting OverlayFS"
        log_exec fusermount -u "$OVERLAYFS_MOUNT"
        LOCAL_OVERLAYFS_MOUNTED=false
    fi
}


function unmount_cvmfs() {
    if $LOCAL_CVMFS_MOUNTED; then
        log "Unmounting CVMFS"
        log_exec fusermount -u "$LOCAL_CVMFS_MOUNT"
        LOCAL_CVMFS_MOUNTED=false
    fi
}


function start_ssh_control() {
    log "Starting SSH control connection to Stratum 0"
    SSH_MASTER_SOCKET="${SSH_MASTER_SOCKET_DIR}/ssh-tunnel-${REPO_USER}-${REPO_STRATUM0}.sock"
    #log_exec mkdir -p "$SSH_MASTER_SOCKET_DIR"
    if [ -S "$SSH_MASTER_SOCKET" ]; then
        log "Testing existing SSH control connection at: $SSH_MASTER_SOCKET"
        log_exec ssh -S "$SSH_MASTER_SOCKET" -l "$REPO_USER" "$REPO_STRATUM0" -- /bin/true
    else
        log_exec ssh -M -S "$SSH_MASTER_SOCKET" -Nfn -l "$REPO_USER" "$REPO_STRATUM0"
    fi
    SSH_MASTER_UP=true
}


function stop_ssh_control() {
    log "Stopping SSH control connection to Stratum 0"
    if $SSH_MASTER_SOCKET_CREATED; then
        log_exec ssh -S "$SSH_MASTER_SOCKET" -O exit -l "$REPO_USER" "$REPO_STRATUM0"
        rm -f "$SSH_MASTER_SOCKET"
    fi
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
    local tag="${1:-}"
    local message="${2:-}"
    log "Aborting transaction on $REPO"
    if [ -n "$message" ]; then
        log "Publish would have been tag: ${tag}, message: ${message}"
    fi
    exec_on cvmfs_server abort -f "$REPO"
    CVMFS_TRANSACTION_UP=false
}


function publish_transaction() {
    local tag="$1"
    local message="$2"
    log "Publishing transaction on $REPO"
    exec_on "cvmfs_server publish -a '${tag}' -m '${message}' ${REPO}"
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
    if [ -d "$SHARED_ROOT" ] && ! $DEVMODE; then
        log_exec rm -rf "$SHARED_ROOT"
    fi
    log_exec mkdir -p "$SHARED_ROOT" "${SHARED_ROOT}/tool-data/config"
    if [ ! -d "$galaxy" ]; then
        log_exec git clone -b "$GALAXY_CLONE_BRANCH" --depth=1 "$GALAXY_CLONE_URL" "$galaxy"
    fi
    if ! $DEVMODE; then
        log_exec rm -f "${galaxy}/config/shed_tool_conf.xml" \
                       "${galaxy}/database/universe.sqlite" \
                       "${galaxy}/database/control.sqlite"
    fi
    if [ ! -f "${galaxy}/config/shed_tool_conf.xml" ]; then
        log_exec sed -e "s#SHARED_ROOT#${SHARED_ROOT}#g" shed_tool_conf.xml > "${galaxy}/config/shed_tool_conf.xml"
    fi
    log_exec sed -e "s#SHARED_ROOT#${SHARED_ROOT}#g" galaxy.yml > "${galaxy}/config/galaxy.yml"
    log_exec sed -e "s#SHARED_ROOT#${SHARED_ROOT}#g" tpv.yml > "${galaxy}/config/tpv.yml"
    log_exec sed -e "s#SHARED_ROOT#${SHARED_ROOT}#g" build_tool_data_table_conf.xml > "${galaxy}/config/tool_data_table_conf.xml"
    if [ ! -d "${galaxy}/.venv" ]; then
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
    #log_exec bwrap --bind / / \
    #    --dev-bind /dev /dev \
    #    --ro-bind "$OVERLAYFS_MOUNT" "/cvmfs/${REPO}" -- \
    #        ./.venv/bin/galaxyctl start
    log_exec ./.venv/bin/galaxyctl start
    deactivate
    GALAXY_UP=true
    popd
    #wait_for_cvmfs_sync
    wait_for_galaxy
}


function create_brc_user() {
    # Sets global $GALAXY_USER_API_KEY
    GALAXY_USER_API_KEY="$(${EPHEMERIS_BIN}/python3 ./create-galaxy-user.py -g "$GALAXY_URL")"
}


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
    local dm_install_list dm dm_repo a dm_version_url dm_version dm_tool
    log "Generating Data Manager tool list"
    #log_exec _idc-data-managers-to-tools
    #IFS=$'\n'; dm_install_list=($(sort <<<"${DM_INSTALL_LIST[*]}" | uniq)); unset IFS
    log "Installing Data Managers"
    . "${EPHEMERIS_BIN}/activate"
    #for dm in "${dm_install_list[@]}"; do
    for dm in "${!DM_LIST[@]}"; do
        dm_repo="${DM_LIST[$dm]}"
        readarray -td/ a < <(echo -n "$dm_repo")
        log_exec shed-tools install -g "$GALAXY_URL" -a "$GALAXY_ADMIN_API_KEY" \
            --skip_install_resolver_dependencies \
            --skip_install_repository_dependencies \
            --owner "${a[0]}" \
            --name "${a[1]}"
        dm_version_url="${GALAXY_URL}/api/tools?key=${GALAXY_ADMIN_API_KEY}&tool_id=${a[2]}"
        # this breaks on e.g. +galaxy versions, hopefully they are already sorted (and there should be only one anyway)
        #dm_version=$(curl "$dm_version_url" | jq -r 'sort_by(split(".") | map(tonumber))[-1]')
        dm_version=$(curl "$dm_version_url" | jq -r '.[-1]')
        dm_tool="toolshed.g2.bx.psu.edu/repos/${dm_repo}/${dm_version}"
        log "DM tool for '${dm}' is: ${dm_tool}"
        DM_TOOL_IDS[$dm]="$dm_tool"
    done
    deactivate
}


function generate_fetch_data_manager_configs() {
    local asm_id
    log "Generating fetch Data Manager configs"
    mkdir -p "$DM_CONFIGS"
    for asm_id in "${MISSING_ASSEMBLY_LIST[@]}"; do
        generate_dm_config 'fetch' "$asm_id"
    done
}


function generate_indexer_data_manager_configs() {
    local asm_id="$1"
    local indexer
    log "Generating indexer Data Manager configs for: ${asm_id}"
    for indexer in "${INDEXER_LIST[@]}"; do
        generate_dm_config "$indexer" "$asm_id"
    done
}


function generate_dm_config() {
    local dm="$1"
    local asm_id="$2"
    local dbkey=${ASSEMBLY_LIST[$asm_id]}
    local tool_id=${DM_TOOL_IDS[$dm]}
    local fname="${DM_CONFIGS}/${dm}-${asm_id}.yaml"
    log "Writing ${fname}"
    case "$dm" in
        fetch)
            local name=${ASSEMBLY_NAMES[$asm_id]}
            local url="$(ucsc_url "$asm_id" "$dbkey")"
            cat >"${fname}" <<EOF
data_managers:
  - id: $tool_id
    params:
      - 'dbkey_source|dbkey_source_selector': 'new'
      - 'dbkey_source|dbkey': '$dbkey'
      - 'dbkey_source|dbkey_name': '$name'
      - 'sequence_name': '$name'
      - 'reference_source|reference_source_selector': 'url'
      - 'reference_source|user_url': '$url'
EOF
            ;;
        star)
            #local genome="${OVERLAYFS_UPPER}/data/${dbkey}/seq/${dbkey}.fa"
            # FIXME: this would not work if we were creating indexes on already-published refs
            local genome="${SHARED_ROOT}/tool-data/${dbkey}/seq/${dbkey}.fa"
            local nbases="$(grep -v '^>' "$genome" | tr -d '\n' | wc -c)"
            local nseqs="$(grep -c '>' "$genome")"
            local saindex_nbases=$(python -c "import math; print(min(14, int(math.log2(${nbases})/2 - 1)))")
            local chr_bin_nbits=$(python -c "import math; print(min(18, int(math.log2(${nbases}/${nseqs}))))")
            cat >"${fname}" <<EOF
data_managers:
  - id: $tool_id
    params:
      - 'all_fasta_source': '$dbkey'
      - 'advanced_options|advanced_options_selector': 'advanced'
      - 'advanced_options|genomeSAindexNbases': '$saindex_nbases'
      - 'advanced_options|genomeChrBinNbits': '$chr_bin_nbits'
EOF
            ;;
        *)
            cat >"${fname}" <<EOF
data_managers:
  - id: $tool_id
    params:
      - 'all_fasta_source': '$dbkey'
EOF
            ;;
    esac
}


function ucsc_url() {
    local asm_id="$1"
    local dbkey="$2"
    local ext="${3:-fa.gz}"
    case "$asm_id" in
        GC?_*)
            echo "https://${HGDOWNLOAD}/hubs/${dbkey:0:3}/${dbkey:4:3}/${dbkey:7:3}/${dbkey:10:3}/${dbkey}/${dbkey}.${ext}"
            ;;
        *)
            echo "https://${HGDOWNLOAD}/goldenPath/${asm_id}/bigZips/${asm_id}.${ext}"
            ;;
    esac
}


function run_data_manager() {
    local dm="$1"
    local asm_id="$2"
    local run_id="${dm}-${asm_id}"
    local dm_config="${DM_CONFIGS}/${run_id}.yaml"
    log "Running Data Manager for: $run_id"
    log_exec "${EPHEMERIS_BIN}/run-data-managers" -g "$GALAXY_URL" -a "$GALAXY_USER_API_KEY" \
        --config "$dm_config" \
        --data-manager-mode bundle \
        --history-name "brc-${run_id}"
}
export -f run_data_manager


function update_tool_data_table_conf() {
    # update tool_data_table_conf.xml from repo
    log "Checking for tool_data_table_conf.xml changes"
    if ! diff -u tool_data_table_conf.xml "${OVERLAYFS_MOUNT}/config/tool_data_table_conf.xml"; then
        log "Updating tool_data_table_conf.xml"
        mkdir -p "${OVERLAYFS_MOUNT}/config"
        log_exec cp tool_data_table_conf.xml "${OVERLAYFS_MOUNT}/config/tool_data_table_conf.xml"
    fi
}


function import_tool_data_bundle() {
    local dm="$1"
    local asm_id="$2"
    local run_id="${dm}-${asm_id}"
    local bundle_uri dataset_id
    log "Importing bundle for: $run_id"
    dataset_id="$(log_exec ${EPHEMERIS_BIN}/python3 get-bundle-url.py --galaxy-url "$GALAXY_URL" --history-name "brc-${run_id}" --galaxy-api-key="$GALAXY_USER_API_KEY")"
    [ -n "$dataset_id" ] || log_exit_error "Could not determine bundle URI!"
    bundle_uri="${GALAXY_URL}/api/datasets/${dataset_id}/display?to_ext=data_manager_json"
    log_debug "bundle URI is: $bundle_uri"
    exec_on mkdir -p "${OVERLAYFS_MOUNT}/data"
    if $USE_LOCAL_OVERLAYFS; then
        log_exec bwrap --bind / / \
            --dev-bind /dev /dev \
            --bind "$OVERLAYFS_MOUNT" "/cvmfs/${REPO}" -- \
                ${GALAXY_MAINTENANCE_SCRIPTS_BIN}/galaxy-import-data-bundle \
                    --tool-data-path "/cvmfs/${REPO}/data" \
                    --data-table-config-path "/cvmfs/${REPO}/config/tool_data_table_conf.xml" \
                    "$bundle_uri"
    else
        exec_on TMPDIR=$IMPORT_TMPDIR ${GALAXY_MAINTENANCE_SCRIPTS_BIN}/galaxy-import-data-bundle \
            --tool-data-path "/cvmfs/${REPO}/data" \
            --data-table-config-path "/cvmfs/${REPO}/config/tool_data_table_conf.xml" \
            "$bundle_uri"
    fi
    if [ "$dm" == 'fetch' ]; then
        post_import_fetch_dm "$asm_id" "$dataset_id"
    fi
}
export -f import_tool_data_bundle


function post_import_fetch_dm() {
    # doing some really horrible stuff here -funroll-loops
    local asm_id="$1"
    local dataset_id="$2"
    local dbkey=${ASSEMBLY_LIST[$asm_id]}
    local file_name="$(curl -s "${GALAXY_URL}/api/datasets/${dataset_id}?key=${GALAXY_USER_API_KEY}" | jq -r '.file_name')"
    log_debug "Data manager bundle file is: ${file_name}"
    log_exec ls -lh "${file_name/.dat/_files}"
    log "Linking sequence and len for indexers"
    log_exec rm -rf "${SHARED_ROOT}/tool-data"
    log_exec mkdir -p "${SHARED_ROOT}/tool-data/config" \
        "${SHARED_ROOT}/tool-data/${dbkey}/seq" \
        "${SHARED_ROOT}/tool-data/${dbkey}/len"
    log_exec test -f "${file_name/.dat/_files}/${dbkey}.fa"
    log_exec ln "${file_name/.dat/_files}/${dbkey}.fa" "${SHARED_ROOT}/tool-data/${dbkey}/seq/${dbkey}.fa"
    log_exec test -f "${file_name/.dat/_files}/${dbkey}.len"
    log_exec ln "${file_name/.dat/_files}/${dbkey}.len" "${SHARED_ROOT}/tool-data/${dbkey}/len/${dbkey}.len"
    log "Updating loc files for indexers"
    exec_on tail -1 "${OVERLAYFS_MOUNT}/config/all_fasta.loc" | sed "s#/cvmfs/${REPO}/data#${SHARED_ROOT}/tool-data#" > "${SHARED_ROOT}/tool-data/config/all_fasta.loc"
    exec_on tail -1 "${OVERLAYFS_MOUNT}/config/dbkeys.loc" | sed "s#/cvmfs/${REPO}/data#${SHARED_ROOT}/tool-data#" > "${SHARED_ROOT}/tool-data/config/dbkeys.loc"
    log_exec cat "${SHARED_ROOT}"/tool-data/config/*.loc
    # there is no "download twobit" DM and there is little harm in doing it this way
    local name=${ASSEMBLY_NAMES[$asm_id]}
    local path="/cvmfs/${REPO}/data/${dbkey}/seq/${dbkey}.2bit"
    log "Fetching UCSC 2bit to ${path}"
    exec_on curl -o "${OVERLAYFS_MOUNT}/data/${dbkey}/seq/${dbkey}.2bit" "$(ucsc_url "$asm_id" "$dbkey" '2bit')"
    printf '%s\t%s\n' "$dbkey" "$path" | exec_on tee -a "${OVERLAYFS_MOUNT}/config/twobit.loc"
    printf '%s\t%s\t%s\n' "$dbkey" "$name" "$path" | exec_on tee -a "${OVERLAYFS_MOUNT}/config/lastz_seqs.loc"
    exec_on ls -lh "${OVERLAYFS_MOUNT}/data/${dbkey}/seq/"
}


function run_dm_and_import() {
    local indexer="$1"
    local asm_id="$2"
    run_data_manager "$indexer" "$asm_id"
    import_tool_data_bundle "$indexer" "$asm_id"
}
export -f run_dm_and_import


function reload_data_tables() {
    for data_table in "$@"; do
        log "Reloading ${data_table}"
        log_exec curl "${GALAXY_URL}/api/tool_data/${data_table}/reload?key=${GALAXY_ADMIN_API_KEY}"
        echo ''
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


function check_for_repo_changes() {
    local lower=
    local changes=false
    declare -a configs
    log "Checking for changes to repo"
    exec_on rm -rf "$IMPORT_TMPDIR"
    log "Contents of OverlayFS upper mount (will be published)"
    exec_on tree "$OVERLAYFS_UPPER"
    mapfile -t configs < <(exec_on compgen -G "'${OVERLAYFS_UPPER}/config/*'")
    for config in ${configs[@]}; do
        log "Checking diff: $config"
        lower="${OVERLAYFS_LOWER}/config/${config##*/}"
        exec_on test -f "$lower" || lower=/dev/null
        # not ideal that we consider a single config change as successful but
        exec_on diff -u "$lower" "$config" || { changes=true; }
    done
    if ! $changes; then
        log_exit_error "Terminating build: expected changes to ${OVERLAYFS_UPPER}/config/* not found!"
    fi
}


function clean_workspace() {
    log_exec rm -rf "${WORKSPACE}/${BUILD_NUMBER}"
}


function post_import() {
    #log "Verifying loc file line counts"
    #if [ $(wc -l "${OVERLAYFS_UPPER}/config/"*.loc | awk '$2 != "total" {print $1}' | sort -n | uniq | wc -l) -ne 1 ]; then
    #    log_exit_error "Terminating build: loc files have differing line counts"
    #fi
    local loc
    local lastloc='_init_'
    declare -a locs
    mapfile -t locs < <(exec_on compgen -G "'${OVERLAYFS_UPPER}/config/*.loc'")
    log "Verifying all locs have matching first column"
    for loc in ${locs[@]}; do
        log_debug "$loc"
        # FIXME: not sure this quoting works when running locally
        if [ "$lastloc" != '_init_' ] && ! diff -u <(exec_on awk "-F$'\t'" "'{print \$1}'" "$lastloc") <(exec_on awk "-F$'\t'" "'{print \$1}'" "$loc"); then
            log_exit_error "loc mismatch: ${loc} != ${lastloc}"
        fi
        lastloc="$loc"
    done
    log "Running post-import tasks"
    exec_on find "$OVERLAYFS_UPPER" -perm -u+r -not -perm -o+r -not -type l -print0 | exec_on xargs -0 --no-run-if-empty chmod go+r
    exec_on find "$OVERLAYFS_UPPER" -perm -u+rx -not -perm -o+rx -not -type l -print0 | exec_on xargs -0 --no-run-if-empty chmod go+rx
}


function copy_upper_to_stratum0() {
    log "Copying changes to Stratum 0"
    set -x
    rsync -ah -e "ssh -o ControlPath=${SSH_MASTER_SOCKET}" --exclude='.wh.*' --stats "${OVERLAYFS_UPPER}/" "${REPO_USER}@${REPO_STRATUM0}:/cvmfs/${REPO}"
    { rc=$?; set +x; } 2>/dev/null
    return $rc
}


function main() {
    local message
    set_repo_vars
    fetch_assembly_list
    mount_overlay_cvmfs
    detect_changes
    if [ "${#MISSING_ASSEMBLY_LIST[@]}" -gt 0 ] || [ "${#MISSING_INDEX_LIST[@]}" -gt 0 ]; then
        log "Total ${#MISSING_ASSEMBLY_LIST[@]} assemblies missing"
        setup_ephemeris
        setup_galaxy
        run_galaxy
        create_brc_user
        install_data_managers
        generate_fetch_data_manager_configs
        start_ssh_control
        setup_galaxy_maintenance_scripts
        local i=0
        if [ "${#MISSING_ASSEMBLY_LIST[@]}" -gt 0 ]; then
            for asm_id in "${MISSING_ASSEMBLY_LIST[@]}"; do
                if $USE_LOCAL_OVERLAYFS; then
                    [ ! -d "${OVERLAYFS_UPPER}/config" ] || clean_overlay
                    update_tool_data_table_conf
                else
                    begin_transaction 600
                    exec_on mkdir -p "$IMPORT_TMPDIR"
                fi
                if ! run_data_manager 'fetch' "$asm_id"; then
                    log_error "Fetch failed, adding to skip list: ${asm_id}"
                    SKIP_LIST+=("$asm_id")
                    continue
                fi
                import_tool_data_bundle 'fetch' "$asm_id"
                reload_data_tables 'all_fasta' '__dbkeys__'
                generate_indexer_data_manager_configs "$asm_id"
                log "Parallelizing indexer data managers"
                parallel -j ${#INDEXER_LIST[@]} run_dm_and_import {} "$asm_id" ::: "${INDEXER_LIST[@]}"
                #for indexer in "${INDEXER_LIST[@]}"; do
                #    run_data_manager "$indexer" "$asm_id"
                #    import_tool_data_bundle "$indexer" "$asm_id"
                #done
                check_for_repo_changes
                post_import
                if $USE_LOCAL_OVERLAYFS; then
                    begin_transaction 600
                    copy_upper_to_stratum0
                fi
                message="Initial sequence and indexes for assembly: ${asm_id}"
                if $PUBLISH; then
                    publish_transaction "brc-initial-${asm_id}" "$message"
                else
                    abort_transaction "brc-initial-${asm_id}" "$message"
                fi
                i=$((i + 1))
                [ $NUM -eq 0 -o $i -lt $NUM ] || { log_exit "Exiting after ${i} assemblies as requested"; }
                [ ! -f exit ] || { rm -f exit; log_exit "Exiting due to presence of exit file"; }
            done
        elif [ "${#MISSING_INDEX_LIST[@]}" -gt 0 ]; then
            echo "NOT IMPLEMENTED"
            exit 1
        fi
        stop_ssh_control
        stop_galaxy
    else
        log "No changes!"
    fi
    unmount_overlay_cvmfs
    #$DEVMODE || clean_workspace
    return 0
}


main
