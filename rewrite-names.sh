#!/bin/bash
# Find the name column (if it exists) in every data table and rewrite it
set -euo pipefail

HGDOWNLOAD='hgdownload2.soe.ucsc.edu'
ASSEMBLY_LIST_URL="https://${HGDOWNLOAD}/hubs/BRC/assemblyList.json"
TOOL_DATA_TABLE_CONF="/cvmfs/brc.galaxyproject.org/config/tool_data_table_conf.xml"

declare -A ASSEMBLY_LIST
declare -A NAME_COLUMNS
declare -A DBKEY_COLUMNS


function fetch_assembly_list() {
    local line a
    local assembly_list_file="$(basename "$ASSEMBLY_LIST_URL")"
    if [ ! -f "$assembly_list_file" ]; then
        curl -O "$ASSEMBLY_LIST_URL"
    fi
    while read line; do
        # this forking is slow but otherwise we have to parse json in bash
        IFS=$'\n'; a=($(echo "$line" | jq -r '.[]')); unset IFS
	    ASSEMBLY_LIST[${a[0]}]="${a[2]} (${a[1]})"
    done < <(jq -cr '.data[] | [(.refSeq // .genBank), .asmId, .sciName]' "$assembly_list_file")
}


function get_table_columns() {
    local a
    while read line; do
        a=($line)
        DBKEY_COLUMNS[${a[0]}]=${a[1]}
        NAME_COLUMNS[${a[0]}]=${a[2]}
    done < <(python3 ./table-columns.py "$TOOL_DATA_TABLE_CONF")
    #declare -p DBKEY_COLUMNS
    #declare -p NAME_COLUMNS
}


function rewrite_names() {
    local table
    for table in ${!DBKEY_COLUMNS[@]}; do
        rewrite_names_in_table "$table"
    done
}


function rewrite_names_in_table() {
    local table="$1"
    local a name dbkey ent
    local name_column=${NAME_COLUMNS[$table]}
    local dbkey_column=${DBKEY_COLUMNS[$table]}
    while IFS=$'\t' read -r -a a; do
        dbkey=${a[$dbkey_column]}
        name=${ASSEMBLY_LIST[$dbkey]}
        a[$name_column]="$name"
        ent=$(printf "\t%s" "${a[@]}")
        echo "${ent:1}" >> $(basename $table)
    done < "$table"
}


fetch_assembly_list
get_table_columns
rewrite_names
