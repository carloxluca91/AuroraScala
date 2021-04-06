#!/bin/bash

scriptName="run_initial_load.sh"
function log() {

  echo -e "$(date "+%Y-%m-%d %H:%M:%S")" "[${1}]" "${2}"
}

export -f log

log "INFO" "Starting $scriptName script"

. run_branch.sh -b INITIAL_LOAD

log "INFO" "End of script $scriptName"