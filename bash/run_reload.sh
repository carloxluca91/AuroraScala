#!/bin/bash

scriptName="run_reload.sh"
function log() {

  echo -e "$(date "+%Y-%m-%d %H:%M:%S")" "[${1}]" "${2}"
}

export -f log

log "INFO" "Starting $scriptName script"

. run_branch.sh -b RELOAD

log "INFO" "End of script $scriptName"