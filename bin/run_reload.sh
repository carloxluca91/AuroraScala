#!/bin/bash

scriptName="run_reload.sh"
function log() {

  echo -e "$(date "+%Y-%m-%d %H:%M:%S")" "[${1}]" "${2}"
}

export -f log

specificationFlag=""
lookupFlag=""
log "INFO" "Starting $scriptName script"
while [[ "$#" -gt 0 ]];
do
  case "$1" in
    -m) log "INFO" "-m flag (Flag for overriding specifications) has been triggered"; specificationFlag="-m" ;;
    -l) log "INFO" "-l flag (Flag for overriding lookup) has been triggered"; lookupFlag="-l" ;;
    *) log "WARNING" "Unknown parameter passed: $1"; ;;
  esac
  shift
done

if [[ -z $specificationFlag ]] && [[ -z $lookupFlag ]];
then
  log "ERROR" "Neither -m or -l flag has been triggered. Thus, Spark job will not be triggered"
else
  log "INFO" "Triggered flags: $specificationFlag $lookupFlag"
  . run_branch_new.sh -b RELOAD $specificationFlag $lookupFlag
fi

log "INFO" "Successfully run $scriptName script"