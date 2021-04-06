#!/bin/bash

scriptName="run_datasource_load.sh"
function log() {

  echo -e "$(date "+%Y-%m-%d %H:%M:%S")" "[${1}]" "${2}"
}

export -f log

log "INFO" "Starting $scriptName script"

# Parse options
dataSourceShort="-s"
dataSourceLong="--data_source"
businessDateShort="-d"
businessDateLong="--dt_business_date"
while [[ "$#" -gt 0 ]];
do
  case "$1" in
    "$dataSourceShort"|"$dataSourceLong") dataSource="$2"; shift ;;
    "$businessDateShort"|"$businessDateLong") businessDate="$2"; shift ;;
    *) log "WARNING" "Unknown parameter passed: $1"; ;;
  esac
  shift
done

if [[ -z $dataSource ]];
then
  log "ERROR" "DataSource option ($dataSourceShort, $dataSourceLong) is unset. Cannot start application"
else
  if [[ -z $businessDate ]];
  then
    log "INFO" "Business date ($businessDateShort, $businessDateLong) not provided. Setting to default value"
    businessDate=$(date "+%Y-%m-%d")
  fi

  log "INFO" "Provided options:

      $dataSourceShort, $dataSourceLong (DataSource to be ingested): $dataSource
      $businessDateShort, $businessDateLong (DataSource partition to be ingested): $businessDate
  "

  . run_branch.sh -b DATASOURCE_LOAD $dataSourceShort "$dataSource" $businessDateShort "$businessDate"

  log "INFO" "End of script $scriptName"
fi