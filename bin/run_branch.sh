#!/bin/bash

log "INFO" "Starting run_branch.sh script"

# Parse options
while getopts ":b:d:s:" option; do
  case "$option" in
    b) branch=$OPTARG ;;
    d) businessDate=$OPTARG ;;
    s) dataSource=$OPTARG ;;
    *) log "WARNING" "Unknown option: $option" ;;
  esac
done

# If -b option has not been given, do not start the application
if [[ -z $branch ]];
then
  log "ERROR" "Undefined branch (-b) option. Cannot start application";
else

  appName="Aurora Dataload - $branch"
  # Check if -s option has been given
  if [[ -n $dataSource ]];
  then
    dataSourceOption="-s $dataSource"
    # Check if -d options has been given
    if [[ -z $businessDate ]];
    then
      businessDate="$(date "+%Y-%m-%d")"
    fi
    businessDateOption="-d $businessDate"
    appName="$appName ($dataSource, $businessDate)"
  fi

  log "INFO" "Supported options (with given arguments if any)

      -b (Application branch): $branch
      -d (Ingestion businessDate): $businessDate
      -s (Ingestion dataSource): $dataSource
      "

  # Spark submit parameters
  queue=root.users.cloudera
  jobPropertiesFileName=dataload_job.properties
  log4jPropertiesFileName=dataload_log4j.properties
  mainClass=it.luca.aurora.Main
  hdfsAppPath="hdfs:///user/cloudera/applications/aurora_dataload"
  jarPath="$hdfsAppPath/lib/aurora-dataload-0.3.0.jar"
  jobPropertiesPath="$hdfsAppPath/lib/$jobPropertiesFileName"
  log4jPropertiesPath="$hdfsAppPath/lib/$log4jPropertiesFileName"
  mainClassParams="-b $branch -p $jobPropertiesFileName $dataSourceOption $businessDateOption"

  log "INFO" "Spark submit parameters:

    app name: $appName
    app .properties file HDFS path: $jobPropertiesPath
    app log4j.properties file HDFS path (for logging): $log4jPropertiesPath
    app jar HDFS path: $jarPath
    app main class: $mainClass
    main class parameters: $mainClassParams
    "
fi

appLocalLogDir=/home/cloudera/workspace/aurora_dataload/log
logFilePath="$appLocalLogDir/dataload_log_$(date "+%Y_%m_%d_%H_%M_%S").log"
mkdir -p "$appLocalLogDir"
touch "$logFilePath"

spark-submit --master yarn --deploy-mode cluster --queue $queue \
  --files "$jobPropertiesPath,$log4jPropertiesPath,/etc/hive/conf/hive-site.xml" \
  --driver-java-options "-Dlog4j.configuration=$log4jPropertiesFileName" \
  --name "$appName" \
  --class "$mainClass" \
  $jarPath \
  -b "$branch" -p "$jobPropertiesFileName" $dataSourceOption $businessDateOption > "$logFilePath" 2>&1

log "INFO" "Successfully run run_branch.sh script"