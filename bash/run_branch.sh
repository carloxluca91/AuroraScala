#!/bin/bash

log "INFO" "Starting run_branch_new.sh script"

# Parse options
branchShort="-b"
branchLong="--branch"
dataSourceShort="-s"
dataSourceLong="--data_source"
businessDateShort="-d"
businessDateLong="--dt_business_date"
while [[ "$#" -gt 0 ]];
do
  case "$1" in
    "$branchShort"|"$branchLong") branch="$2"; shift ;;
    "$businessDateShort"|"$businessDateLong") businessDate="$2"; shift ;;
    "$dataSourceShort"|"$dataSourceLong") dataSource="$2"; shift ;;
    *) log "WARNING" "Unknown parameter passed: $1"; ;;
  esac
  shift
done

# If -b option has not been given, do not start the application
if [[ -z $branch ]];
then
  log "ERROR" "Branch option ($branchShort, $branchLong) is unset. Cannot start application";
else

  appName="Aurora Dataload - $branch"
  # Check if -s option has been given
  if [[ -n $dataSource ]];
  then
    dataSourceOption="$dataSourceShort $dataSource"
    # Check if -d options has been given
    if [[ -z $businessDate ]];
    then
      businessDate="$(date "+%Y-%m-%d")"
    fi
    businessDateOption="$businessDateShort $businessDate"
    appName="$appName ($dataSource, $businessDate)"
  fi

  log "INFO" "Supported options (with given arguments if any)

      $branchShort, $branchLong (Application branch): $branch
      $dataSourceShort, $dataSourceLong (DataSource to be ingested): $dataSource
      $businessDateShort, $businessDateLong (DataSource partition to be ingested): $businessDate
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
  impalaJdbcDriverJarPath="$hdfsAppPath/lib/impala-jdbc-driver.jar"
  mainClassParams="-b $branch -p $jobPropertiesFileName $dataSourceOption"

  log "INFO" "Spark submit parameters:

    app name: $appName
    app .properties file HDFS path: $jobPropertiesPath
    app log4j.properties file HDFS path (for logging): $log4jPropertiesPath
    Impal JDBC driver jar path: $impalaJdbcDriverJarPath
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
  --jars $impalaJdbcDriverJarPath \
  --name "$appName" \
  --class "$mainClass" $jarPath \
  -b "$branch" -p "$jobPropertiesFileName" $dataSourceOption $businessDateOption > "$logFilePath" 2>&1

log "INFO" "End of script run_branch.sh"