#! /bin/bash

# cd to scripts folder

_filepath=$1


# Generate Log file
log_dir="./logs"
`mkdir -p $log_dir`

log_dt=$(date +"%Y%m%d%s")
log_file_name="expanse_tracker_etl_postgres_${log_dt}"
log_file="${log_dir}/${log_file_name}"


if [ -z $_filepath ]; then
    echo "Empty string provided as filepath...."
    exit 1
fi

echo "Starting ETL script.."
python3 etl.py --filepath "$_filepath" >> $log_file 2>&1

if [ "$?" -eq "0" ]; then
    echo "Process complete..."
else
    echo "Process failed with exit code $?"
fi

exit 0