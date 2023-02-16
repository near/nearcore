#!/usr/bin/env bash
#
# Shows diff of host1:file and host2:file
# First copies those files locally
#
# Usage: ./diff_remote_files.sh project file host1 host2

project=$1
file=$2
host1=$3
host2=$4

local_file=$(echo $file | tr / _)
local_file1=$host1\_$local_file
local_file2=$host2\_$local_file

zone1=$(gcloud compute instances list --project=$project | grep ^$host1 | awk -F ' ' '{{print $2}}')
zone2=$(gcloud compute instances list --project=$project | grep ^$host2 | awk -F ' ' '{{print $2}}')

gcloud compute scp ubuntu@$host1:$file $local_file1 --project=$project --zone=$zone1
gcloud compute scp ubuntu@$host2:$file $local_file2 --project=$project --zone=$zone2

diff $local_file1 $local_file2
