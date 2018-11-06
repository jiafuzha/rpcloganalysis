#!/bin/bash


if [ ! "$#" == 1 ]; then
    echo "need log folder" 2>&1
    exit 1
fi

logDir="$1"

if [[ ! $logDir == /* ]]; then
	logDir="$(pwd)"/"$logDir"
fi

if [ ! -d "$logDir" ]; then
	echo "$logDir doesn't exist or not a directory" 2>&1
	exit 1
fi

echo "processing logs under $logDir"

outputDir="$logDir"/processed
if [ -d "$outputDir" ]; then
	echo "removing old files under $outputDir" 2>&1
	rm -rf "$outputDir"
fi
mkdir "$outputDir"

echo "output processed logs to $outputDir" 2>&1

logDirName=`basename "$logDir"`
IFS='-'
fields=($logDirName)
jobId="${fields[0]}"
nodes="${fields[1]}"
blocks="${fields[2]}"

metaInfoFile=$outputDir/"meta.txt"
echo "nodes=$nodes" >> "$metaInfoFile"
echo "blocks=$blocks" >> "$metaInfoFile"

extract(){
	sourceLogFile=$1
	targetLogFile=$outputDir/$2
	regex=$3
	hasSite=$4
	searchIp=0
	if [ ! -z "$hasSite" ]; then
	    searchIp=1
	fi
	if [ "$searchIp" == "1" ]; then
	    while read line
	    do
		        matchLine=`grep "RequestMessage(.*, NettyRpcEndpointRef(.*), .*)" <<< $line | sed 's/.*RequestMessage(//g' | sed 's/:.*, NettyRpcEndpointRef.*//g'`

		        if [ ! -z "$matchLine" ]; then
		            echo "$matchLine $hasSite" >> "$metaInfoFile"
			        searchIp=0
			        break
		        fi
	    done < "$sourceLogFile"
	fi
    if [[ $sourceLogFile == *inbox.log ]]; then
	    grep "$regex" "$sourceLogFile" | xargs -L 1 echo >> "$targetLogFile"
    else
        grep "$regex" "$sourceLogFile" | xargs -L 1 | sed "s/, buffer length(/, LaunchTask - buffer length(/" | sed "s/,buffer length(/, StatusUpdate - buffer length(/"  >> "$targetLogFile"
    fi
}

extractAll(){
	sourceLogFile=$1
	site=$2
	searchIp=$3

    	if [ "$searchIp" == "1" ]; then
	    extract "$sourceLogFile" "$site-endpointref.log" "NettyRpcEndpointRef: ^^" "$site"
	else
	    extract "$sourceLogFile" "$site-endpointref.log" "NettyRpcEndpointRef: ^^"
	fi

	if [ ! "$site" == "master" ]; then
		extract "$sourceLogFile" "$site-broadcast.log" "TorrentBroadcast: ^^"
		extract "$sourceLogFile" "$site-clientport.log" "TransportClientFactory: ^^"
	fi
	extract "$sourceLogFile" "$site-inbox.log" "Inbox: ^^"
	extract "$sourceLogFile" "$site-reply.log" "\^\^reply to"
}

#process driver log
extractAll "$logDir/$jobId.error" "driver" "1"

#process worker & master logs
for ff in "$logDir/logs"/*
do
	case "$ff" in
		*master*)	extractAll "$ff" "master" "1";;
		*worker*)
			fname=`basename "$ff"`
			IFS='.'
			fname=($fname)
			lastIndex=$((${#fname[@]}-2))
			IFS='-'
			workerFields=(${fname[$lastIndex]})
			lastIndex=$((${#workerFields[@]}-1))
			site=${workerFields[$lastIndex]}
			extractAll "$ff" "$site" "1";;
	esac
done

#process executor logs
for ff in "$logDir/workers"/*/*/stderr
do
	site=
	while read line
	do
		matchLine=`grep "CoarseGrainedExecutorBackend: Started daemon with process name:" <<< "$line"`
		if [ ! -z "$matchLine" ]; then
			IFS='@'
			fields=($line)
			site=${fields[1]}
			break
		fi
	done < "$ff"
	if [ -z "$site" ]; then
		echo "cannot find site info from executor log, $ff" 2>&1
		exit 1
	fi
	echo "$site"
	extractAll "$ff" "$site-executor"
done
