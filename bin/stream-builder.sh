#!/bin/bash

display_usage() { 
	echo -e "\nUsage:\n./stream-builder.sh streams_folder [output_file]\n"
        echo "If output_file is specified the output will be stored at it." 
} 

if [  $# -lt 1 ] 
then 
	display_usage
	exit 1
fi

STREAM_FOLDER="$1"
OUTPUT_FILE="$2"

FILES_NAME_ARRAY=()

for f in `ls $1`
do
	FILES_NAME_ARRAY+=($STREAM_FOLDER/$f)
done

if [ -z "$OUTPUT_FILE" ]
then
	jq -s '{"inputs" : [. | to_entries[] | .value | to_entries[] | {"key": .key , "value": [.key]}] | from_entries, "streams": .}'  ${FILES_NAME_ARRAY[*]}
else
	echo	
	echo "Loaded files ${FILES_NAME_ARRAY[*]}"
        echo
	echo -e "\nSaving stream to $OUTPUT_FILE\n"
	jq -s '{"inputs" : [. | to_entries[] | .value | to_entries[] | {"key": .key , "value": [.key]}] | from_entries, "streams": .}'  ${FILES_NAME_ARRAY[*]} > $OUTPUT_FILE
fi

