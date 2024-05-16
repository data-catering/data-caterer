#!/bin/bash

if [ "$#" -ne 3 ]; then
    echo "Usage: $0 input_directory output_directory sample_responses_directory"
    exit 1
fi

input_directory=$1
output_directory=$2
sample_responses_directory=$3

if [ ! -d "$input_directory" ]; then
    echo "Error: Input directory '$input_directory' not found."
    exit 1
fi

if [ ! -d "$output_directory" ]; then
    mkdir -p "$output_directory"
fi

if [ ! -d "$sample_responses_directory" ]; then
    echo "Error: Sample responses directory '$sample_responses_directory' not found."
    exit 1
fi

# Get a list of sample response files
sample_responses=("$sample_responses_directory"/*)

# Loop through all .js files in the input directory
for input_file in "$input_directory"/*.js; do
    if [ -f "$input_file" ]; then
        file_name=$(basename "$input_file")
        output_file="$output_directory/$file_name"

        # Extract paths from fetch calls
        fetch_paths=$(grep -o -P 'fetch\s*\(\s*["'"'"'].*?["'"'"']\s*\)' "$input_file" | grep -o -P '["'"'"'].*?["'"'"']')

        # Replace dummy response with matching sample response
        dummy_response='Promise.resolve({ dummy: "response" })'

        for fetch_path in $fetch_paths; do
            # Extract filename from the fetch path and search for a matching sample response
            filename=$(basename "$fetch_path")
            matching_sample_response=$(find "$sample_responses_directory" -name "$filename" | head -n 1)

            if [ -f "$matching_sample_response" ]; then
                # Replace the dummy response with the content of the matching sample response
                perl -0777 -pe "s/fetch\s*\([^)]*\)\s*\)/\$(cat $matching_sample_response | awk '{gsub(/\\n/,\"\\\\n\")}1')/gs" "$input_file" > "$output_file"
            fi
        done

        echo "Successfully modified $input_file. Result saved to $output_file"
    fi
done
