#!/bin/bash


# Iterate over all .py files in the directory
for file in "/*.py"; do
    # Check if the file is readable
    if [ -r "$file" ]; then
        echo "Running $file ..."
        python3 "$file"
    else
        echo "Error: Unable to read $file."
    fi
done
