#!/bin/bash

# Create a temporary directory
TMP_DIR=$(mktemp -d)
echo "Temporary directory created: $TMP_DIR"

# Define the log file path
LOG_FILE="$TMP_DIR/test.log"

# Create an initial log file with some data, including duplicates
echo '{"key1":"old_value_1"}' > "$LOG_FILE"
echo '{"key2":"value2"}' >> "$LOG_FILE"
echo '{"key1":"old_value_2"}' >> "$LOG_FILE" # This is the latest for key1 before the write op
echo '{"key3":"value3"}' >> "$LOG_FILE"
echo "Initial log file content created at $LOG_FILE"

# Run the main program to write a new value for key1.
# This should trigger compaction first (keeping "key1":"old_value_2"),
# and then append the new write operation ("key1":"new_value").
echo "Running main.go to write 'new_value' for 'key1'..."
go run main.go -file="$LOG_FILE" -type=log -op=write -key=key1 -value=new_value

# Read the content of the log file to verify the result
echo "Content of $LOG_FILE after running main.go:"
cat "$LOG_FILE"

# Clean up the temporary directory
# rm -rf "$TMP_DIR"
# echo "Temporary directory $TMP_DIR removed."
