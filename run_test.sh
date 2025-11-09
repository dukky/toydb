#!/bin/bash

# Create a temporary directory
TMP_DIR=$(mktemp -d)
echo "Temporary directory created: $TMP_DIR"

# Define the log file path
LOG_FILE="$TMP_DIR/test.log"

# Create an initial log file with some data, including duplicates
echo '{"key":"key1","value":"old_value_1"}' > "$LOG_FILE"
echo '{"key":"key2","value":"value2"}' >> "$LOG_FILE"
echo '{"key":"key1","value":"old_value_2"}' >> "$LOG_FILE" # This is the latest for key1 before the write op
echo '{"key":"key3","value":"value3"}' >> "$LOG_FILE"
echo "Initial log file content created at $LOG_FILE"

# Run the main program to write a new value for key1.
# This should trigger compaction first (keeping "key1":"old_value_2"),
# and then append the new write operation ("key1":"new_value").
echo "Running main.go to write 'new_value' for 'key1'..."
go run main.go -file="$LOG_FILE" -type=log -op=write -key=key1 -value=new_value

# Read the content of the log file to verify the result
echo "Content of $LOG_FILE after write operation:"
cat "$LOG_FILE"
echo ""

# Delete key2 to test delete functionality
echo "Running main.go to delete 'key2'..."
go run main.go -file="$LOG_FILE" -type=log -op=delete -key=key2

# Read the content again to see the delete operation
echo "Content of $LOG_FILE after delete operation:"
cat "$LOG_FILE"
echo ""

# Write a new key to trigger another compaction
echo "Running main.go to write 'value4' for 'key4'..."
go run main.go -file="$LOG_FILE" -type=log -op=write -key=key4 -value=value4

# Final read to verify compaction removed the deleted key
echo "Content of $LOG_FILE after final write (should compact and remove deleted key2):"
cat "$LOG_FILE"

# Clean up the temporary directory
# rm -rf "$TMP_DIR"
# echo "Temporary directory $TMP_DIR removed."
