#!/bin/bash

# Script to fix API mismatches in integration tests
# Converts from old API to new ShardingClient API

set -e

FILES=(
    "tests/integration/multi_node_test.cpp"
    "tests/integration/scaling_test.cpp"
)

for file in "${FILES[@]}"; do
    echo "Processing $file..."

    # Create backup
    cp "$file" "${file}.bak"

    # Fix 1: Convert vector<uint8_t> value = {'a', 'b'} to string value = "ab"
    # This is complex, so we'll do it manually for specific patterns

    # Fix 2: Convert bool success = client_->Set() to auto result = client_->Set() with .success check
    # Fix 3: Similar for Delete
    # Fix 4: Fix Get() return value checks

    echo "  Manual fixes needed for $file - will do with Python script"
done

echo "Backups created. Running Python fix script..."
python3 scripts/fix_integration_api.py
