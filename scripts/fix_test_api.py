#!/usr/bin/env python3
"""
Script to fix API mismatches in test files.
Converts:
- std::vector<uint8_t> value to std::string value
- bool set_success = client_->Set() to auto result = client_->Set() + check result.success
- client_->Get().has_value() to client_->Get().success && client_->Get().value.has_value()
"""

import re
import sys

def convert_vector_to_string(content):
    # Convert vector initialization to string initialization
    # {'h', 'e', 'l', 'l', 'o'} -> "hello"
    patterns = [
        (r"std::vector<uint8_t> (\w+) = \{'(.)'\};", r'std::string \1 = "\2";'),
        (r"std::vector<uint8_t> (\w+) = \{([^}]+)\};", r'std::string \1 = "\2";'),  # simplified
        (r"std::vector<uint8_t> (\w+)\((\d+), '(.)'", r'std::string \1(\2, "\3")'),
        (r"std::vector<uint8_t> (\w+)\((\d+), static_cast<uint8_t>\(", r'std::string \1(\2, static_cast<char>('),
    ]

    for pattern, replacement in patterns:
        content = re.sub(pattern, replacement, content)

    return content

def fix_api_calls(content):
    # Fix bool success = client_->Set() to auto result = client_->Set()
    content = re.sub(
        r'bool (set_success|delete_success|success) = client_->Set\(([^)]+)\);',
        r'auto result = client_->Set(\2);\n    bool \1 = result.success;',
        content
    )

    content = re.sub(
        r'bool (set_success|delete_success|success) = client_->Delete\(([^)]+)\);',
        r'auto result = client_->Delete(\2);\n    bool \1 = result.success;',
        content
    )

    # Fix Get() calls
    content = re.sub(
        r'auto (\w+) = client_->Get\(([^)]+)\);\s*if \(\1\.has_value\(\)\)',
        r'auto \1 = client_->Get(\2);\n    if (\1.success && \1.value.has_value())',
        content
    )

    return content

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print(f"Usage: {sys.argv[0]} <file>")
        sys.exit(1)

    filepath = sys.argv[1]
    with open(filepath, 'r') as f:
        content = f.read()

    content = convert_vector_to_string(content)
    content = fix_api_calls(content)

    with open(filepath, 'w') as f:
        f.write(content)

    print(f"Fixed {filepath}")
