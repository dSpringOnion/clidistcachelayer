#!/usr/bin/env python3
"""
Comprehensive API fixer for integration tests.
Converts old API to new ShardingClient OperationResult API.
"""

import re
import sys
from pathlib import Path

def fix_vector_to_string_literals(content):
    """Convert {'h', 'e', 'l', 'l', 'o'} to "hello" """

    # Pattern 1: {'h', 'e', 'l', 'l', 'o'} -> "hello"
    def convert_char_array(match):
        chars = match.group(1)
        # Extract characters from 'a', 'b', 'c' format
        char_list = re.findall(r"'(.)'", chars)
        string_val = ''.join(char_list)
        return f'"{string_val}"'

    content = re.sub(
        r"std::vector<uint8_t> (\w+) = \{([^}]+)\};",
        lambda m: f'std::string {m.group(1)} = {convert_char_array(m)};',
        content
    )

    return content

def fix_vector_initialization(content):
    """Convert std::vector<uint8_t> value(10, 'x') to std::string value(10, 'x')"""

    # Pattern: std::vector<uint8_t> value(size, 'c') -> std::string value(size, 'c')
    content = re.sub(
        r"std::vector<uint8_t> (\w+)\((\d+), '(.)'\)",
        r'std::string \1(\2, "\3")',
        content
    )

    # Pattern: std::vector<uint8_t> value(size, static_cast<uint8_t>(x))
    # -> std::string value(size, static_cast<char>(x))
    content = re.sub(
        r"std::vector<uint8_t> (\w+)\(([^,]+), static_cast<uint8_t>\(([^)]+)\)\)",
        r'std::string \1(\2, static_cast<char>(\3))',
        content
    )

    return content

def fix_set_calls(content):
    """Fix client_->Set() to use OperationResult"""

    # Pattern: bool success = client_->Set(...)
    # Replace with: auto result = client_->Set(...); bool success = result.success;

    lines = content.split('\n')
    fixed_lines = []

    for i, line in enumerate(lines):
        # Match: bool XXX = client_->Set(...)
        match = re.match(r'(\s+)bool (\w+) = client_->Set\(', line)
        if match:
            indent = match.group(1)
            var_name = match.group(2)

            # Replace bool with auto result
            new_line = line.replace(f'bool {var_name}', 'auto set_result')
            fixed_lines.append(new_line)

            # Add: bool var_name = result.success;
            fixed_lines.append(f'{indent}bool {var_name} = set_result.success;')
        else:
            fixed_lines.append(line)

    return '\n'.join(fixed_lines)

def fix_delete_calls(content):
    """Fix client_->Delete() to use OperationResult"""

    lines = content.split('\n')
    fixed_lines = []

    for i, line in enumerate(lines):
        match = re.match(r'(\s+)bool (\w+) = client_->Delete\(', line)
        if match:
            indent = match.group(1)
            var_name = match.group(2)

            new_line = line.replace(f'bool {var_name}', 'auto del_result')
            fixed_lines.append(new_line)
            fixed_lines.append(f'{indent}bool {var_name} = del_result.success;')
        else:
            fixed_lines.append(line)

    return '\n'.join(fixed_lines)

def fix_get_calls(content):
    """Fix client_->Get() to check .success and .value.has_value()"""

    # Pattern: auto var = client_->Get(key);
    #          if (var.has_value())
    # Replace with: if (var.success && var.value.has_value())

    content = re.sub(
        r'if \((\w+)\.has_value\(\)\)',
        r'if (\1.success && \1.value.has_value())',
        content
    )

    # Pattern: ASSERT_TRUE(var.has_value())
    content = re.sub(
        r'ASSERT_TRUE\((\w+)\.has_value\(\)\)',
        r'ASSERT_TRUE(\1.success && \1.value.has_value())',
        content
    )

    # Pattern: EXPECT_TRUE(var.has_value())
    content = re.sub(
        r'EXPECT_TRUE\((\w+)\.has_value\(\)\)',
        r'EXPECT_TRUE(\1.success && \1.value.has_value())',
        content
    )

    # Pattern: EXPECT_FALSE(var.has_value())
    content = re.sub(
        r'EXPECT_FALSE\((\w+)\.has_value\(\)\)',
        r'EXPECT_FALSE(\1.success && \1.value.has_value())',
        content
    )

    # Pattern: var->value (dereference) -> *var.value (dereference optional)
    content = re.sub(
        r'(\w+)->value',
        r'*\1.value',
        content
    )

    return content

def process_file(filepath):
    """Process a single file"""
    print(f"Processing {filepath}...")

    with open(filepath, 'r') as f:
        content = f.read()

    # Apply all fixes
    content = fix_vector_to_string_literals(content)
    content = fix_vector_initialization(content)
    content = fix_set_calls(content)
    content = fix_delete_calls(content)
    content = fix_get_calls(content)

    # Write back
    with open(filepath, 'w') as f:
        f.write(content)

    print(f"  Fixed {filepath}")

if __name__ == '__main__':
    files = [
        'tests/integration/multi_node_test.cpp',
        'tests/integration/scaling_test.cpp'
    ]

    for filepath in files:
        if Path(filepath).exists():
            process_file(filepath)
        else:
            print(f"  WARNING: {filepath} not found")

    print("\nDone! All files processed.")
