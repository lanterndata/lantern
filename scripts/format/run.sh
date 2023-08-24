#!/bin/bash

set -e

function usage() {
    echo "Usage: $0 [check|format] <filename>"
    exit 1
}

action="$1"
input_file="$2"

if [[ "$action" != "check" && "$action" != "format" ]]; then
    echo "Invalid action: $action"
    usage
fi

if [[ ! -f "$input_file" ]]; then
    echo "File not found: $input_file"
    usage
fi

formatting="on"

formatted_tmp=$(mktemp)
buffer_tmp=$(mktemp)

declare -A placeholder_map

process_buffer_with_pgpp() {
    if [[ -s "$buffer_tmp" ]]; then
        if ! grep -q -v -E '^\s*(--|#)' "$buffer_tmp"; then
            cat "$buffer_tmp" >> "$formatted_tmp"            
        else
            pgpp --preserve-comments --semicolon-after-last-statement --comma-at-eoln < "$buffer_tmp" | while IFS= read -r formatted_line; do
                for dummy_value in "${!placeholder_map[@]}"; do
                    original_value="${placeholder_map[$dummy_value]}"
                    formatted_line="${formatted_line//$dummy_value/$original_value}"
                done
                echo "$formatted_line" >> "$formatted_tmp"
            done
        fi
        > "$buffer_tmp"
    fi
}

trap "rm -f $buffer_tmp $formatted_tmp" EXIT

while IFS= read -r line || [[ -n "$line" ]]; do
    if [[ $line == *"-- format off"* ]]; then
        process_buffer_with_pgpp
        echo "$line" >> "$formatted_tmp"
        formatting="off"
    elif [[ $line == *"-- format on"* ]]; then
        echo "$line" >> "$formatted_tmp"
        formatting="on"
    elif [[ $formatting == "off" ]]; then
        process_buffer_with_pgpp
        echo "$line" >> "$formatted_tmp"
    elif [[ $line == *"\\"* && ! "$line" =~ ^[[:space:]]*(--|\/\*) ]]; then
        process_buffer_with_pgpp
        cmd="${line#*\\}"
        sql_part="${line%\\*}"
        if [[ "$sql_part" =~ [^[:space:]] ]]; then
            formatted_sql_part=$(echo "$sql_part" | pgpp --preserve-comments --comma-at-eoln)
            echo "$formatted_sql_part\\$cmd" >> "$formatted_tmp"
        else
            echo "\\$cmd" >> "$formatted_tmp"
        fi
    elif [[ $line =~ (:'[a-zA-Z0-9_]+'|:"[a-zA-Z0-9_]+"|:[a-zA-Z0-9_]+) ]]; then
        modified_line="$line"
        idx=1
        while [[ $modified_line =~ (:'[a-zA-Z0-9_]+'|:"[a-zA-Z0-9_]+"|:[a-zA-Z0-9_]+) ]]; do
            placeholder="${BASH_REMATCH[0]}"
            dummy_value="__dummy${idx}__"
            placeholder_map["$dummy_value"]="$placeholder"
            modified_line="${modified_line//$placeholder/$dummy_value}"
            idx=$((idx+1))
        done
        echo "$modified_line" >> "$buffer_tmp"
    elif [[ $line =~ [^[:space:]] ]]; then
        echo "$line" >> "$buffer_tmp"
    fi
done < "$input_file"

process_buffer_with_pgpp

if [[ "$action" == "check" ]]; then
    if ! diff "$input_file" "$formatted_tmp" &> /dev/null; then
        exit 1
    else
        exit 0
    fi
elif [[ "$action" == "format" ]]; then
    cat "$formatted_tmp"
fi
