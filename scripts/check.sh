#!/bin/bash

temp_file=$(mktemp /tmp/file_hashes.XXXXXX)

for file in "html_sources"/*
do
    hash=$(md5 -q "$file")
    match=$(grep "^$hash " "$temp_file")

    if [ -n "$match" ]; then
        matched_file=$(echo "$match" | awk '{print $2}')
        echo "Файл '$file' имеет такой же хеш, как и файл '$matched_file'"
    else
        echo "$hash $file" >> "$temp_file"
    fi
done

echo "Проверка завершена."

rm -f "$temp_file"
