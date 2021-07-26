#!/usr/bin/bash
find . -path ./lib -prune -o -regex '.*\.\(cpp\|hpp\)' -exec clang-format -style=file -i {} \;

