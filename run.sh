#!/bin/bash
if [[ !($1 =~ ^(RR|ML|FIX|MIN|QCK|ALL)$)  ||
      !($2 =~ ^[0-9]+$)           ||
      !($3 =~ ^(static|dynamic)$) ||
      !($4 =~ ^(steal|no-steal)$) ]]; then
    echo "usage: ./run.sh [RR|ML|FIX|MIN|QCK|ALL] [repeat_count] [static|dynamic] [steal|no-steal]"
    exit 1
fi

options=""
if [ $3 == dynamic ]; then
    options="${options} --stream"
fi
if [ $4 == no-steal ]; then
    options="${options} --no-steal"
fi

for (( i = 0; i < $2; i++ )); do
    if [ $1 == ALL ]; then
        python3 main.py --allocator RR $options
        python3 main.py --allocator FIX $options
        python3 main.py --allocator MIN $options
        python3 main.py --allocator QCK $options
        python3 main.py --allocator ML $options
    else
        python3 main.py --allocator $1 $options
    fi
done