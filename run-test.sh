#!/bin/sh

set -euo

export PATH="tests/_utils:bin:$PATH"

stop_services

echo "Starting tidb-master..."
tidb-master \
    -P 4001 \
    --status 10081 &

echo "Verifying tidb-master is started..."
i=0
while ! wget -q -O - "http://127.0.0.1:10081/status"; do
    i=$((i+1))
    if [ "$i" -gt 50 ]; then
        echo 'Failed to start tidb-master'
        return 1
    fi
    sleep 3
done

echo "Starting tidb-4.0..."
tidb-4.0 \
    -P 4002 \
    --status 10082 &

echo "Verifying tidb-4.0 is started..."
i=0
while ! wget -q -O - "http://127.0.0.1:10082/status"; do
    i=$((i+1))
    if [ "$i" -gt 50 ]; then
        echo 'Failed to start tidb-4.0'
        return 1
    fi
    sleep 3
done

clustered-index-rand-test --store 'root:@tcp(127.0.0.1:4002)/test' clear
clustered-index-rand-test --store 'root:@tcp(127.0.0.1:4002)/test' init
clustered-index-rand-test --store 'root:@tcp(127.0.0.1:4002)/test' gen --test 5
clustered-index-rand-test --store 'root:@tcp(127.0.0.1:4002)/test' run \
  --dsn1 'root:@tcp(127.0.0.1:4001)/?time_zone=UTC' \
  --dsn2 'root:@tcp(127.0.0.1:4002)/?time_zone=UTC'
