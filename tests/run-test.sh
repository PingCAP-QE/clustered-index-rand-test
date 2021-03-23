#!/bin/bash

set -euo

export PATH="tests/_utils:bin:$PATH"

stop_services
start_services

clustered-index-rand-test --store 'root:@tcp(127.0.0.1:4002)/test' clear
clustered-index-rand-test --store 'root:@tcp(127.0.0.1:4002)/test' init
clustered-index-rand-test --store 'root:@tcp(127.0.0.1:4002)/test' gen --test 5
clustered-index-rand-test --store 'root:@tcp(127.0.0.1:4002)/test' run \
  --dsn1 'root:@tcp(127.0.0.1:4001)/?time_zone=UTC' \
  --dsn2 'root:@tcp(127.0.0.1:4002)/?time_zone=UTC'
