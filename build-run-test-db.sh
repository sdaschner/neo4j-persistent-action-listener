#!/bin/bash
set -euo pipefail
cd ${0%/*}

trap cleanup EXIT

function cleanup() {
  echo "shutting down"
  docker stop test-db &> /dev/null || true
  rm -rf $tmpDir &> /dev/null || true
}


mvn package

tmpDir=$(mktemp -d)
mkdir $tmpDir/plugins

cp ./target/neo4j-persistent-action*.jar $tmpDir/plugins
touch $tmpDir/neo4j.conf

docker run --rm \
  --name test-db \
  -p 7474:7474 -p 7687:7687 \
  -v $tmpDir/neo4j.conf:/conf/neo4j.conf \
  -v $tmpDir/plugins:/plugins \
  --env NEO4J_AUTH=neo4j/test \
  --env NEO4J_ACCEPT_LICENSE_AGREEMENT=yes \
  neo4j:4.0.4-enterprise
