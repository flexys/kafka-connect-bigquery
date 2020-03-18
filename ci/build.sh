#!/bin/bash

set -exo pipefail

pushd kafka-connect-bigquery

./gradlew clean test distTar

cp -R kcbq-confluent/build/distributions/kcbq-confluent-*.tar ../workspace/kafka-connect-bigquery.tar

echo $(git rev-parse --short HEAD) > ../kafka-connect-bigquery-version/tag

popd
