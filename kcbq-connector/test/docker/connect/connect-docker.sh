#! /usr/bin/env bash
# Copyright 2016 WePay, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

tar -C /usr/local/share/kafka/plugins/kafka-connect-bigquery/ -xf /usr/local/share/kafka/plugins/kafka-connect-bigquery/kcbq.tar

connect-standalone \
    /etc/kafka-connect-bigquery/standalone-single-schema.properties \
    /etc/kafka-connect-bigquery/connector-single-schema.properties &
SINGLE_PID=$!

sleep 60
kill $SINGLE_PID

connect-standalone \
    /etc/kafka-connect-bigquery/standalone-multi-schema.properties \
    /etc/kafka-connect-bigquery/connector-multi-schema.properties &
MULTI_PID=$!

sleep 60
kill $MULTI_PID
