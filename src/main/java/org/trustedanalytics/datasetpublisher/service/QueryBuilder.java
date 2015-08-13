/**
 * Copyright (c) 2015 Intel Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.trustedanalytics.datasetpublisher.service;

import org.trustedanalytics.datasetpublisher.entity.HiveTable;

import org.springframework.stereotype.Service;

import java.util.stream.Collectors;

@Service
public class QueryBuilder {

    public String createDatabase(HiveTable table) {
        return String.format("create database if not exists %s", table.databaseName);
    }

    public String createTable(HiveTable table) {
        return String.format("create external table if not exists %s (" +
                table.fields.stream().map(column -> column + " string")
                    .collect(Collectors.joining(",")) +
                ") row format delimited fields terminated by ',' stored as textfile location '%s'",
            table.getFullyQualifiedName(), table.location);
    }

    public String dropTable(HiveTable table) {
        return String.format("drop table if exists %s", table.getFullyQualifiedName());
    }
}
