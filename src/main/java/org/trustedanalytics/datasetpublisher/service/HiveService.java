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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcOperations;
import org.springframework.stereotype.Service;

/**
 * Service executing queries to create and drop tables.
 */
@Service
public class HiveService {

    private static final Logger LOGGER = LoggerFactory.getLogger(HiveService.class);
    private final JdbcOperations operations;
    private final QueryBuilder queryBuilder;

    @Autowired
    public HiveService(JdbcOperations operations, QueryBuilder queryBuilder) {
        this.operations = operations;
        this.queryBuilder = queryBuilder;
    }

    /**
     * Creates table if it doesn't exist
     * @param table
     */
    public void createTable(HiveTable table) {
        // ensure database exists
        execute(queryBuilder.createDatabase(table));
        execute(queryBuilder.createTable(table));
    }

    /**
     * Drops table if it exists
     * @param table
     */
    public void dropTable(HiveTable table) {
        execute(queryBuilder.dropTable(table));
    }

    private void execute(String sql) {
        LOGGER.info("Execute: {}", sql);

        operations.execute(sql);
    }
}
