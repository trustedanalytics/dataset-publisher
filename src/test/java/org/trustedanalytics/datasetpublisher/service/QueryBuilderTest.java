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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import org.trustedanalytics.datasetpublisher.entity.HiveTable;

import com.google.common.collect.ImmutableList;

import org.junit.Test;

import java.sql.SQLException;
import java.util.List;

public class QueryBuilderTest {

    private final String databaseName = "testDb";
    private final String tableName = "testTable";
    private final List<String> columns = ImmutableList.of("one", "two");
    private final String location = "testLocation";
    private final HiveTable table = new HiveTable(databaseName, tableName, columns, location);

    @Test
    public void testCreateDatabaseQuery() throws SQLException {
        // given
        final QueryBuilder builder = new QueryBuilder();

        // when
        final String sql = builder.createDatabase(table);

        // then
        assertThat(sql, is("create database if not exists " + databaseName));
    }

    @Test
    public void testDropTableQuery() throws SQLException {
        // given
        final QueryBuilder builder = new QueryBuilder();

        // when
        final String sql = builder.dropTable(table);

        // then
        assertThat(sql, is("drop table if exists " + databaseName + "." + tableName));
    }

    @Test
    public void testCreateTableQuery() throws SQLException {
        // given
        final QueryBuilder builder = new QueryBuilder();

        // when
        final String sql = builder.createTable(table);

        // then
        assertThat(sql, is(String.format(
            "create external table if not exists %s (%s string,%s string) row format delimited fields terminated by ',' stored as textfile location '%s'",
            databaseName + "." + tableName, columns.get(0), columns.get(1), location)));
    }

}
