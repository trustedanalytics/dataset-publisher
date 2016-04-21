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

import com.google.common.base.Throwables;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.trustedanalytics.datasetpublisher.entity.HiveTable;
import org.trustedanalytics.hadoop.config.client.helper.Hive;
import org.trustedanalytics.hadoop.config.client.oauth.JwtToken;

import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import javax.security.auth.login.LoginException;

/**
 * Service executing queries to create and drop tables.
 */
@Service
public class HiveService {

  private static final Logger LOGGER = LoggerFactory.getLogger(HiveService.class);
  private final QueryBuilder queryBuilder;
  private final Hive hiveClient;

  @Autowired
  public HiveService(QueryBuilder queryBuilder, Hive hiveClient) {
    this.queryBuilder = queryBuilder;
    this.hiveClient = hiveClient;
  }

  /**
   * Creates table if it doesn't exist
   * @param table hive table
   * @param userIdentity user identity
   */
  public void createTable(HiveTable table, JwtToken userIdentity) {
    // ensure database exists
    execute(queryBuilder.createTable(table), userIdentity);
  }

  /**
   * Drops table if it exists
   * @param table hive table
   * @param userIdentity user identity
   */
  public void dropTable(HiveTable table, JwtToken userIdentity) {
    execute(queryBuilder.dropTable(table), userIdentity);
  }

  private void execute(String sql, JwtToken userIdentity) {
    LOGGER.info("Execute: {}", sql);
    try (Connection connection = hiveClient.getConnection(userIdentity)) {
      Statement stm = connection.createStatement();
      stm.executeUpdate(sql);
    } catch (InterruptedException |
        IOException |
        LoginException |
        SQLException |
        URISyntaxException e) {
      LOGGER.error(String.format("Can't execute query %s", sql), e);
      throw Throwables.propagate(e);
    }
  }
}
