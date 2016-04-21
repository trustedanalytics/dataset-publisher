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

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.trustedanalytics.datasetpublisher.entity.HiveTable;
import org.trustedanalytics.hadoop.config.client.helper.Hive;
import org.trustedanalytics.hadoop.config.client.oauth.JwtToken;

import java.sql.Connection;
import java.sql.Statement;
import java.util.Collections;

import javax.security.auth.login.LoginException;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = {HiveServiceTest.HiveServiceTestConfiguration.class,
                                 HiveService.class})
public class HiveServiceTest {

  @Autowired
  private QueryBuilder queryBuilder;

  @Autowired
  private JwtToken userIdentity;

  @Autowired
  private Hive hiveClient;

  @Autowired
  private HiveService hiveService;

  @Test
  public void testCreateTable() throws Exception {
    // given
    final HiveTable hiveTable = new HiveTable("db", "table", Collections.emptyList(), "loc");
    Connection connection = mock(Connection.class);
    Statement stm = mock(Statement.class);

    // when
    when(connection.createStatement()).thenReturn(stm);
    when(hiveClient.getConnection(userIdentity)).thenReturn(connection);
    hiveService.createTable(hiveTable, userIdentity);

    // then
    verify(queryBuilder).createTable(hiveTable);
  }

  @Test
  public void testDropTable() throws Exception {
    //given
    final HiveTable hiveTable = new HiveTable("db", "table", Collections.emptyList(), "loc");
    Connection connection = mock(Connection.class);
    Statement stm = mock(Statement.class);

    //when
    when(connection.createStatement()).thenReturn(stm);
    when(hiveClient.getConnection(userIdentity)).thenReturn(connection);
    hiveService.dropTable(hiveTable, userIdentity);

    //then
    verify(queryBuilder).dropTable(hiveTable);
  }

  @Test(expected = RuntimeException.class)
  public void testCreateTable_executionQueryException_propagatesAsRuntime() throws Exception {
    //given
    final HiveTable hiveTable = new HiveTable("db", "table", Collections.emptyList(), "loc");
    Connection connection = mock(Connection.class);
    Statement stm = mock(Statement.class);
    when(connection.createStatement()).thenReturn(stm);
    when(hiveClient.getConnection(userIdentity)).thenReturn(connection);
    when(stm.executeUpdate(anyObject())).thenThrow(LoginException.class);

    //when
    hiveService.createTable(hiveTable, userIdentity);

    //then
    //any Exception in executing query propagates as RuntimeException
  }

  @Configuration
  static class HiveServiceTestConfiguration {

    @Bean
    public QueryBuilder queryBuilder() {
      QueryBuilder builder = mock(QueryBuilder.class);
      when(builder.createDatabase(any())).thenReturn("sql");
      when(builder.createTable(any())).thenReturn("sql");
      when(builder.dropTable(any())).thenReturn("sql");
      return builder;
    }

    @Bean
    public JwtToken userIdentity() {
      return mock(JwtToken.class);
    }

    @Bean
    public Hive hiveClient(JwtToken token) {
      return mock(Hive.class);
    }
  }
}
