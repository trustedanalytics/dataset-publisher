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
package org.trustedanalytics.datasetpublisher.boundary;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableSet;
import org.trustedanalytics.datasetpublisher.entity.HiveTable;
import org.trustedanalytics.cloud.cc.api.CcOperations;
import org.trustedanalytics.cloud.cc.api.CcOrg;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.trustedanalytics.datasetpublisher.service.DatabaseNameResolver;
import rx.Observable;

import java.util.Set;
import java.util.UUID;
import java.util.function.BiFunction;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = {MetadataMapperTest.HiveControllerTestConfig.class})
public class MetadataMapperTest {

    @Autowired
    private UUID orgUUID;

    @Autowired
    private String orgname;

    @Autowired
    private BiFunction<Metadata, String, HiveTable> metadataMapper;

    @Autowired
    private DatabaseNameResolver databaseNameResolver;

    @Test
    public void testMapValidMetadata() {
        // given
        final Metadata metadata = new Metadata();
        metadata.orgUUID = orgUUID.toString();
        metadata.title = "Qatar: GDP (constant LCU)";
        metadata.dataSample = "Complaint ID,Product,Location,Date,Date received,Timely response?";
        metadata.targetUri = "hdfs://10.10.123.123/cf/broker/instances/9614e6a4/3460a1d320b2/000000_1";
        metadata.isPublic = false;

        // when
        final HiveTable table = metadataMapper.apply(metadata, databaseNameResolver.resolveName(metadata));

        // then
        assertThat(table.databaseName, is(orgname));
        assertThat(table.tableName, is("qatar__gdp__constant_lcu_"));
        assertThat(table.fields, hasItems("complaint_id", "location_", "date_", "timely_response_"));
        assertThat(table.location, is("/cf/broker/instances/9614e6a4/3460a1d320b2"));
    }

    @Test
    public void testMapInvalidMetadata() {
        // given
        final Metadata metadata = new Metadata();
        metadata.orgUUID = orgUUID.toString();
        metadata.title = "1900: CHINA ITS";
        metadata.dataSample = ":One,*Two two,3Three,Four_";
        metadata.targetUri = "hdfs://10.10.123.123/cf/broker/instances/12/34/000000_1";
        metadata.isPublic = false;

        // when
        final HiveTable table = metadataMapper.apply(metadata, databaseNameResolver.resolveName(metadata));

        // then
        assertThat(table.databaseName, is(orgname));
        assertThat(table.tableName, is("x1900__china_its"));
        assertThat(table.fields, hasItems("x_one", "x_two_two", "x3three", "four_"));
        assertThat(table.location, is("/cf/broker/instances/12/34"));
    }

    @Test
    public void testMapPublicDataset() {
        // given
        final Metadata metadata = new Metadata();
        metadata.orgUUID = orgUUID.toString();
        metadata.title = "Qatar: GDP (constant LCU)";
        metadata.dataSample = "Complaint ID,Product,Location,Date,Date received,Timely response?";
        metadata.targetUri = "hdfs://10.10.123.123/cf/broker/instances/9614e6a4/3460a1d320b2/000000_1";
        metadata.isPublic = true;

        // when
        final HiveTable table = metadataMapper.apply(metadata, databaseNameResolver.resolveName(metadata));

        // then
        assertThat(table.databaseName, is("public"));
    }

    @Configuration
    static class HiveControllerTestConfig {

        @Bean
        public DatabaseNameResolver databaseNameResolver() {
            return new DatabaseNameResolver(ccOperations());
        }

        @Bean
        public UUID orgid() {
            return UUID.fromString("77fca4b7-5e06-4c40-909e-36fcaff90534");
        }

        @Bean
        public String orgName() {
            return "orgname";
        }


        @Bean
        public BiFunction<Metadata, String, HiveTable> metadataMapper() {
            Set<String> restrictedKeywords = ImmutableSet.of("location", "date");
            return new MetadataMapper(() -> restrictedKeywords);
        }



        @Bean
        public CcOperations ccOperations() {
            CcOperations ccOperations = mock(CcOperations.class);
            when(ccOperations.getOrg(orgid())).thenReturn(Observable.just(new CcOrg(orgid(), orgName())));
            return ccOperations;
        }
    }
}
