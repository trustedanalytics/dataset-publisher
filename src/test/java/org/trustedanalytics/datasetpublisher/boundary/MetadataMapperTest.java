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
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.trustedanalytics.cloud.cc.api.CcOperations;
import org.trustedanalytics.cloud.cc.api.CcOrg;
import org.trustedanalytics.datasetpublisher.entity.HiveTable;
import org.trustedanalytics.datasetpublisher.service.DatabaseNameResolver;

import com.google.common.collect.ImmutableSet;

import org.apache.commons.lang.StringUtils;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.Set;
import java.util.UUID;
import java.util.function.BiFunction;

import rx.Observable;

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

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @Test
    public void testMapValidMetadata() {
        // given
        final Metadata metadata = new Metadata();
        metadata.setOrgUUID(orgUUID.toString());
        metadata.setTitle("Qatar: GDP (constant LCU)");
        metadata.setDataSample("Complaint ID,Product,Location,Date,Date received,Timely response?");
        metadata.setTargetUri("hdfs://10.10.123.123/cf/broker/instances/9614e6a4/3460a1d320b2/000000_1");
        metadata.setIsPublic(false);

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
        metadata.setOrgUUID(orgUUID.toString());
        metadata.setTitle("1900: CHINA ITS");
        metadata.setDataSample(":One,*Two two,3Three,Four_");
        metadata.setTargetUri("hdfs://10.10.123.123/cf/broker/instances/12/34/000000_1");
        metadata.setIsPublic(false);

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
        metadata.setOrgUUID(orgUUID.toString());
        metadata.setTitle("Qatar: GDP (constant LCU)");
        metadata.setDataSample("Complaint ID,Product,Location,Date,Date received,Timely response?");
        metadata.setTargetUri("hdfs://10.10.123.123/cf/broker/instances/9614e6a4/3460a1d320b2/000000_1");
        metadata.setIsPublic(true);

        // when
        final HiveTable table = metadataMapper.apply(metadata, databaseNameResolver.resolveName(metadata));

        // then
        assertThat(table.databaseName, is("public"));
    }

    @Test
    public void testValidateDuplicatedFields(){
        // given
        final Metadata metadata = new Metadata();
        metadata.setOrgUUID(orgUUID.toString());
        metadata.setTitle("Qatar: GDP (constant LCU)");
        metadata.setDataSample(
            "A11,6,A34,A43,1169,A65,A75,4,A93,A101,4,A121,67,A143,A152,2,A173,1,A192,A201,1,A11,A192");
        metadata.setTargetUri(
            "hdfs://10.10.123.123/cf/broker/instances/9614e6a4/3460a1d320b2/000000_1");
        metadata.setIsPublic(true);

        exception.expect(IllegalStateException.class);
        exception.expectMessage("Duplicated header fields in file: 1, 4, A11, A192");

        // when
        final HiveTable table = metadataMapper.apply(metadata, databaseNameResolver.resolveName(metadata));

        // then
        Assert.assertNull(table);
    }

    @Test
    public void testValidateDuplicatedColumns(){
        // given
        final Metadata metadata = new Metadata();
        metadata.setOrgUUID(orgUUID.toString());
        metadata.setTitle("Qatar: GDP (constant LCU)");
        metadata.setDataSample("one,two, sth,#sth,_4,x$4,run ,run#");
        metadata.setTargetUri("hdfs://10.10.123.123/cf/broker/instances/9614e6a4/3460a1d320b2/000000_1");
        metadata.setIsPublic(false);

        exception.expect(IllegalStateException.class);
        exception.expectMessage("Duplicated columns in table: run_, x_4, x_sth");

        // when
        final HiveTable table = metadataMapper.apply(metadata, databaseNameResolver.resolveName(metadata));

        // then
        Assert.assertNull(table);
    }

    @Test
    public void testLongIdentifier() {
        // given
        final Metadata metadata = new Metadata();
        metadata.setOrgUUID(orgUUID.toString());
        metadata.setTitle(StringUtils.repeat("c", MetadataMapper.IDENTIFIER_MAX_LEN * 2));
        metadata.setDataSample("one,two," + StringUtils.repeat("a", MetadataMapper.IDENTIFIER_MAX_LEN
            * 2));
        metadata.setTargetUri("hdfs://10.10.123.123/cf/broker/instances/9614e6a4/3460a1d320b2/000000_1");
        metadata.setIsPublic(false);

        // when
        final HiveTable table = metadataMapper.apply(metadata, databaseNameResolver.resolveName(metadata));

        // then
        assertThat(table.tableName, is(StringUtils.repeat("c", MetadataMapper.IDENTIFIER_MAX_LEN)));
        assertThat(table.fields, containsInAnyOrder("one", "two", StringUtils.repeat("a", MetadataMapper.IDENTIFIER_MAX_LEN)));
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
