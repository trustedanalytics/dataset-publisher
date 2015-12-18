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

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.trustedanalytics.datasetpublisher.Config;
import org.trustedanalytics.datasetpublisher.entity.HiveTable;
import org.trustedanalytics.datasetpublisher.service.DatabaseNameResolver;
import org.trustedanalytics.datasetpublisher.service.HiveService;

import java.util.Optional;
import java.util.function.BiFunction;

@RunWith(MockitoJUnitRunner.class)
public class HiveControllerTest {

    private HiveController sut;

    @Mock
    private HiveService hiveService;

    @Mock
    private BiFunction<Metadata, String, HiveTable> metadataMapper;

    @Mock
    private HiveTable hiveTable;

    private Config.Hue hue;
    private Config.Arcadia arcadia;

    @Mock
    private DatabaseNameResolver databaseNameResolver;

    @Before
    public void setUp() {
        when(metadataMapper.apply(any(), any())).thenReturn(hiveTable);
        when(databaseNameResolver.resolveName(any())).thenReturn("orgName");
        
        hue = new Config.Hue();
        hue.setUrl("http://hue.example.com");
        hue.setAvailable(true);
        arcadia = new Config.Arcadia();
        arcadia.setUrl("http://arcadia.example.com");
        arcadia.setAvailable(true);

        sut = new HiveController(hiveService, metadataMapper, hue, arcadia, databaseNameResolver);
    }

    @Test
    public void test_createTable_createTableAndSendBackLinks() {
        CreateTableResponse result = sut.createTable(new Metadata());

        verify(hiveService).createTable(hiveTable);
        Assert.assertNotNull(result.getArcadiaUrl());
        Assert.assertNotNull(result.getHueUrl());
    }

    @Test
    public void test_createTable_onlyArcadiaAvailable_sendOnlyArcadiaLink() {
        hue.setAvailable(false);

        CreateTableResponse result = sut.createTable(new Metadata());

        verify(hiveService).createTable(hiveTable);
        Assert.assertNotNull(result.getArcadiaUrl());
        Assert.assertNull(result.getHueUrl());
    }

    @Test
    public void test_createTable_onlyHueAvailable_sendOnlyHueLink() {
        arcadia.setAvailable(false);

        CreateTableResponse result = sut.createTable(new Metadata());

        verify(hiveService).createTable(hiveTable);
        Assert.assertNull(result.getArcadiaUrl());
        Assert.assertNotNull(result.getHueUrl());
    }

    @Test
    public void test_createTable_noToolAvailable_sendNoLink() {
        hue.setAvailable(false);
        arcadia.setAvailable(false);

        CreateTableResponse result = sut.createTable(new Metadata());

        verify(hiveService).createTable(hiveTable);
        Assert.assertNull(result.getArcadiaUrl());
        Assert.assertNull(result.getHueUrl());
    }

    @Test
    public void test_dropTable_scope_public() {
        sut.dropTable(new Metadata(), Optional.of("public"));

        verify(hiveService, times(1)).dropTable(any());
    }

    @Test
    public void test_dropTable_scope_private() {
        when(metadataMapper.apply(any(), any())).thenReturn(hiveTable);
        Metadata metadata = new Metadata();
        metadata.setOrgUUID("cccccf34-f597-4634-8dd2-1875c06b9c4c");
        sut.dropTable(metadata, Optional.<String>empty());

        verify(hiveService, times(2)).dropTable(any());
    }
}
