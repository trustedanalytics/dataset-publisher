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

import static org.springframework.http.HttpStatus.BAD_REQUEST;
import static org.springframework.http.HttpStatus.CREATED;
import static org.springframework.http.HttpStatus.OK;
import static org.springframework.web.bind.annotation.RequestMethod.DELETE;
import static org.springframework.web.bind.annotation.RequestMethod.POST;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;
import org.trustedanalytics.datasetpublisher.Config;
import org.trustedanalytics.datasetpublisher.entity.HiveTable;
import org.trustedanalytics.datasetpublisher.service.DatabaseNameResolver;
import org.trustedanalytics.datasetpublisher.service.HiveService;

import java.util.Optional;
import java.util.UUID;
import java.util.function.BiFunction;

@RestController
public class HiveController {

    private final Config.Hue hue;
    private final Config.Arcadia arcadia;
    private final HiveService hiveService;
    private final BiFunction<Metadata, String, HiveTable> metadataMapper;
    private final DatabaseNameResolver databaseNameResolver;

    @Autowired
    public HiveController(HiveService hiveService, BiFunction<Metadata, String, HiveTable> metadataMapper,
        Config.Hue hue, Config.Arcadia arcadia, DatabaseNameResolver databaseNameResolver) {
        this.hiveService = hiveService;
        this.metadataMapper = metadataMapper;
        this.hue = hue;
        this.arcadia = arcadia;
        this.databaseNameResolver = databaseNameResolver;
    }

    @RequestMapping(value = "/rest/tables", method = POST)
    @ResponseStatus(value = CREATED)
    public CreateTableResponse createTable(@RequestBody Metadata metadata) {
        final HiveTable table = metadataMapper.apply(metadata, databaseNameResolver.resolveName(metadata));
        hiveService.createTable(table);

        final String hueUrl = hue.isAvailable()
            ? String.join("/", hue.getUrl(), table.databaseName, table.tableName)
            : null;
        final String arcadiaUrl = arcadia.isAvailable() ? arcadia.getUrl() : null;
        return new CreateTableResponse(hueUrl, arcadiaUrl);
    }

    @RequestMapping(value = "/rest/tables", method = DELETE)
    @ResponseStatus(value = OK)
    public void dropTable(@RequestBody Metadata metadata, @RequestParam Optional<String> scope) {

        hiveService.dropTable(metadataMapper.apply(metadata, databaseNameResolver.resolvePublicName()));
        if(!scope.filter("public"::equalsIgnoreCase).isPresent()) {
            hiveService.dropTable(metadataMapper.apply(metadata, databaseNameResolver.resolvePrivateName(UUID.fromString(metadata.orgUUID))));
        }
    }

    @ExceptionHandler(IllegalArgumentException.class)
    @ResponseStatus(value = BAD_REQUEST)
    public String errorHandler(IllegalArgumentException e) {
        return e.getMessage();
    }
}
