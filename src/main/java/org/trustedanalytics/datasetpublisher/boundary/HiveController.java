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

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;
import org.trustedanalytics.datasetpublisher.Config;
import org.trustedanalytics.datasetpublisher.entity.HiveTable;
import org.trustedanalytics.datasetpublisher.service.HiveService;
import org.trustedanalytics.hadoop.config.client.oauth.JwtToken;

import java.util.Optional;
import java.util.function.Function;

import io.swagger.annotations.ApiOperation;

import static org.springframework.http.HttpStatus.BAD_REQUEST;
import static org.springframework.http.HttpStatus.CREATED;
import static org.springframework.http.HttpStatus.OK;
import static org.springframework.web.bind.annotation.RequestMethod.DELETE;
import static org.springframework.web.bind.annotation.RequestMethod.POST;

@RestController
public class HiveController {

  @Autowired
  private Config.Hue hue;

  @Autowired
  private Config.Arcadia arcadia;

  @Autowired
  private HiveService hiveService;

  @Autowired
  private Function<Metadata, HiveTable> metadataMapper;

  @Autowired
  private JwtToken userIdentity;

  @ApiOperation(
      value = "Create Hive table",
      notes = "Privilege level: Consumer of this endpoint must be a member of specified organization"
  )
  @RequestMapping(value = "/rest/tables", method = POST)
  @ResponseStatus(value = CREATED)
  public CreateTableResponse createTable(@RequestBody Metadata metadata) {
    final HiveTable table = metadataMapper.apply(metadata);
    hiveService.createTable(table, userIdentity);

    final String hueUrl = hue.isAvailable()
                          ? String.join("/", hue.getUrl(), table.databaseName, table.tableName)
                          : null;
    final String arcadiaUrl = arcadia.isAvailable() ? arcadia.getUrl() : null;
    return new CreateTableResponse(hueUrl, arcadiaUrl);
  }

  @ApiOperation(
      value = "Drop Hive table",
      notes = "Privilege level: Consumer of this endpoint must be a member of specified organization"
  )
  @RequestMapping(value = "/rest/tables", method = DELETE)
  @ResponseStatus(value = OK)
  public void dropTable(@RequestBody Metadata metadata, @RequestParam Optional<String> scope) {
    hiveService.dropTable(metadataMapper.apply(metadata), userIdentity);
  }

  @ExceptionHandler({IllegalArgumentException.class, IllegalStateException.class})
  @ResponseStatus(value = BAD_REQUEST)
  public String badRequestExceptionHandler(RuntimeException e) {
    return e.getMessage();
  }

  public static final class Builder {

    private Config.Hue hueConfig;

    private Config.Arcadia arcadiaConfig;

    private Function<Metadata, HiveTable> metadataMapper;

    private HiveService hiveService;

    private JwtToken userIdentity;

    public static Builder create() {
      return new Builder();
    }

    public Builder withHue(Config.Hue config) {
      this.hueConfig =  config;
      return this;
    }

    public Builder withArcadia(Config.Arcadia config) {
      this.arcadiaConfig = config;
      return this;
    }

    public Builder withMetadataMapper(Function<Metadata, HiveTable> mapper) {
      this.metadataMapper = mapper;
      return this;
    }

    public Builder withHive(HiveService hiveService) {
      this.hiveService = hiveService;
      return this;
    }

    public Builder asWho(JwtToken userIdentity) {
      this.userIdentity = userIdentity;
      return this;
    }

    public HiveController build() {
      HiveController controller = new HiveController();
      controller.hue = this.hueConfig;
      controller.hiveService = this.hiveService;
      controller.arcadia = this.arcadiaConfig;
      controller.userIdentity = this.userIdentity;
      controller.metadataMapper = this.metadataMapper;
      return controller;
    }

    private Builder() {
    }
  }
}
