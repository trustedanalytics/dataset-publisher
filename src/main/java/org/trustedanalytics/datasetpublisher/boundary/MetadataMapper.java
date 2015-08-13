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

import org.trustedanalytics.datasetpublisher.entity.HiveTable;
import org.trustedanalytics.cloud.cc.api.CcOperations;
import org.trustedanalytics.cloud.cc.api.CcOrg;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.net.URI;
import java.util.*;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Maps external metadata entity to internally used HiveTable. During conversion names of
 * database, table and columns may change to fit database engine constraints.
 */
@Component
public class MetadataMapper implements Function<Metadata, HiveTable> {

    private final CcOperations ccOperations;
    private final Set<String> restrictedKeywords;

    @Autowired
    public MetadataMapper(CcOperations ccOperations, Supplier<Set<String>> restrictedKeywords) {
        this.ccOperations = ccOperations;
        this.restrictedKeywords = restrictedKeywords.get();
    }

    @Override
    public HiveTable apply(Metadata metadata) {
        final CcOrg ccOrg = ccOperations.getOrg(UUID.fromString(metadata.orgUUID));
        final String databaseName = toValidName(ccOrg.getName());
        final String tableName = toValidName(metadata.title);
        final List<String> fields = Arrays.stream(metadata.dataSample.split(","))
            .map(this::toValidName)
            .collect(Collectors.toList());
        final String location = toValidLocation(metadata.targetUri);

        return new HiveTable(databaseName, tableName, fields, location);
    }

    /**
     * Converts name to valid string according to database engine and driver constraints.
     * @param string name
     * @return valid name
     */
    private String toValidName(String string) {
        final Function<String, String> lowercase = String::toLowerCase;

        return lowercase
            // prefix must be a letter
            .andThen(s -> s.matches("^[a-zA-Z].*") ? s : "x" + s)
            // replace non alphanumeric characters
            .andThen(s -> s.replaceAll("\\W", "_"))
            // add underscore after name that is Impala reserved word
            .andThen(s -> toValidKeyword(s) ? s + "_" : s)
            // apply initial name
            .apply(string);
    }

    private String toValidLocation(String location) {
        String path = URI.create(location).getPath();
        return path.substring(0, path.lastIndexOf("/"));
    }

    private boolean toValidKeyword(String string) {
        return restrictedKeywords.contains(string);
    }

}
