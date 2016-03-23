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

import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.net.URI;
import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Maps external metadata entity to internally used HiveTable. During conversion names of
 * database, table and columns may change to fit database engine constraints.
 */
@Component
public class MetadataMapper implements BiFunction<Metadata, String, HiveTable> {

    public static final int IDENTIFIER_MAX_LEN = 64;
    private final Set<String> restrictedKeywords;

    @Autowired
    public MetadataMapper(Supplier<Set<String>> restrictedKeywords) {
        this.restrictedKeywords = restrictedKeywords.get();
    }

    @Override
    public HiveTable apply(Metadata metadata, String databaseName) {
        final List<String> fields = Arrays.asList(metadata.getDataSample().split(","));
        // validate if initial names of header fields are distinct
        checkDuplicates(fields, "Duplicated header fields in file");

        final String tableName = toValidName(metadata.getTitle());
        final List<String> columns = fields.stream()
                .map(this::toValidName)
                .collect(Collectors.toList());
        final String location = toValidLocation(metadata.getTargetUri());

        // validate if names of fields transformed into name columns are distinct
        checkDuplicates(columns, "Duplicated columns in table");
        return new HiveTable(toValidName(databaseName), tableName, columns, location);
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
            .andThen(s -> isValidKeyword(s) ? s : s + "_")
            // limit identifier length
            .andThen(s -> StringUtils.left(s, IDENTIFIER_MAX_LEN))
            // apply initial name
            .apply(string);
    }

    private String toValidLocation(String location) {
        String path = URI.create(location).getPath();
        return path.substring(0, path.lastIndexOf("/"));
    }

    private boolean isValidKeyword(String string) {
        return !restrictedKeywords.contains(string);
    }

    private void checkDuplicates(List<String> strings, String exceptionMessagePrefix) {
        strings.stream()
                .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()))
                .entrySet()
                .stream()
                .filter(group -> group.getValue() > 1)
                .map(Map.Entry::getKey)
                .sorted()
                .reduce((acc, column) -> acc + ", " + column)
                .ifPresent(duplicates -> {
                    throw new IllegalStateException(exceptionMessagePrefix + ": " + duplicates);
                });
    }
}
