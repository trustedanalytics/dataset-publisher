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
package org.trustedanalytics.datasetpublisher;

import static org.springframework.context.annotation.ScopedProxyMode.INTERFACES;
import static org.springframework.web.context.WebApplicationContext.SCOPE_REQUEST;

import com.google.common.collect.ImmutableSet;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.trustedanalytics.cloud.auth.AuthTokenRetriever;
import org.trustedanalytics.cloud.auth.OAuth2TokenRetriever;
import org.trustedanalytics.cloud.cc.FeignClient;
import org.trustedanalytics.cloud.cc.api.CcOperations;

import org.apache.hive.jdbc.HiveDriver;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;
import org.springframework.jdbc.core.JdbcOperations;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.SimpleDriverDataSource;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.trustedanalytics.datasetpublisher.boundary.ExternalTool;

import java.sql.Driver;
import java.util.Set;
import java.util.function.Supplier;

@Configuration
@EnableConfigurationProperties({ Config.Hue.class, Config.Arcadia.class })
public class Config {

    @Value("${hive.url}")
    private String hiveUrl;

    @Value("${hive.user}")
    private String hiveUser;

    @Value("${cf.resource}")
    private String apiBaseUrl;

    @Bean
    public Driver hiveDriver() {
        return new HiveDriver();
    }

    @Bean
    public JdbcOperations hiveTemplate() {
        return new JdbcTemplate(new SimpleDriverDataSource(hiveDriver(), hiveUrl, hiveUser, ""));
    }

    @Bean
    public AuthTokenRetriever authTokenRetriever() {
        return new OAuth2TokenRetriever();
    }

    @Bean
    @Scope(value = SCOPE_REQUEST, proxyMode = INTERFACES)
    public CcOperations ccOperations() {
        final Authentication auth = SecurityContextHolder.getContext().getAuthentication();
        final String token = authTokenRetriever().getAuthToken(auth);

        return new FeignClient(apiBaseUrl, builder -> builder
            .requestInterceptor(template -> template.header("Authorization", "bearer " + token)));
    }

    @Bean
    public Supplier<Set<String>> restrictedKeywords() {
        return () -> ImmutableSet.<String>builder().add("add", "aggregate", "all", "alter",
                "and", "api_version", "as", "asc", "avro", "between", "bigint", "binary", "boolean", "by", "cached",
                "case", "cast", "change", "char", "class", "close_fn", "column", "columns", "comment", "compute",
                "create", "cross", "data", "database", "databases", "date", "datetime", "decimal", "delimited",
                "desc", "describe", "distinct", "div", "double", "drop", "else", "end", "escaped", "exists",
                "explain", "external", "false", "fields", "fileformat", "finalize_fn", "first", "float", "format",
                "formatted", "from", "full", "function", "functions", "group", "having", "if", "in", "incremental",
                "init_fn", "inner", "inpath", "insert", "int", "integer", "intermediate", "interval", "into",
                "invalidate", "is", "join", "last", "left", "like", "limit", "lines", "load", "location", "merge_fn",
                "metadata", "not", "null", "nulls", "offset", "on", "or", "order", "outer", "overwrite", "parquet",
                "parquetfile", "partition", "partitioned", "partitions", "prepare_fn", "produced", "rcfile", "real",
                "refresh", "regexp", "rename", "replace", "returns", "right", "rlike", "row", "schema", "schemas",
                "select", "semi", "sequencefile", "serdeproperties", "serialize_fn", "set", "show", "smallint",
                "stats", "stored", "straight_join", "string", "symbol", "table", "tables", "tblproperties",
                "terminated", "textfile", "then", "timestamp", "tinyint", "to", "true", "uncached", "union",
                "update_fn", "use", "using", "values", "view", "when", "where", "with").build();
    }

    @ConfigurationProperties(prefix = "hue")
    public static class Hue extends ExternalTool {

    }

    @ConfigurationProperties(prefix = "arcadia")
    public static class Arcadia extends ExternalTool {

    }
}
