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

import org.springframework.beans.factory.annotation.Autowired;
import org.trustedanalytics.cloud.cc.api.CcOperations;
import org.trustedanalytics.datasetpublisher.boundary.Metadata;

import java.util.UUID;

public class DatabaseNameResolver {

    private final CcOperations ccOperations;

    @Autowired
    public DatabaseNameResolver(CcOperations ccOperations) {
        this.ccOperations = ccOperations;
    }

    public String resolvePublicName() {
        return "public";
    }

    public String resolvePrivateName(UUID orgUUID) {
        return ccOperations.getOrg(orgUUID).toBlocking().single().getName();
    }

    public String resolveName(Metadata metadata) {
        if(metadata.isPublic) {
            return resolvePublicName();
        }
        return resolvePrivateName(UUID.fromString(metadata.orgUUID));
    }
}
