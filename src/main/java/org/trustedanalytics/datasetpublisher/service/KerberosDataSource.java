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

import org.springframework.jdbc.datasource.AbstractDataSource;
import org.trustedanalytics.hadoop.config.client.AppConfiguration;
import org.trustedanalytics.hadoop.config.client.Configurations;
import org.trustedanalytics.hadoop.config.client.Property;
import org.trustedanalytics.hadoop.config.client.ServiceInstanceConfiguration;
import org.trustedanalytics.hadoop.kerberos.KrbLoginManager;
import org.trustedanalytics.hadoop.kerberos.KrbLoginManagerFactory;

import javax.security.auth.Subject;
import javax.security.auth.login.LoginException;
import javax.sql.DataSource;
import java.io.IOException;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Objects;

public class KerberosDataSource extends AbstractDataSource implements DataSource {

    private static final String JDBC_DRIVER = "org.apache.hive.jdbc.HiveDriver";

    private final String jdbcUrl;
    private final String user;
    private final String pass;
    private final KrbLoginManager loginManager;

    public KerberosDataSource(String jdbcUrl) {

        this.jdbcUrl = Objects.requireNonNull(jdbcUrl);

        //Read application config from environment
        AppConfiguration helper;
        try {
            helper = Configurations.newInstanceFromEnv();
        } catch (IOException e) {
            throw new IllegalStateException("Could not create instance of Helper", e);
        }

        ServiceInstanceConfiguration krbConf = helper.getServiceConfig("kerberos-service");

        //Getting config properties values
        final String kdc = krbConf.getProperty(Property.KRB_KDC).get();
        final String realm = krbConf.getProperty(Property.KRB_REALM).get();
        user = krbConf.getProperty(Property.USER).get();
        pass = krbConf.getProperty(Property.PASSWORD).get();

        loginManager = KrbLoginManagerFactory.getInstance()
            .getKrbLoginManagerInstance(kdc, realm);
    }

    @Override
    public Connection getConnection(String user, String pass) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Connection getConnection() {
        try {
            Subject subject = loginManager.loginWithCredentials(user, pass.toCharArray());
            return getConnection(subject);
        } catch (LoginException | PrivilegedActionException e) {
            throw new IllegalStateException("Could not login with given credentials", e);
        }
    }

    private Connection getConnection(Subject signedOnUserSubject) throws PrivilegedActionException {
        return (Connection) Subject.doAs(signedOnUserSubject, (PrivilegedExceptionAction<Object>) () -> {
            Class.forName(JDBC_DRIVER);
            return DriverManager.getConnection(jdbcUrl, null, null);
        });
    }
}
