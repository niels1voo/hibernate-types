package com.vladmihalcea.hibernate.type.util.providers;

import java.util.List;
import org.hibernate.dialect.PostgreSQL95Dialect;
import org.postgresql.ds.PGSimpleDataSource;

import javax.sql.DataSource;
import java.util.Properties;

/**
 * @author Vlad Mihalcea
 */
public class PostgreSQLDataSourceProvider implements DataSourceProvider {

    @Override
    public String hibernateDialect() {
        return PostgreSQL95Dialect.class.getName();
    }

    @Override
    public DataSource dataSource() {
        PGSimpleDataSource dataSource = new PGSimpleDataSource();
        dataSource.setDatabaseName("postgres");
        dataSource.setServerNames(new String[]{"localhost"});
        dataSource.setUser("niels");
        dataSource.setPassword("");
        return dataSource;
    }

    @Override
    public Class<? extends DataSource> dataSourceClassName() {
        return PGSimpleDataSource.class;
    }

    @Override
    public Properties dataSourceProperties() {
        Properties properties = new Properties();
        properties.setProperty("databaseName", "postgres");
        properties.setProperty("serverName", "localhost");
        properties.setProperty("user", username());
        properties.setProperty("password", password());
        return properties;
    }

    @Override
    public String url() {
        return null;
    }

    @Override
    public String username() {
        return "niels";
    }

    @Override
    public String password() {
        return "";
    }

    @Override
    public Database database() {
        return Database.POSTGRESQL;
    }
}
