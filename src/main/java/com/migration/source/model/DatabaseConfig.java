package com.migration.source.model;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class DatabaseConfig {
    private String driverClass;
    private String url;
    private String username;
    private String password;
    private String sourceTable;
}
