package com.galiglobal.antonmry.springazureblogstorageconnector;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties(UploadManagerConfiguration.class)
public class UploadManagerConfigurationHelper {
}
