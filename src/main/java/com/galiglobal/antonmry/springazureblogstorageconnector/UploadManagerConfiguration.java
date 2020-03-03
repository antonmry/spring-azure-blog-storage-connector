package com.galiglobal.antonmry.springazureblogstorageconnector;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;
import org.springframework.validation.annotation.Validated;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;
import java.util.HashMap;
import java.util.Map;

@Data
@Validated
@Component
@ConfigurationProperties(prefix = "connector.azure.blob.kafka")
public class UploadManagerConfiguration {

    @NotNull
    private String[] topics;

    @NotEmpty
    private String groupId;

    private String dlqTopic;

    private Integer timeout;

    private Integer retries;

    private Integer firstBackoff;

    private Integer maxBackoff;

    private Integer blockSize;

    private Integer numBuffers;

    private Integer maxSingleUploadSize;
}
