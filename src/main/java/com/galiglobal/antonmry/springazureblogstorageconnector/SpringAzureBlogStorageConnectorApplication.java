package com.galiglobal.antonmry.springazureblogstorageconnector;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import java.util.function.Function;

@SpringBootApplication
@Slf4j
public class SpringAzureBlogStorageConnectorApplication {
    public static void main(String[] args) {
        SpringApplication.run(SpringAzureBlogStorageConnectorApplication.class);
    }

    @Bean
    public Function<byte[], byte[]> handle() {
        return in -> new String(in).toUpperCase().getBytes();
    }
}
