package com.galiglobal.antonmry.springazureblogstorageconnector;

import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(SpringRunner.class)
@SpringBootTest
@Slf4j
public class KafkaTests {

    private static final String TOPIC = "testEmbeddedOut";

    @Autowired
    private UploadManager azure;

    @Test
    public void testSendReceive() throws IOException, InterruptedException {
        KafkaHelper kafkaManager = new KafkaHelper();
        kafkaManager.produce(TOPIC, "foo.txt".getBytes(), "foo".getBytes());
        kafkaManager.produce(TOPIC, "foo2.txt".getBytes(), "foo2".getBytes());

        Thread.sleep(2000);

        assertThat(new String(azure.download("foo.txt"))).isEqualTo("foo");
        assertThat(new String(azure.download("foo2.txt"))).isEqualTo("foo2");

        azure.deleteBatch(Arrays.asList("foo.txt", "foo2.txt"));
    }
}

