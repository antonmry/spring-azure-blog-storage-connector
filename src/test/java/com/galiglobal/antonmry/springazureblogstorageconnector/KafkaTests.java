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
    private static final String TOPIC2 = "testEmbeddedOut2";

    @Autowired
    private AzureHelper azure;

    private KafkaHelper kafka = new KafkaHelper();

    @Test
    public void testSendReceive() throws IOException, InterruptedException {

        try {
/*
            kafka.produce(TOPIC, null, "foo".getBytes());
*/
            kafka.produce(TOPIC, "foo.txt".getBytes(), "foo".getBytes());
            kafka.produce(TOPIC, "foo.txt".getBytes(), "foo".getBytes());
            kafka.produce(TOPIC2, "foo2.txt".getBytes(), "foo2".getBytes());

            // Make stronger this test
            Thread.sleep(5000);

            assertThat(new String(azure.download("foo.txt"))).isEqualTo("foo");
            assertThat(new String(azure.download("foo2.txt"))).isEqualTo("foo2");

        } finally {
            azure.deleteBatch(Arrays.asList("foo.txt", "foo2.txt"));
        }
    }
}

