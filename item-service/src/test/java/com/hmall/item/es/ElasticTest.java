package com.hmall.item.es;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

public class ElasticTest {

    private RestHighLevelClient client;

    @Test
    void testConnection() {
        System.out.println("ElasticTest testConnection");
    }

    @BeforeEach
    void setup() {
        client =  new RestHighLevelClient(RestClient.builder(
                HttpHost.create("http://192.168.58.128:9200")
        ));
    }

    @AfterEach
    void teardown() throws IOException {
        if (client != null) {
            client.close();
        }
    }
}
