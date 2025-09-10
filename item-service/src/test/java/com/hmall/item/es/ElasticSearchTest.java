package com.hmall.item.es;

import cn.hutool.json.JSONUtil;
import com.hmall.item.domain.dto.ItemDoc;
import org.apache.http.HttpHost;
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;

@SpringBootTest(properties = "spring.profiles.active=local")
public class ElasticSearchTest {

    private RestHighLevelClient client;

    @Test
    void testMatchAll() throws IOException {
        SearchRequest request = new SearchRequest("items");
        request.source()
                .query(QueryBuilders.matchAllQuery());

        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        SearchHits searchHits = response.getHits();
        TotalHits total = searchHits.getTotalHits();
        SearchHit[] hits = searchHits.getHits();
        for(SearchHit hit: hits) {
            String json = hit.getSourceAsString();
            System.out.println(json);
        }
        parseResponseResult(response);
    }

    @Test
    void testSearch() throws IOException {
        SearchRequest request = new SearchRequest("items");

        request.source().query(
                QueryBuilders.boolQuery()
                        .must(QueryBuilders.matchQuery("name", "milk"))
                        .filter(QueryBuilders.termQuery("name", "vita"))
                        .filter(QueryBuilders.rangeQuery("price").lt(300.0))
        );

        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        parseResponseResult(response);
    }

    @Test
    void testAgg() throws IOException {
        SearchRequest request = new SearchRequest("items");

        request.source().size(0);

        String brandAggName = "brandAgg";
        request.source().aggregation(
                AggregationBuilders.terms(brandAggName).field("brand").size(10)
        );

        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
    }

    @BeforeEach
    void setUp() {
        this.client = new RestHighLevelClient(RestClient.builder(
                HttpHost.create("http://192.168.58.128:9200")
        ));
    }

    @AfterEach
    void tearDown() throws IOException {
        this.client.close();
    }

    private void parseResponseResult(SearchResponse response) throws IOException {
        SearchHits searchHits = response.getHits();
        TotalHits total = searchHits.getTotalHits();
        SearchHit[] hits = searchHits.getHits();
        for(SearchHit hit: hits) {
            String json = hit.getSourceAsString();
            ItemDoc item = JSONUtil.toBean(json, ItemDoc.class);
            System.out.println(item);
        }
    }
}
