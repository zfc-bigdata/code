package com.atguigu.gmall0317.publisher.service.impl;

import com.atguigu.gmall0317.publisher.service.DauService;
import io.searchbox.client.JestClient;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.TermsAggregation;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
@Slf4j
public class DauServiceImpl implements DauService {

    @Autowired
    JestClient jestClient;

    @Override
    public Long getDauTotal(String date) {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(new MatchAllQueryBuilder());

        String indexName = "gmall_dau_info_0317_" + date;
        Search search = new Search.Builder(searchSourceBuilder.toString())
                .addIndex(indexName).addType("_doc").build();
        SearchResult searchResult = null;
        try {
            SearchResult searchResult1 = jestClient.execute(search);
        }catch (IOException e){
            e.printStackTrace();
            throw new RuntimeException("es查询异常");
        }
        Long total = searchResult.getTotal();

        return total;

    }

    @Override
    public Map<String, Long> getDauHour(String date) {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(new MatchAllQueryBuilder());

        String indexName = "gmall_dau_info_0317_" + date;
        Search search = new Search.Builder(searchSourceBuilder.toString())
                .addIndex(indexName).addType("_doc").build();
        SearchResult searchResult = null;
        try {
            SearchResult searchResult1 = jestClient.execute(search);
        }catch (IOException e){
            e.printStackTrace();
            throw new RuntimeException("es查询异常");
        }
        Map<String, Long> rsMap = new HashMap<>();
        TermsAggregation termsAggregation = searchResult.getAggregations()
                .getTermsAggregation("groupby_hr");

        if (termsAggregation!=null) {
            List<TermsAggregation.Entry> buckets = termsAggregation.getBuckets();
            for (TermsAggregation.Entry bucket:buckets) {
                rsMap.put(bucket.getKey(),bucket.getCount());
            }
        }

        return rsMap;
    }
}
