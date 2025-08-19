package org.sg.tjproject;

import com.alibaba.excel.EasyExcel;
import com.alibaba.excel.context.AnalysisContext;
import com.alibaba.excel.read.listener.ReadListener;
import com.alibaba.excel.util.ListUtils;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.beanutils.PropertyUtils;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.bucket.filter.FilterAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.filter.ParsedFilter;
import org.elasticsearch.search.aggregations.bucket.terms.ParsedStringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.cardinality.CardinalityAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.cardinality.ParsedCardinality;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.jupiter.api.Test;
import org.sg.tjproject.bean.ExpVO;
import org.sg.tjproject.bean.IndexOrNameData;
import org.sg.tjproject.utils.MgtOrgUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

@Slf4j
@SpringBootTest
public class CityDay {
    String day = LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMMdd"));
    String index = "loglnfo_" + day;
    String type = "log";
    String path = "src/main/resources/xlsx/";

    @Autowired
    RestHighLevelClient restHighLevelClient;

    @Test
    public void doAll() throws Exception {
        String fileName = "2025-08-14.xlsx";
        createIndex();
        importData(path + fileName);
    }

    /**
     * 全省每日统计
     *
     * @throws Exception
     */
    @Test
    public void expDayAll() throws Exception {
        System.out.println(index);
        SearchRequest searchRequest = new SearchRequest(index);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        BoolQueryBuilder rootQuery = QueryBuilders.boolQuery();
        TermsQueryBuilder rs = QueryBuilders.termsQuery("operView.keyword", "退出机器人", "连接成功", "连接失败", "关闭助理", "通知唤醒", "初始化机器人");
        rootQuery.mustNot(rs);
//        PrefixQueryBuilder orgNoListQu = QueryBuilders.prefixQuery("mgtOrgCode.keyword", "32401");
//        rootQuery.filter(orgNoListQu);
        searchSourceBuilder.size(0);
        searchSourceBuilder.query(rootQuery);

        TermsAggregationBuilder mgt = AggregationBuilders.terms("mgtorg_agg").field("cityCode.keyword").size(2000).order(BucketOrder.key(true));

        CardinalityAggregationBuilder rsf = AggregationBuilders.cardinality("dis_rs").field("handleId.keyword");
        mgt.subAggregation(rsf);

        TermsQueryBuilder gzp = QueryBuilders.termsQuery("operView.keyword", "查询作业计划", "制定作业计划", "创建作业计划", "查询问题制作工作票", "查询我的工作票", "工作票安全交底", "工作票编制", "工作票签发", "工作票许可", "工作票终结", "我的工作票", "许可工作票", "终结工作票");
        FilterAggregationBuilder filterGzp = AggregationBuilders.filter("filter_gzp", gzp);
        mgt.subAggregation(filterGzp);

//        TermsQueryBuilder zc = QueryBuilders.termsQuery("operView.keyword", "一键扫码", "扫码装拆");
//        FilterAggregationBuilder filterSm = AggregationBuilders.filter("filter_zc", zc);
//        mgt.subAggregation(filterSm);
        BoolQueryBuilder zc = QueryBuilders.boolQuery()
                .must(QueryBuilders.termsQuery("operView.keyword", "一键扫码", "扫码装拆"))
                .must(QueryBuilders.termsQuery("appNo.keyword", "_"));
        FilterAggregationBuilder filterSm = AggregationBuilders.filter("filter_zc", zc);
        mgt.subAggregation(filterSm);

        BoolQueryBuilder znzscll = QueryBuilders.boolQuery()
                .must(QueryBuilders.termsQuery("operView.keyword", "扫码装拆"))
                .mustNot(QueryBuilders.termsQuery("appNo.keyword", "_"));
        FilterAggregationBuilder znzscll_dis = AggregationBuilders.filter("filter_znzscll", znzscll)
                .subAggregation(AggregationBuilders.cardinality("znzscll_dis").field("appNo.keyword"));
        mgt.subAggregation(znzscll_dis);

        TermsQueryBuilder zs = QueryBuilders.termsQuery("operView.keyword", "查询知识库", "查询知识详情", "知识考试", "大模型数据", "练习题库");
        FilterAggregationBuilder filterZs = AggregationBuilders.filter("filter_zs", zs);
        mgt.subAggregation(filterZs);

        TermsQueryBuilder ckzb = QueryBuilders.termsQuery("operView.keyword", "工作总览", "查询线损", "查询欠费", "查询台区异常", "查询采集异常", "欠费已停电用户", "临时用电超期用户", "断相指标", "减容预警用户", "临时用电超期预警用户", "综合线损", "电费回收率");
        FilterAggregationBuilder filterckzb = AggregationBuilders.filter("filter_ckzb", ckzb);
        mgt.subAggregation(filterckzb);

        TermsQueryBuilder zygd = QueryBuilders.termsQuery("operView.keyword", "待办工单", "附近工单", "当前位置工单", "规划工单路径", "查询更名工单", "查询居民峰谷电变更工单", "查询定比定量变更工单", "查询装拆调试工单", "查询上门服务工单", "查询农电工单", "规划工作", "创建三入走访工单", "创建充电设施维护工单", "创建巡视检查工单", "创建光伏设施维护工单");
        FilterAggregationBuilder filterzygd = AggregationBuilders.filter("filter_zygd", zygd);
        mgt.subAggregation(filterzygd);

        searchSourceBuilder.aggregation(mgt);

        searchRequest.source(searchSourceBuilder);
        System.out.println(searchRequest.source().toString());

        SearchResponse searchResponse = restHighLevelClient.search(searchRequest);
        System.out.println(searchResponse);

        List<ExpVO> result = new ArrayList<>();
        ParsedStringTerms mgtorgAgg = searchResponse.getAggregations().get("mgtorg_agg");
        List<? extends Terms.Bucket> buckets = mgtorgAgg.getBuckets();
        for (Terms.Bucket bucket : buckets) {
            String mgtOrgCode = bucket.getKeyAsString();
            if ("32101".equals(mgtOrgCode)) {
                continue;
            }
            ExpVO expVO = new ExpVO();
            expVO.setMgtOrgCode(mgtOrgCode);
            expVO.setSyrc(bucket.getDocCount());
            Map<String, Aggregation> map = bucket.getAggregations().asMap();
            ParsedCardinality dis_rs = (ParsedCardinality) map.get("dis_rs");
            expVO.setSyrs(dis_rs.getValue());
            ParsedFilter filter_gzp = (ParsedFilter) map.get("filter_gzp");
            expVO.setGzp(filter_gzp.getDocCount());

            ParsedFilter filter_zc = (ParsedFilter) map.get("filter_zc");
            expVO.setZczyfz(filter_zc.getDocCount());

            ParsedFilter filter_znzscll = (ParsedFilter) map.get("filter_znzscll");
            ParsedCardinality aggregation = (ParsedCardinality) filter_znzscll.getAggregations().asList().get(0);
            expVO.setZnzscll(aggregation.getValue());

            ParsedFilter filter_zs = (ParsedFilter) map.get("filter_zs");
            expVO.setZswds(filter_zs.getDocCount());

            ParsedFilter filter_ckzb = (ParsedFilter) map.get("filter_ckzb");
            expVO.setCkzb(filter_ckzb.getDocCount());

            ParsedFilter filter_zygd = (ParsedFilter) map.get("filter_zygd");
            expVO.setZygd(filter_zygd.getDocCount());
            result.add(expVO);
        }

        System.out.println(JSON.toJSONString(result));

        ExpVO all = new ExpVO();
        all.setMgtOrgCode("合计");
        all.setMgtOrgCodeName("合计");
        List<String> proList = Arrays.asList("syrs", "syrc", "zczyfz", "gzp", "zswds", "ckzb", "zygd", "znzscll");
        result.forEach(v -> {
            for (String key : proList) {
                try {
                    long value = (Long) PropertyUtils.getProperty(all, key) + (Long) PropertyUtils.getProperty(v, key);
                    PropertyUtils.setProperty(all, key, value);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
            v.setMgtOrgCodeName(MgtOrgUtils.getCodeName(v.getMgtOrgCode()));
        });
        result.add(all);
        System.out.println("da==>" + JSON.toJSONString(result));

        String fileName = path + day + "全省.xlsx";
        EasyExcel.write(fileName, ExpVO.class).useDefaultStyle(false).sheet("").doWrite(result);
    }


    boolean exists(String index) throws Exception {
        GetIndexRequest request = new GetIndexRequest();
        request.indices(index);
        boolean exists = restHighLevelClient.indices().exists(request);
        return exists;
    }

    //    @Test
    void createIndex() throws Exception {
        if (exists(index)) {
            System.out.println("已存在索引");
            return;
        }
        CreateIndexRequest createIndexRequest = new CreateIndexRequest(index);
        createIndexRequest.settings(Settings.builder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0));
        CreateIndexResponse createIndexResponse = restHighLevelClient.indices().create(createIndexRequest);
        log.info("创建index结果：{}", createIndexResponse.isAcknowledged());
    }


    public void importData(String fileName) throws Exception {
        EasyExcel.read(fileName, IndexOrNameData.class, new ReadListener<IndexOrNameData>() {
            int BATCH_COUNT = 500;
            List dataList = ListUtils.newArrayListWithExpectedSize(BATCH_COUNT);
            int total = 0;

            @SneakyThrows
            @Override
            public void invoke(IndexOrNameData dto, AnalysisContext analysisContext) {
                dto.setCityCode(StringUtils.substring(dto.getMgtOrgCode(), 0, 5));
                dto.setCountryCode(StringUtils.substring(dto.getMgtOrgCode(), 0, 7));
                String linkName = dto.getLinkName();
                if (StringUtils.isNotBlank(linkName)) {
                    String substring = linkName.substring(linkName.indexOf("{"));
                    JSONObject jsonObject = JSON.parseObject(substring);
                    dto.setType(jsonObject.getString("type"));
                }
                dataList.add(dto);
                if (dataList.size() >= BATCH_COUNT) {
                    batchCreateUserDocument(dataList);
                    total += BATCH_COUNT;
                    dataList = ListUtils.newArrayListWithExpectedSize(BATCH_COUNT);
                }
            }

            @SneakyThrows
            @Override
            public void doAfterAllAnalysed(AnalysisContext analysisContext) {
                if (CollectionUtils.isNotEmpty(dataList)) {
                    total += dataList.size();
                    batchCreateUserDocument(dataList);
                }
                System.out.println("总插入条数===" + total);
            }
        }).sheet().doRead();


    }

    public Boolean batchCreateUserDocument(List<IndexOrNameData> list) throws Exception {
        BulkRequest bulkRequest = new BulkRequest();
        for (IndexOrNameData document : list) {
            IndexRequest indexRequest = new IndexRequest(index, type).source(JSON.toJSONString(document), XContentType.JSON);
            bulkRequest.add(indexRequest);
        }
        BulkResponse bulk = restHighLevelClient.bulk(bulkRequest);
        boolean hasFailures = bulk.hasFailures();
        if (hasFailures) {
            throw new RuntimeException("异常:" + bulk.buildFailureMessage());
        }
        return hasFailures;
    }


}
