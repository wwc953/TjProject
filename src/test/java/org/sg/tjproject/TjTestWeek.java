package org.sg.tjproject;

import com.alibaba.excel.EasyExcel;
import com.alibaba.excel.context.AnalysisContext;
import com.alibaba.excel.read.listener.ReadListener;
import com.alibaba.excel.util.ListUtils;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
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
import org.elasticsearch.index.query.PrefixQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
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
import org.sg.tjproject.bean.ExpVOWeek;
import org.sg.tjproject.bean.IndexOrNameData;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.*;

@Slf4j
@SpringBootTest
public class TjTestWeek {
    String index = "loglnfo";
    String type = "log";
    String path = "src/main/resources/xlsx2/";

    @Autowired
    RestHighLevelClient restHighLevelClient;

    @Test
    public void doAll() throws Exception {
        createIndex();
        List<String> list = Arrays.asList("2025-08-04.xlsx", "2025-08-05.xlsx", "2025-08-06.xlsx", "2025-08-07.xlsx", "2025-08-08.xlsx");
        list.forEach(fileName -> {
            try {
                initData(path + fileName);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

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
        createIndexRequest.settings(Settings.builder()
                .put("index.number_of_shards", 1)
                .put("index.number_of_replicas", 0)
        );
        CreateIndexResponse createIndexResponse = restHighLevelClient.indices().create(createIndexRequest);
        log.info("创建index结果：{}", createIndexResponse.isAcknowledged());
    }


    public void initData(String fileName) throws Exception {
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
                System.out.println(fileName + "===总插入条数===" + total);
            }
        }).sheet().doRead();


    }

    public Boolean batchCreateUserDocument(List<IndexOrNameData> list) throws Exception {
//        System.out.println("需要插入==>" + index + ",数量:" + list.size());
        BulkRequest bulkRequest = new BulkRequest();
        for (IndexOrNameData document : list) {
            IndexRequest indexRequest = new IndexRequest(index, type)
                    .source(JSON.toJSONString(document), XContentType.JSON);
            bulkRequest.add(indexRequest);
        }
        BulkResponse bulk = restHighLevelClient.bulk(bulkRequest);
        boolean hasFailures = bulk.hasFailures();
        if (hasFailures) {
            throw new RuntimeException("异常:" + bulk.buildFailureMessage());
        }
        return hasFailures;
    }


    @Test
    public void week() throws Exception {
        int days = 5;
        System.out.println("===week====" + index);
        SearchRequest searchRequest = new SearchRequest(index);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        BoolQueryBuilder rootQuery = QueryBuilders.boolQuery();
        TermsQueryBuilder rs = QueryBuilders.termsQuery("operView.keyword", "退出机器人", "连接成功", "连接失败", "关闭助理", "通知唤醒", "初始化机器人");
        rootQuery.mustNot(rs);
//        PrefixQueryBuilder orgNoListQu = QueryBuilders.prefixQuery("mgtOrgCode.keyword", "32401");
//        rootQuery.filter(orgNoListQu);

        searchSourceBuilder.size(0);
        searchSourceBuilder.query(rootQuery);


        TermsAggregationBuilder mgt = AggregationBuilders.terms("mgtorg_agg")
                .field("cityCode.keyword").size(2000).order(BucketOrder.key(true));
        CardinalityAggregationBuilder rsf = AggregationBuilders.cardinality("dis_rs").field("handleId.keyword");
        mgt.subAggregation(rsf);

        TermsQueryBuilder gzp = QueryBuilders.termsQuery("operView.keyword", "查询作业计划", "制定作业计划", "创建作业计划", "查询问题制作工作票", "查询我的工作票", "工作票安全交底", "工作票编制", "工作票签发", "工作票许可", "工作票终结", "我的工作票", "许可工作票", "终结工作票");
        FilterAggregationBuilder filterGzp = AggregationBuilders.filter("filter_gzp", gzp);
        mgt.subAggregation(filterGzp);

        TermsQueryBuilder zc = QueryBuilders.termsQuery("operView.keyword", "一键扫码", "扫码装拆");
        FilterAggregationBuilder filterSm = AggregationBuilders.filter("filter_zc", zc);
        mgt.subAggregation(filterSm);

        TermsQueryBuilder zs = QueryBuilders.termsQuery("operView.keyword", "查询知识库", "查询知识详情", "知识考试", "大模型数据", "练习题库");
        FilterAggregationBuilder filterZs = AggregationBuilders.filter("filter_zs", zs);
        mgt.subAggregation(filterZs);

        TermsQueryBuilder ckzb = QueryBuilders.termsQuery("operView.keyword", "工作总览", "查询线损", "查询欠费", "查询台区异常",
                "查询采集异常", "欠费已停电用户", "临时用电超期用户", "断相指标", "减容预警用户",
                "临时用电超期预警用户", "综合线损", "电费回收率");
        FilterAggregationBuilder filterckzb = AggregationBuilders.filter("filter_ckzb", ckzb);
        mgt.subAggregation(filterckzb);

        TermsQueryBuilder zygd = QueryBuilders.termsQuery("operView.keyword",
                "待办工单", "附近工单", "当前位置工单", "规划工单路径", "查询更名工单", "查询居民峰谷电变更工单",
                "查询定比定量变更工单", "查询装拆调试工单", "查询上门服务工单", "查询农电工单", "规划工作", "创建三入走访工单"
                , "创建充电设施维护工单", "创建巡视检查工单", "创建光伏设施维护工单");
        FilterAggregationBuilder filterzygd = AggregationBuilders.filter("filter_zygd", zygd);
        mgt.subAggregation(filterzygd);

        searchSourceBuilder.aggregation(mgt);

        searchRequest.source(searchSourceBuilder);
        System.out.println(searchRequest.source().toString());

        SearchResponse searchResponse = restHighLevelClient.search(searchRequest);
        System.out.println(searchResponse);

        List<ExpVOWeek> result = new ArrayList<>();
        ParsedStringTerms mgtorgAgg = searchResponse.getAggregations().get("mgtorg_agg");
        List<? extends Terms.Bucket> buckets = mgtorgAgg.getBuckets();
        for (Terms.Bucket bucket : buckets) {
            ExpVOWeek expVO = new ExpVOWeek();
            expVO.setMgtOrgCode(bucket.getKeyAsString());
            expVO.setSyrc(bucket.getDocCount());
            expVO.setSyrcDay((long) Math.ceil(Double.valueOf(expVO.getSyrc()) / days));

            Map<String, Aggregation> map = bucket.getAggregations().asMap();
            ParsedCardinality dis_rs = (ParsedCardinality) map.get("dis_rs");
            expVO.setSyrs(dis_rs.getValue());
            expVO.setSyrsDay((long) Math.ceil(Double.valueOf(expVO.getSyrs()) / days));

            ParsedFilter filter_gzp = (ParsedFilter) map.get("filter_gzp");
            expVO.setGzp(filter_gzp.getDocCount());
            expVO.setGzpDay((long) Math.ceil(Double.valueOf(expVO.getGzp()) / days));

            ParsedFilter filter_zc = (ParsedFilter) map.get("filter_zc");
            expVO.setZczyfz(filter_zc.getDocCount());
            expVO.setZczyfzDay((long) Math.ceil(Double.valueOf(expVO.getZczyfz()) / days));

            ParsedFilter filter_zs = (ParsedFilter) map.get("filter_zs");
            expVO.setZswds(filter_zs.getDocCount());
            expVO.setZswdsDay((long) Math.ceil(Double.valueOf(expVO.getZswds()) / days));

            ParsedFilter filter_ckzb = (ParsedFilter) map.get("filter_ckzb");
            expVO.setCkzb(filter_ckzb.getDocCount());
            expVO.setCkzbDay((long) Math.ceil(Double.valueOf(expVO.getCkzb()) / days));

            ParsedFilter filter_zygd = (ParsedFilter) map.get("filter_zygd");
            expVO.setZygd(filter_zygd.getDocCount());
            expVO.setZygdDay((long) Math.ceil(Double.valueOf(expVO.getZygd()) / days));

            result.add(expVO);
        }
        String fileName = path + "一周.xlsx";
        EasyExcel.write(fileName, ExpVOWeek.class).sheet("").doWrite(result);

    }


    @Test
    public void moreDayTimeMgt() throws Exception {
        SearchRequest searchRequest = new SearchRequest(index);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        BoolQueryBuilder rootQuery = QueryBuilders.boolQuery();
        TermsQueryBuilder rs = QueryBuilders.termsQuery("operView.keyword", "退出机器人", "连接成功", "连接失败", "关闭助理", "通知唤醒", "初始化机器人");
        rootQuery.mustNot(rs);
        PrefixQueryBuilder orgNoListQu = QueryBuilders.prefixQuery("mgtOrgCode.keyword", "32401");
        rootQuery.filter(orgNoListQu);

        searchSourceBuilder.size(0);
        searchSourceBuilder.query(rootQuery);

        TermsAggregationBuilder dateAgg = AggregationBuilders.terms("time_datehis")
                .script(new Script(ScriptType.INLINE, Script.DEFAULT_SCRIPT_LANG,
                        "def time = doc['systemTime.keyword'].value;def beginIndex =time.indexOf(' ');def domain = time.substring(0,beginIndex);return domain", Collections.emptyMap(), Collections.emptyMap())
                ).size(2000).order(BucketOrder.key(true));

        TermsAggregationBuilder mgt = AggregationBuilders.terms("mgtorg_agg")
                .script(getMgtOrgCodeScript(7)).size(2000).order(BucketOrder.key(true));
        CardinalityAggregationBuilder rsf = AggregationBuilders.cardinality("dis_rs").field("handleId.keyword");
        mgt.subAggregation(rsf);

        TermsQueryBuilder gzp = QueryBuilders.termsQuery("operView.keyword", "查询作业计划", "制定作业计划", "创建作业计划", "查询问题制作工作票", "查询我的工作票", "工作票安全交底", "工作票编制", "工作票签发", "工作票许可", "工作票终结", "我的工作票", "许可工作票", "终结工作票");
        FilterAggregationBuilder filterGzp = AggregationBuilders.filter("filter_gzp", gzp);
        mgt.subAggregation(filterGzp);

        TermsQueryBuilder zc = QueryBuilders.termsQuery("operView.keyword", "一键扫码", "扫码装拆");
        FilterAggregationBuilder filterSm = AggregationBuilders.filter("filter_zc", zc);
        mgt.subAggregation(filterSm);

        TermsQueryBuilder zs = QueryBuilders.termsQuery("operView.keyword", "查询知识库", "查询知识详情", "知识考试", "大模型数据", "练习题库");
        FilterAggregationBuilder filterZs = AggregationBuilders.filter("filter_zs", zs);
        mgt.subAggregation(filterZs);

        dateAgg.subAggregation(mgt);

        searchSourceBuilder.aggregation(dateAgg);

        searchRequest.source(searchSourceBuilder);
        System.out.println(searchRequest.source().toString());

        SearchResponse searchResponse = restHighLevelClient.search(searchRequest);
        System.out.println(searchResponse);

//        List<ExpVO> result = new ArrayList<>();
//        ParsedStringTerms mgtorgAgg = searchResponse.getAggregations().get("mgtorg_agg");
//        List<? extends Terms.Bucket> buckets = mgtorgAgg.getBuckets();
//        for (Terms.Bucket bucket : buckets) {
//            ExpVO expVO = new ExpVO();
//            expVO.setMgtOrgCode(bucket.getKeyAsString());
//            expVO.setSyrc(bucket.getDocCount());
//            Map<String, Aggregation> map = bucket.getAggregations().asMap();
//            ParsedCardinality dis_rs = (ParsedCardinality) map.get("dis_rs");
//            expVO.setSyrs(dis_rs.getValue());
//            ParsedFilter filter_gzp = (ParsedFilter) map.get("filter_gzp");
//            expVO.setGzp(filter_gzp.getDocCount());
//
//            ParsedFilter filter_zc = (ParsedFilter) map.get("filter_zc");
//            expVO.setZczyfz(filter_zc.getDocCount());
//
//            ParsedFilter filter_zs = (ParsedFilter) map.get("filter_zs");
//            expVO.setZswds(filter_zs.getDocCount());
//            result.add(expVO);
//        }
//
//
//        System.out.println(JSON.toJSONString(result));
//
//        //读取模版
//        List<ExpVO> dataList = ListUtils.newArrayListWithExpectedSize(500);
//        EasyExcel.read(path + "模版.xlsx", ExpVO.class, new ReadListener<ExpVO>() {
//            @SneakyThrows
//            @Override
//            public void invoke(ExpVO dto, AnalysisContext analysisContext) {
//                dataList.add(dto);
//            }
//
//            @SneakyThrows
//            @Override
//            public void doAfterAllAnalysed(AnalysisContext analysisContext) {
//
//            }
//        }).sheet().doRead();
//
//
//        Map<String, ExpVO> collect = result.stream().collect(Collectors.toMap(ExpVO::getMgtOrgCode, v -> v));
//        dataList.forEach(v -> {
//            ExpVO expVO = collect.get(v.getMgtOrgCode());
//            if (expVO != null) {
//                String mgtOrgCodeName = v.getMgtOrgCodeName();
//                BeanUtils.copyProperties(expVO, v);
//                v.setMgtOrgCodeName(mgtOrgCodeName);
//            } else {
//                System.out.println(v.getMgtOrgCode() + "--无数据");
//            }
//        });
//
//        ExpVO all = new ExpVO();
//        all.setMgtOrgCodeName("合计");
//        dataList.forEach(v -> {
//            all.setSyrs(all.getSyrs() + v.getSyrs());
//            all.setSyrc(all.getSyrc() + v.getSyrc());
//            all.setZczyfz(all.getZczyfz() + v.getZczyfz());
//            all.setGzp(all.getGzp() + v.getGzp());
//            all.setZswds(all.getZswds() + v.getZswds());
//        });
//        dataList.add(all);
//
//        System.out.println("da==>" + JSON.toJSONString(dataList));
//
//
//        String fileName = path +   "more.xlsx";
//        EasyExcel.write(fileName, ExpVO.class).sheet("模板").doWrite(dataList);

    }

    public Script getMgtOrgCodeScript(int length) {
        Map<String, Object> param = new HashMap<>();
        param.put("len", length);
        Script script = new Script(ScriptType.INLINE, Script.DEFAULT_SCRIPT_LANG,
                "def domain = doc['mgtOrgCode.keyword'].value; if(domain.length()<params['len']) { return domain;} else { return domain.substring(0, params['len']);}", Collections.emptyMap(), param);
        return script;
    }


}
