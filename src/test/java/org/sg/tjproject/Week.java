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
import org.elasticsearch.index.query.*;
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
import org.sg.tjproject.utils.DynamicEasyExcelExportUtils;
import org.sg.tjproject.utils.MgtOrgUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;

import java.io.File;
import java.io.FileOutputStream;
import java.util.*;

@Slf4j
@SpringBootTest
public class Week {
    String index = "loglnfo";
    String type = "log";
    String path = "src/main/resources/xlsx2/";

    @Autowired
    RestHighLevelClient restHighLevelClient;

    @Autowired
    ResourceLoader resourceLoader;

    @Test
    public void initAll() throws Exception {
        createIndex();
        Resource resource = resourceLoader.getResource("classpath:xlsx2/");
        File[] files = resource.getFile().listFiles();
        for (File file : files) {
            try {
                String fx = file.getName().split("\\.")[1];
                if ("xlsx".equals(fx)) {
                    initData(file);
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
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


        TermsAggregationBuilder mgt = AggregationBuilders.terms("mgtorg_agg").field("cityCode.keyword").size(2000).order(BucketOrder.key(true));
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
        EasyExcel.write(fileName, ExpVOWeek.class).useDefaultStyle(false).sheet("").doWrite(result);

    }


    /**
     * 每日累加
     *
     * @throws Exception
     */
    @Test
    public void weekLj() throws Exception {
        int days = 5;
        SearchRequest searchRequest = new SearchRequest(index);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        BoolQueryBuilder rootQuery = QueryBuilders.boolQuery();
        TermsQueryBuilder rs = QueryBuilders.termsQuery("operView.keyword", "退出机器人", "连接成功", "连接失败", "关闭助理", "通知唤醒", "初始化机器人");
        rootQuery.mustNot(rs);

        searchSourceBuilder.size(0);
        searchSourceBuilder.query(rootQuery);


        TermsAggregationBuilder mgt = AggregationBuilders.terms("mgtorg_agg").script(getMgtOrgCodeScript(5)).size(2000).order(BucketOrder.key(true));

        TermsAggregationBuilder dateAgg = AggregationBuilders.terms("time_datehis").script(new Script(ScriptType.INLINE, Script.DEFAULT_SCRIPT_LANG, "def time = doc['systemTime.keyword'].value;def beginIndex =time.indexOf(' ');def domain = time.substring(0,beginIndex);return domain", Collections.emptyMap(), Collections.emptyMap())).size(2000).order(BucketOrder.key(true));


        CardinalityAggregationBuilder rsf = AggregationBuilders.cardinality("dis_rs").field("handleId.keyword");
        dateAgg.subAggregation(rsf);

        TermsQueryBuilder gzp = QueryBuilders.termsQuery("operView.keyword", "查询作业计划", "制定作业计划", "创建作业计划", "查询问题制作工作票", "查询我的工作票", "工作票安全交底", "工作票编制", "工作票签发", "工作票许可", "工作票终结", "我的工作票", "许可工作票", "终结工作票");
        FilterAggregationBuilder filterGzp = AggregationBuilders.filter("filter_gzp", gzp);
        dateAgg.subAggregation(filterGzp);

        TermsQueryBuilder zc = QueryBuilders.termsQuery("operView.keyword", "一键扫码", "扫码装拆");
        FilterAggregationBuilder filterSm = AggregationBuilders.filter("filter_zc", zc);
        dateAgg.subAggregation(filterSm);

        TermsQueryBuilder zs = QueryBuilders.termsQuery("operView.keyword", "查询知识库", "查询知识详情", "知识考试", "大模型数据", "练习题库");
        FilterAggregationBuilder filterZs = AggregationBuilders.filter("filter_zs", zs);
        dateAgg.subAggregation(filterZs);

        TermsQueryBuilder ckzb = QueryBuilders.termsQuery("operView.keyword", "工作总览", "查询线损", "查询欠费", "查询台区异常", "查询采集异常", "欠费已停电用户", "临时用电超期用户", "断相指标", "减容预警用户", "临时用电超期预警用户", "综合线损", "电费回收率");
        FilterAggregationBuilder filterckzb = AggregationBuilders.filter("filter_ckzb", ckzb);
        dateAgg.subAggregation(filterckzb);

        TermsQueryBuilder zygd = QueryBuilders.termsQuery("operView.keyword", "待办工单", "附近工单", "当前位置工单", "规划工单路径", "查询更名工单", "查询居民峰谷电变更工单", "查询定比定量变更工单", "查询装拆调试工单", "查询上门服务工单", "查询农电工单", "规划工作", "创建三入走访工单", "创建充电设施维护工单", "创建巡视检查工单", "创建光伏设施维护工单");
        FilterAggregationBuilder filterzygd = AggregationBuilders.filter("filter_zygd", zygd);
        dateAgg.subAggregation(filterzygd);

        mgt.subAggregation(dateAgg);

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

            ParsedStringTerms timeDatehis = (ParsedStringTerms) bucket.getAggregations().getAsMap().get("time_datehis");
            timeDatehis.getBuckets().stream().forEach(v -> {
                Map<String, Aggregation> map = v.getAggregations().asMap();
                ParsedCardinality dis_rs = (ParsedCardinality) map.get("dis_rs");
                expVO.setSyrs(expVO.getSyrs() + dis_rs.getValue());

                ParsedFilter filter_gzp = (ParsedFilter) map.get("filter_gzp");
                expVO.setGzp(expVO.getGzp() + filter_gzp.getDocCount());

                ParsedFilter filter_zc = (ParsedFilter) map.get("filter_zc");
                expVO.setZczyfz(expVO.getZczyfz() + filter_zc.getDocCount());

                ParsedFilter filter_zs = (ParsedFilter) map.get("filter_zs");
                expVO.setZswds(expVO.getZswds() + filter_zs.getDocCount());

                ParsedFilter filter_ckzb = (ParsedFilter) map.get("filter_ckzb");
                expVO.setCkzb(expVO.getCkzb() + filter_ckzb.getDocCount());

                ParsedFilter filter_zygd = (ParsedFilter) map.get("filter_zygd");
                expVO.setZygd(expVO.getZygd() + filter_zygd.getDocCount());
            });

            result.add(expVO);
        }

        //日均
        List<String> proList = Arrays.asList("syrs", "syrc", "zczyfz", "gzp", "zswds", "ckzb", "zygd");
        result.forEach(v -> {
            for (String key : proList) {
                try {
                    PropertyUtils.setProperty(v, key + "Day", (long) Math.ceil(Double.valueOf(String.valueOf(PropertyUtils.getProperty(v, key))) / days));
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        });

        System.out.println("result===>" + JSON.toJSONString(result));

        EasyExcel.write(path + "累计一周.xlsx", ExpVOWeek.class).useDefaultStyle(false).sheet("").doWrite(result);
    }


    /**
     * 一周 每天
     * 人数 类别
     * <p>
     * df3 = df[df["当前操作界面"].isin(["点击唤醒","语音唤醒"])]#语音唤醒 类别
     * <p>
     * df3 = df3[~df3["当前操作界面"].isin(["退出机器人","连接成功","连接失败","关闭助理","通知唤醒","初始化机器人","点击唤醒","语音唤醒"])] #指令 类别
     * <p>
     * df3 = df[df["当前操作界面"].isin(["查询知识库","查询知识详情","知识考试","大模型数据","练习题库"])] #知识类 类别
     */
    @Test
    public void weekLjDay() throws Exception {
        SearchRequest searchRequest = new SearchRequest(index);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        BoolQueryBuilder rootQuery = QueryBuilders.boolQuery();
        TermsQueryBuilder rs = QueryBuilders.termsQuery("operView.keyword", "退出机器人", "连接成功", "连接失败", "关闭助理", "通知唤醒", "初始化机器人");
        rootQuery.mustNot(rs);

        searchSourceBuilder.size(0);
        searchSourceBuilder.query(rootQuery);

        TermsAggregationBuilder mgt = AggregationBuilders.terms("mgtorg_agg").script(getMgtOrgCodeScript(5)).size(2000).order(BucketOrder.key(true));

        TermsAggregationBuilder dateAgg = AggregationBuilders.terms("time_datehis").script(new Script(ScriptType.INLINE, Script.DEFAULT_SCRIPT_LANG, "def time = doc['systemTime.keyword'].value;def beginIndex =time.indexOf(' ');def domain = time.substring(0,beginIndex);return domain", Collections.emptyMap(), Collections.emptyMap())).size(2000).order(BucketOrder.key(true));

        CardinalityAggregationBuilder rsf = AggregationBuilders.cardinality("dis_rs").field("handleId.keyword");
        dateAgg.subAggregation(rsf);

        TermsQueryBuilder yyhx = QueryBuilders.termsQuery("operView.keyword", "点击唤醒", "语音唤醒");
        FilterAggregationBuilder filterYyhx = AggregationBuilders.filter("filter_yyhx", yyhx);
        dateAgg.subAggregation(filterYyhx);

        TermsQueryBuilder zs = QueryBuilders.termsQuery("operView.keyword", "查询知识库", "查询知识详情", "知识考试", "大模型数据", "练习题库");
        FilterAggregationBuilder filterZs = AggregationBuilders.filter("filter_zs", zs);
        dateAgg.subAggregation(filterZs);

        FilterAggregationBuilder count_bul = AggregationBuilders.filter("count_zl",
                QueryBuilders.boolQuery().mustNot(QueryBuilders.termsQuery("operView.keyword",
                        "退出机器人", "连接成功", "连接失败", "关闭助理", "通知唤醒", "初始化机器人", "点击唤醒", "语音唤醒")));
        dateAgg.subAggregation(count_bul);
        mgt.subAggregation(dateAgg);

        searchSourceBuilder.aggregation(mgt);

        searchRequest.source(searchSourceBuilder);
        System.out.println(searchRequest.source().toString());

        SearchResponse searchResponse = restHighLevelClient.search(searchRequest);
        System.out.println(searchResponse);

        Map<String, List<Map<String, Object>>> result = new LinkedHashMap<>();
        ParsedStringTerms mgtorgAgg = searchResponse.getAggregations().get("mgtorg_agg");
        List<? extends Terms.Bucket> buckets = mgtorgAgg.getBuckets();
        for (Terms.Bucket bucket : buckets) {
            List<Map<String, Object>> objList = new ArrayList<>();
            ParsedStringTerms timeDatehis = (ParsedStringTerms) bucket.getAggregations().getAsMap().get("time_datehis");
            timeDatehis.getBuckets().stream().forEach(v -> {
                Map<String, Object> mapval = new HashMap<>();
                Map<String, Aggregation> map = v.getAggregations().asMap();
                mapval.put("date", v.getKeyAsString());
                mapval.put("mgtOrgCode", bucket.getKeyAsString());

                ParsedCardinality dis_rs = (ParsedCardinality) map.get("dis_rs");
                mapval.put("rs", dis_rs.getValue());

                ParsedFilter filter_yyhx = (ParsedFilter) map.get("filter_yyhx");
                mapval.put("yyhx", filter_yyhx.getDocCount());

                ParsedFilter filter_zs = (ParsedFilter) map.get("filter_zs");
                mapval.put("zs", filter_zs.getDocCount());

                ParsedFilter count_zl = (ParsedFilter) map.get("count_zl");
                mapval.put("zl", count_zl.getDocCount());

                objList.add(mapval);
            });
            if (!"32101".equals(bucket.getKeyAsString())) {
                result.put(bucket.getKeyAsString(), objList);
            }
        }
        System.out.println(JSON.toJSONString(result));

        List<String> proList = Arrays.asList("rs", "yyhx", "zs", "zl");
        List<String> proNameList = Arrays.asList("日均使用人数", "语音唤醒", "知识类", "指令");
        LinkedHashMap<String, String> headColumnMap = new LinkedHashMap<>();
        headColumnMap.put("mgtOrgCode", "单位");
        result.forEach((k, dateList) -> {
            for (int i = 0; i < proList.size(); i++) {
                for (Map<String, Object> obj : dateList) {
                    headColumnMap.put(proList.get(i) + obj.get("date").toString(), proNameList.get(i) + "," + obj.get("date").toString());
                }
            }
        });

        List<Map<String, Object>> dataList = new ArrayList<>();
        result.forEach((k, dateList) -> {
            Map<String, Object> dataMap = new LinkedHashMap<>();
            dataMap.put("mgtOrgCode", k);
            for (String key : proList) {
                for (Map<String, Object> obj : dateList) {
                    dataMap.put(key + obj.get("date").toString(), obj.get(key).toString());
                }
            }
            dataList.add(dataMap);
        });

        System.out.println(JSON.toJSONString(dataList));

        Map<String, Object> total = new LinkedHashMap<>();
        total.put("mgtOrgCode", "合计");
        dataList.get(0).forEach((k, v) -> {
            if (!"mgtOrgCode".equals(k)) {
                dataList.forEach(item -> {
                    Object val = total.get(k);
                    if (val == null) {
                        total.put(k, item.get(k));
                    } else {
                        long l = Long.valueOf(val.toString()) + Long.valueOf(item.get(k).toString());
                        total.put(k, l);
                    }
                });
            }
        });
        dataList.add(total);

        System.out.println(JSON.toJSONString(dataList));

        dataList.forEach(v -> {
            String code = v.get("mgtOrgCode").toString();
            v.put("mgtOrgCode", MgtOrgUtils.getCodeName(code));
        });

        byte[] stream = DynamicEasyExcelExportUtils.exportExcelFile(headColumnMap, dataList);
        FileOutputStream outputStream = new FileOutputStream(path + "地市明细.xlsx");
        outputStream.write(stream);
        outputStream.close();

    }


//    @Test
//    public void test() throws Exception {
//        LinkedHashMap<String, String> headColumnMap = Maps.newLinkedHashMap();
//        headColumnMap.put("mgtOrgCode", "单位");
//        for (int i = 0; i < 5; i++) {
//            headColumnMap.put("name" + i, "学生数据1,姓名" + i);
//        }
//        for (int i = 0; i < 5; i++) {
//            headColumnMap.put("sex" + i, "学生数据2,性别" + i);
//        }
//
//        List<Map<String, Object>> dataList = new ArrayList<>();
//        for (int i = 0; i < 5; i++) {
//            Map<String, Object> dataMap = Maps.newHashMap();
//            dataMap.put("mgtOrgCode", "一年级");
//            dataMap.put("name" + i, "张三99" + i);
//            dataMap.put("sex" + i, "男" + i);
//            dataList.add(dataMap);
//        }
//
//        byte[] stream = DynamicEasyExcelExportUtils.exportExcelFile(headColumnMap, dataList);
//        FileOutputStream outputStream = new FileOutputStream(new File(path + "easyexcel-export-user5.xlsx"));
//        outputStream.write(stream);
//        outputStream.close();
//    }


    public Script getMgtOrgCodeScript(int length) {
        Map<String, Object> param = new HashMap<>();
        param.put("len", length);
        Script script = new Script(ScriptType.INLINE, Script.DEFAULT_SCRIPT_LANG, "def domain = doc['mgtOrgCode.keyword'].value; if(domain.length()<params['len']) { return domain;} else { return domain.substring(0, params['len']);}", Collections.emptyMap(), param);
        return script;
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


    public void initData(File file) throws Exception {
//        System.out.println(file.getAbsolutePath());
        EasyExcel.read(file, IndexOrNameData.class, new ReadListener<IndexOrNameData>() {
            final int BATCH_COUNT = 500;
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
                System.out.println(file.getName() + "===总插入条数===" + total);
            }
        }).sheet().doRead();


    }

    public Boolean batchCreateUserDocument(List<IndexOrNameData> list) throws Exception {
//        System.out.println("需要插入==>" + index + ",数量:" + list.size());
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
