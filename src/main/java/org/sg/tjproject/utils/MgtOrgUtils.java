package org.sg.tjproject.utils;

import com.alibaba.excel.EasyExcel;
import com.alibaba.excel.context.AnalysisContext;
import com.alibaba.excel.read.listener.ReadListener;
import lombok.SneakyThrows;
import org.apache.commons.lang3.StringUtils;
import org.sg.tjproject.bean.ExpVO;
import org.sg.tjproject.bean.MgtOrgDTO;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class MgtOrgUtils {

    static Map<String, String> codeMap = new HashMap<String, String>() {{
        put("32401", "南京");
        put("32402", "无锡");
        put("32403", "徐州");
        put("32404", "常州");
        put("32405", "苏州");
        put("32406", "南通");
        put("32407", "连云港");
        put("32408", "淮安");
        put("32409", "盐城");
        put("32410", "扬州");
        put("32411", "镇江");
        put("32412", "泰州");
        put("32413", "宿迁");
        put("3240101", "南京供电公司市区");
        put("3240106", "南京市溧水区供电公司");
        put("3240107", "南京市高淳区供电公司");
        put("3240108", "江北公司六合供电服务中心");
        put("3240109", "江北公司浦口供电服务中心");
        put("3240110", "南京市江宁区供电公司");
        put("3240111", "江北公司新区供电服务中心");
        put("3240112", "栖霞分部");
        put("3240113", "玄武分部");
        put("3240114", "秦淮分部");
        put("3240115", "鼓楼分部");
        put("3240116", "建邺分部");
        put("3240117", "雨花分部");
    }};

    public static String getCodeName(String code) {
        if (StringUtils.isBlank(code)) {
            return null;
        }
        String name = codeMap.get(code);
        if (name == null) {
            return code;
        }
        return name;
    }

    public Map<String, MgtOrgDTO> getMgtOrgCode() {
        return readMgtOrgCodeList().stream().collect(Collectors.toMap(MgtOrgDTO::getMgtOrgCode, v -> v));
    }

    public List<MgtOrgDTO> readMgtOrgCodeList() {
        List<MgtOrgDTO> result = new ArrayList<>(2000);
        EasyExcel.read("src/main/resources/doc/组织树数据.xls", MgtOrgDTO.class, new ReadListener<MgtOrgDTO>() {
            @Override
            public void invoke(MgtOrgDTO dto, AnalysisContext analysisContext) {
                result.add(dto);
            }

            @Override
            public void doAfterAllAnalysed(AnalysisContext analysisContext) {
            }
        }).sheet().doRead();
        return result;
    }

}
