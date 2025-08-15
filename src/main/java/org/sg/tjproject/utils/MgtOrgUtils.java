package org.sg.tjproject.utils;

import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
import java.util.Map;

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
}
