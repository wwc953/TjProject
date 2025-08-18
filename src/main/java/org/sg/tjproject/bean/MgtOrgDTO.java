package org.sg.tjproject.bean;

import com.alibaba.excel.annotation.ExcelProperty;
import lombok.Data;

@Data
public class MgtOrgDTO {
    @ExcelProperty("供电单位编码")
    String mgtOrgCode;
    @ExcelProperty("供电单位名称")
    String mgtOrgName;
    @ExcelProperty("上级供电单位编码")
    String prMgtOrgCode;
    @ExcelProperty("上级供电单位名称")
    String prMgtOrgName;
}
