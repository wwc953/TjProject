package org.sg.tjproject.bean;

import com.alibaba.excel.annotation.ExcelProperty;
import lombok.Data;

@Data
public class ExpVO {
    @ExcelProperty("供电单位编码")
    private String mgtOrgCode;
    @ExcelProperty("供电单位名称")
    private String mgtOrgCodeName;

    @ExcelProperty("使用人数")
    private Long syrs = 0L;
    @ExcelProperty("使用人次")
    private Long syrc = 0L;
    @ExcelProperty("装拆作业辅助")
    private Long zczyfz = 0L;
    @ExcelProperty("工作票")
    private Long gzp = 0L;
    @ExcelProperty("知识问答数")
    private Long zswds = 0L;
    @ExcelProperty("查看指标")
    private Long ckzb = 0L;
    @ExcelProperty("作业工单")
    private Long zygd = 0L;
    @ExcelProperty("装拆智能助手处理量")
    private Long znzscll = 0L;
}
