package org.sg.tjproject.bean;

import com.alibaba.excel.annotation.ExcelProperty;
import lombok.Data;

@Data
public class ExpVOWeek {
    private String date;
    @ExcelProperty("供电单位编码")
    private String mgtOrgCode;
    @ExcelProperty("供电单位名称")
    private String mgtOrgCodeName;

    @ExcelProperty({"日均使用", "使用人数"})
    private Long syrsDay = 0L;
    @ExcelProperty({"日均使用", "使用人次"})
    private Long syrcDay = 0L;
    @ExcelProperty({"日均使用", "装拆作业辅助"})
    private Long zczyfzDay = 0L;
    @ExcelProperty({"日均使用", "工作票"})
    private Long gzpDay = 0L;
    @ExcelProperty({"日均使用", "知识问答数"})
    private Long zswdsDay = 0L;
    @ExcelProperty({"日均使用", "查看指标"})
    private Long ckzbDay = 0L;
    @ExcelProperty({"日均使用", "作业工单"})
    private Long zygdDay = 0L;

    @ExcelProperty({"累计使用", "使用人数"})
    private Long syrs = 0L;
    @ExcelProperty({"累计使用", "使用人次"})
    private Long syrc = 0L;
    @ExcelProperty({"累计使用", "装拆作业辅助"})
    private Long zczyfz = 0L;
    @ExcelProperty({"累计使用", "工作票"})
    private Long gzp = 0L;
    @ExcelProperty({"累计使用", "知识问答数"})
    private Long zswds = 0L;
    @ExcelProperty({"累计使用", "查看指标"})
    private Long ckzb = 0L;
    @ExcelProperty({"累计使用", "作业工单"})
    private Long zygd = 0L;
}
