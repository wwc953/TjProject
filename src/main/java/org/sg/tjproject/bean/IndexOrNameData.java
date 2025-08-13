package org.sg.tjproject.bean;

import com.alibaba.excel.annotation.ExcelProperty;

public class IndexOrNameData {
    @ExcelProperty("产品名称")
    private String procName;
    @ExcelProperty("产品编码")
    private String procCode;
    @ExcelProperty("工单编号")
    private String appNo;
    @ExcelProperty("供电单位")
    private String mgtOrgName;
    @ExcelProperty("供电单位编码")
    private String mgtOrgCode;
    @ExcelProperty("操作账号")
    private String handleId;
    @ExcelProperty("进入方式")
    private String viewType;
    @ExcelProperty("操作时间")
//    @DateTimeFormat("yyyy-MM-dd HH:mm:ss")
//    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss", timezone = "GMT+8")
    private String systemTime;
    @ExcelProperty("人员姓名")
    private String handleName;
    @ExcelProperty("环节名称")
    private String stepName;
    @ExcelProperty("接口名称")
    private String linkName;
    @ExcelProperty("环节编码")
    private String stepCode;
    @ExcelProperty("当前操作界面")
    private String operView;
    @ExcelProperty("操作行为")
    private String operType;
    @ExcelProperty("操作结果")
    private String operResult;
    @ExcelProperty("访问IP")
    private String operIp;

    private String type;//指令type
    private String cityCode;
    private String countryCode;

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getCityCode() {
        return cityCode;
    }

    public void setCityCode(String cityCode) {
        this.cityCode = cityCode;
    }

    public String getCountryCode() {
        return countryCode;
    }

    public void setCountryCode(String countryCode) {
        this.countryCode = countryCode;
    }

    public String getProcName() {
        return procName;
    }

    public void setProcName(String procName) {
        this.procName = procName;
    }

    public String getProcCode() {
        return procCode;
    }

    public void setProcCode(String procCode) {
        this.procCode = procCode;
    }

    public String getAppNo() {
        return appNo;
    }

    public void setAppNo(String appNo) {
        this.appNo = appNo;
    }

    public String getMgtOrgName() {
        return mgtOrgName;
    }

    public void setMgtOrgName(String mgtOrgName) {
        this.mgtOrgName = mgtOrgName;
    }

    public String getMgtOrgCode() {
        return mgtOrgCode;
    }

    public void setMgtOrgCode(String mgtOrgCode) {
        this.mgtOrgCode = mgtOrgCode;
    }

    public String getHandleId() {
        return handleId;
    }

    public void setHandleId(String handleId) {
        this.handleId = handleId;
    }

    public String getViewType() {
        return viewType;
    }

    public void setViewType(String viewType) {
        this.viewType = viewType;
    }

    public String getSystemTime() {
        return systemTime;
    }

    public void setSystemTime(String systemTime) {
        this.systemTime = systemTime;
    }

    public String getHandleName() {
        return handleName;
    }

    public void setHandleName(String handleName) {
        this.handleName = handleName;
    }

    public String getStepName() {
        return stepName;
    }

    public void setStepName(String stepName) {
        this.stepName = stepName;
    }

    public String getLinkName() {
        return linkName;
    }

    public void setLinkName(String linkName) {
        this.linkName = linkName;
    }

    public String getStepCode() {
        return stepCode;
    }

    public void setStepCode(String stepCode) {
        this.stepCode = stepCode;
    }

    public String getOperView() {
        return operView;
    }

    public void setOperView(String operView) {
        this.operView = operView;
    }

    public String getOperType() {
        return operType;
    }

    public void setOperType(String operType) {
        this.operType = operType;
    }

    public String getOperResult() {
        return operResult;
    }

    public void setOperResult(String operResult) {
        this.operResult = operResult;
    }

    public String getOperIp() {
        return operIp;
    }

    public void setOperIp(String operIp) {
        this.operIp = operIp;
    }
}
