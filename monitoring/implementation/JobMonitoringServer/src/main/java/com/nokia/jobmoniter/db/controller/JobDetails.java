package com.nokia.jobmoniter.db.controller;

import java.math.BigDecimal;

public class JobDetails {
private String jobName;
private String startTime;
private BigDecimal processId;
private String endTime;
private String loadTime;
private String boundary;
private BigDecimal numberOfRecords;
private String upperBound;
private String lowerBound;
private String status;
private Boolean deviation;
private String timeTaken;
private String traceFile;
private String sourceTable;
private String description;
private String jobType;
public String getSourceTable() {
	return sourceTable;
}

public void setSourceTable(String sourceTable) {
	this.sourceTable = sourceTable;
}

public String getTargetTable() {
	return targetTable;
}

public void setTargetTable(String targetTable) {
	this.targetTable = targetTable;
}

private String targetTable;

public String getJobName() {
	return jobName;
}

public void setJobName(String jobName) {
	this.jobName = jobName;
}
public JobDetails(){}
public JobDetails(int int1, String string, int int2) {
	// TODO Auto-generated constructor stub
}

public String getStartTime() {
	return startTime;
}

public void setStartTime(String string) {
	this.startTime = string;
}

public String getEndTime() {
	return endTime;
}

public void setEndTime(String string) {
	this.endTime = string;
}

public BigDecimal getNumberOfRecords() {
	return numberOfRecords;
}

public void setNumberOfRecords(BigDecimal bigDecimal) {
	this.numberOfRecords = bigDecimal;
}

public String getUpperBound() {
	return upperBound;
}

public void setUpperBound(String upperBound) {
	this.upperBound = upperBound;
}

public String getLowerBound() {
	return lowerBound;
}

public void setLowerBound(String lowerBound) {
	this.lowerBound = lowerBound;
}

public Boolean getDeviation() {
	return deviation;
}

public void setDeviation(Boolean deviation) {
	this.deviation = deviation;
}

public String getTimeTaken() {
	return timeTaken;
}

public void setTimeTaken(String timeTaken) {
	this.timeTaken = timeTaken;
}

public String getBoundary() {
	return boundary;
}

public void setBoundary(String string) {
	this.boundary = string;
}

public String getLoadTime() {
	return loadTime;
}

public void setLoadTime(String loadTime) {
	this.loadTime = loadTime;
}

public String getDescription() {
	return description;
}

public void setDescription(String description) {
	this.description = description;
}

public String getStatus() {
	return status;
}

public void setStatus(String status) {
	this.status = status;
}

public String getJobType() {
	return jobType;
}

public void setJobType(String jobType) {
	this.jobType = jobType;
}
public BigDecimal getProcessId() {
	return processId;
}

public void setProcessId(BigDecimal bigDecimal) {
	this.processId = bigDecimal;
}

public String getTraceFile() {
	return traceFile;
}

public void setTraceFile(String traceFile) {
	this.traceFile = traceFile;
}



}
