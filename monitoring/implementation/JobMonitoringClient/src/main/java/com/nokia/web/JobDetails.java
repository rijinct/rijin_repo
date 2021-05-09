package com.nokia.web;

import java.math.BigDecimal;
import java.util.Date;

public class JobDetails {
	private String jobName;
	private Date startTime;

	private Date endTime;
	private String boundary;
	private BigDecimal numberOfRecords;
	private String upperBound;
	private String lowerBound;
	private Boolean deviation;
	private BigDecimal timeTaken;
	private String sourceTable;

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

	public JobDetails() {
	}

	public JobDetails(int int1, String string, int int2) {
		// TODO Auto-generated constructor stub
	}

	public Date getStartTime() {
		return startTime;
	}

	public void setStartTime(Date startTime) {
		this.startTime = startTime;
	}

	public Date getEndTime() {
		return endTime;
	}

	public void setEndTime(Date date) {
		this.endTime = date;
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

	public BigDecimal getTimeTaken() {
		return timeTaken;
	}

	public void setTimeTaken(BigDecimal bigDecimal) {
		this.timeTaken = bigDecimal;
	}

	public String getBoundary() {
		return boundary;
	}

	public void setBoundary(String string) {
		this.boundary = string;
	}
	
	
	@Override
	public String toString() {
		return jobName+"\n";
	}

}
