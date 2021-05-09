<%@ taglib uri="http://java.sun.com/jsp/jstl/core" prefix="c"%>

<br />
<div class="navigation" style="margin-left: 25px;">
	<a href="#" onclick="loadHomePage(${contentPack})">Home</a> > ${selectedJobName}
</div>

<table class="jobTable" cellpadding="0" cellspacing="0" width="1150px"
	style="padding-top: 20px;">
	<tr>
		<%-- <th style="visibility: hidden;"></th>
		<th>Trace</th> --%>
		<th>Start Time</th>
		<th>End Time</th>
		<th>Load Time</th>
		<th>Time Taken</th>
		<!-- <th>Number Of Records</th> -->
	</tr>
	<c:if test="${not empty jobDetails}">
		<c:forEach items="${jobDetails}" var="jobDetail">
			<tr>
				<%-- <td style="visibility: hidden;"><input type="hidden"
					value="${jobDetail.traceFile}" id="traceFile"></input></td>
				<td><a style="color: blue;" href="#"
					onclick="loadTrace(this);"><c:out value="[TRACE]" /></a></td> --%>
				<td><c:out value="${jobDetail.startTime}" /></td>
				<td><c:out value="${jobDetail.endTime}" /></td>
				<td><c:out value="${jobDetail.loadTime}" /></td>
				<td><c:out value="${jobDetail.timeTaken}" /></td>
				<%-- <td><c:out value="${jobDetail.numberOfRecords}" /></td> --%>
			</tr>
		</c:forEach>
	</c:if>
	<c:if test="${ empty jobDetails}">
		<tr>
			<td colspan="4">No Data</td>
		</tr>
	</c:if>
	
	<div id="toggleDiv"></div>
</table>
<div style="margin-bottom: 85px"></div>