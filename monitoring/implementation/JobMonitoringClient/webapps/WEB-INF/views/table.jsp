<%@ taglib uri="http://java.sun.com/jsp/jstl/core" prefix="c"%>
<table class="jobTable" id="headerOnly" cellpadding="0" cellspacing="0"
	width="1150px">
	<tr>
		<th>Job name</th>
		<th>Boundary</th>
		<th>Source table</th>
		<th>Target table</th>
	</tr>
</table>
<div class="tableScroll">
	<table class="jobTable" cellpadding="0" cellspacing="0" width="1150px">
		<c:if test="${not empty jobDetails}">
			<c:forEach items="${jobDetails}" var="jobDetail">
				<c:choose>
					<c:when test="${jobDetail.deviation==false}">
						<tr class="greenColor" id="${jobDetail.jobName}">
							<td id="jobname" style="max-width: 300px" width="300px"><a
								onclick="loadJobDetails(this);" href="#"><c:out
										value="${jobDetail.jobName}" /></a></td>
							<td width="300px"><c:out value="${jobDetail.boundary}" /></td>
							<td id="sourceTable" width="300px"><a
								onclick="drillDown(this)" href="#"><c:out
										value="${jobDetail.sourceTable}" /></a></td>
							<td width="300px"><c:out value="${jobDetail.targetTable}" /></td>
						</tr>
					</c:when>
					<c:otherwise>

						<tr class="redColor">
							<td id="jobname" style="max-width: 300px" width="300px"><a
								onclick="loadJobDetails(this);" href="#"><c:out
										value="${jobDetail.jobName}" /></a></td>
							<td width="300px"><c:out value="${jobDetail.boundary}" /></td>
							<td id="sourceTable" width="300px"><a
								onclick="drillDown(this)" href="#"><c:out
										value="${jobDetail.sourceTable}" /></td>
							<td width="300px"><c:out value="${jobDetail.targetTable}" /></td>

						</tr>
					</c:otherwise>

				</c:choose>
			</c:forEach>

		</c:if>
		<c:if test="${ empty jobDetails}">
			<tr>
				<td colspan="4">No Data</td>
			</tr>
		</c:if>
	</table>
</div>