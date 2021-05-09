<%@ taglib uri="http://java.sun.com/jsp/jstl/core" prefix="c"%>

<div class="drillDownBackGround" id="sourceTableDrillDown">

	<div class="navigation">> <span class="clickDrillDownNavigation" onclick="loadDrillDownAsPerSelectedSourceTable(this)">${sourceTable}</span></div>
	<div class="closeButton" onclick="closeDrillDown(this)" />
	<div class="popupSourceTable">
		<table class="drillDownTable" cellpadding="0" cellspacing="0"
			width="1150px">
			<tr>
				<th>Job name</th>
				<th>Boundary</th>
				<th>Source table</th>
				<th>Target table</th>
			</tr>
			<c:if test="${not empty jobDetails}">
				<c:forEach items="${jobDetails}" var="jobDetail">
					<c:choose>
						<c:when test="${jobDetail.deviation==false}">
							<tr class="greenColor" id="${jobDetail.jobName}">
								<td><a onclick="loadJobDetails(this);" href="#"><c:out
											value="${jobDetail.jobName}" /></a></td>
								<td><c:out value="${jobDetail.boundary}" /></td>
								<td><a onclick="drillDown(this)" href="#"><c:out
											value="${jobDetail.sourceTable}" /></a></td>
								<td><c:out value="${jobDetail.targetTable}" /></td>
							</tr>
						</c:when>
						<c:otherwise>

							<tr class="redColor">
								<td><a onclick="loadJobDetails(this);" href="#"><c:out
											value="${jobDetail.jobName}" /></a></td>
								<td><c:out value="${jobDetail.boundary}" /></td>
								<td><a onclick="drillDown(this)" href="#"><c:out
											value="${jobDetail.sourceTable}" /></td>
								<td><c:out value="${jobDetail.targetTable}" /></td>

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

	<br />

</div>