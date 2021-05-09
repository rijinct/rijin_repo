<%@ taglib uri="http://java.sun.com/jsp/jstl/core" prefix="c"%>
<div class="page_body">
	<div class="menu-tab" id="menu">
		<ul>
			<c:forEach var="i" items="${ContentPacks}">
				<li id="${i}" onclick="showRespectiveJob(this)"><c:out
						value="${i}" /></li>
			</c:forEach>
		</ul>
	</div>
	<div class="timeFrequency">
		Time frequency: <select name="timeFrequency" class="timeFrequencies"
			onchange="showJobAsPerSelectedFrequency(this)">
			<option>All</option>
			<c:forEach var="i" items="${timeFrequency}">
				<option><c:out value="${i}" /></option>
			</c:forEach>
		</select>
	</div>

	<div id="table-container"></div>

</div>
<div id="footer_container"></div>
<div class="modal" id="loading" style="display: none">
	<div class="center">
		<img alt="" src="images/loading.gif" />
	</div>
</div>