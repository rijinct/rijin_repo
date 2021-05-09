<%@ page language="java" contentType="text/html; charset=ISO-8859-1"
	pageEncoding="ISO-8859-1"%>
<%@ taglib uri="http://java.sun.com/jsp/jstl/core" prefix="c"%>
<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
<head>
<meta http-equiv="Content-Type" content="text/html; charset=ISO-8859-1">
<link rel="shortcut icon"
	href="http://www.nokia.com/sites/all/themes/nokia/favicon.ico" />
<link rel="stylesheet" type="text/css" href="css/homepage.css" />
<script type="text/javascript" src="js/jquery-2.1.3.min.js"></script>
<script type="text/javascript" src="js/homepage.js"></script>
<title>Nokia - Job Monitor</title>
</head>
<body>
	<div id="header_container">
		<div class="header">
			<div id="logo_containter">
				<a href="home"><img src="images/nokia.png" class="logo_img"
					border="0" /></a>
			</div>
			<div class="top_header">Job Monitor</div>
		</div>
	</div>
	<!-- div id="login_container"></div-->
	<div style="margin-top: 55px"></div>
	<div id="page_containter">
		<div class="page_body">
			<div class="menu-tab" id="menu">
				<ul>
				<li id="ALL" onclick="showRespectiveJob(this)"><c:out
								value="ALL" /></li>
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
			<p>
				&nbsp;&nbsp;&nbsp;<label for="search"> <strong>Enter keyword to
						search </strong>
				</label> <input type="text" id="search" /> <label>e.g. VOLTE, SMS,
					VOICE</label>
			</p>
			<a href="#" class="tooltip"> <img src="images/export.png"
				onclick="exportCSV()" class="export_img" border="0" /> <span>
				<img class="callout" src="images/callout.gif" > <strong>Click
					to export<br />Job Table
			</strong></a>
			</span> <span id="table-container"></span>
		</div>

		<div class="modal" id="loading" style="display: none">
			<div class="center">
				<img alt="" src="images/loading.gif" />
			</div>
		</div>
	</div>
	<div id="footer_container" class="footer">
		<div class="bottom_footer">© 2016 Nokia</div>
	</div>
</body>
</html>
