<%@ taglib uri="http://java.sun.com/jsp/jstl/core" prefix="c"%>

<div class="drillDownBackGround" style="background-color: white">
	
<span class="navigation">Logs of job run between  ${jobStartTime} -  ${jobEndTime}	</span><div align="right"  style="margin-top: -20px" class="closeButton" onclick="closeDrillDown(this)" ></div>

	<div class="popup">
		${traceFileDisplay}
		</div>
	<br/>
	
</div>