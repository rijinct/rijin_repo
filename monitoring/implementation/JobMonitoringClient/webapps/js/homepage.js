$(document).ready(function() {
	$('.menu-tab li:nth-child(1)').addClass('selected');
	$('.menu-tab li:nth-child(1)').click();
	$(".drillDownTable tr th").css("background", "#66a");
	$('#search').keyup(function()
			{
				searchTable($(this).val());
			});
	// loadOptions();
	manageTabs();
});

$(window).scroll(function() {
	/*
	 * if ($(this).scrollTop() > 135) {
	 * $('#header_container').addClass('fixed'); } else {
	 * $('#header_container').removeClass('fixed'); }
	 */
});
function searchTable(inputVal)
{
	var table = $('.jobTable');
	table.find('tr').each(function(index, row)
	{
		var allCells = $(row).find('td:nth-child(1)');
		if(allCells.length > 0)
		{
			var found = false;
			allCells.each(function(index, td)
			{
				var regExp = new RegExp(inputVal, 'i');
				if(regExp.test($(td).text()))
				{
					found = true;
					return false;
				}
			});
			if(found == true)$(row).show();else $(row).hide();
		}
	});
}
function showRespectiveJob(event) {
	var e = event;
	$('.tooltip').show();
	var frequency = $('.timeFrequency :selected').text();
	if (e.id=="ALL_JOBS"){
		$(".timeFrequency").hide();
	}   else{
	 	$(".timeFrequency").show();
    }
	
	$
			.ajax(
					{
						type : 'GET',
						url : 'jobs',
						contentType : "application/json",
						data : {
							'contentPack' : e.id,
							'frequency' : frequency
						},
						cache : true,
						error : function(jqXHR, textStatus) {
							//
						},
						success : function(data) {
							$('#table-container').html(data);
							$(".tableScroll .jobTable tr td").css("max-width",
									"400px");
							$("#headerOnly th:nth-child(1)")
									.attr(
											'width',
											$(
													".tableScroll .jobTable tr:nth-child(1) td:nth-child(1)")
													.width());
							$("#headerOnly th:nth-child(2)")
									.attr(
											'width',
											$(
													".tableScroll .jobTable tr:nth-child(1) td:nth-child(2)")
													.width());
							$("#headerOnly th:nth-child(3)")
									.attr(
											'width',
											$(
													".tableScroll .jobTable tr:nth-child(1) td:nth-child(3)")
													.width());
							$("#headerOnly th:nth-child(4)")
									.attr(
											'width',
											$(
													".tableScroll .jobTable tr:nth-child(1) td:nth-child(4)")
													.width());
						},
						beforeSend : function() {
							// show gif here, eg:
							$("#loading").show();
						},
						complete : function() {
							// hide gif here, eg:
							$("#loading").hide();
						}
					}).done();
}

var totalPadding = 0;

function drillDown(event) {
	var e = event;
	var navigationPath = $(".navigation").text();
	$('.drillDownTable-container').remove();
	if (event.parentElement.parentElement.parentElement.parentElement.className == "jobTable") {
		totalPadding = $("." + event.parentElement.parentElement.className)
				.height();
	} else if (event.parentElement.parentElement.parentElement.parentElement.className == "drillDownTable") {
		$(".navigation").empty();
		totalPadding = totalPadding + event.parentNode.parentNode.offsetTop;
	}

	$('body .tableScroll table')
			.append(
					'<div class="drillDownTable-container" style="top:'
							+ (totalPadding + 230)
							+ 'px;"><div class="drillDownCenter"><img alt="" src="images/loading.gif"/></div></div>');
	
	$
			.ajax(
					{
						type : 'GET',
						url : 'drillDown',
						contentType : "application/json",
						data : {
							'sourceTable' : e.text
						},
						cache : true,
						error : function(jqXHR, textStatus) {
							//
						},
						success : function(data) {
							$('.drillDownTable-container').html(data);
							var finalNavigationPath = "";
							var navigationPathSplit = navigationPath
									.split(' > ');
							for (var i = 1, l = navigationPathSplit.length; i < l; i++) {
								finalNavigationPath = finalNavigationPath
										+ " > <span class=\"clickDrillDownNavigation\" onclick=\"loadDrillDownAsPerSelectedSourceTable(this)\">"
										+ navigationPathSplit[i] + "</span>";
							}
							$(".navigation").prepend(finalNavigationPath + " ");
						}
					}).done();

	$('html, body').animate({
		scrollTop : totalPadding
	}, 800);
}
function chartDrillDown(event) {
	var e = event;
	var navigationPath = $(".navigation").text();
	$('.drillDownTable-container').remove();
	if (event.parentElement.parentElement.parentElement.parentElement.className == "jobTable") {
		totalPadding = $("." + event.parentElement.parentElement.className)
				.height();
	} else if (event.parentElement.parentElement.parentElement.parentElement.className == "drillDownTable") {
		$(".navigation").empty();
		totalPadding = totalPadding + event.parentNode.parentNode.offsetTop;
	}

	$('body .tableScroll table')
			.append(
					'<div class="drillDownTable-container" style="top:'
							+ (totalPadding + 230)
							+ 'px;"><div class="drillDownCenter"><img alt="" src="images/loading.gif"/></div></div>');
	var jobName=$('#jobname a').html();
	var sourceTable=$('#sourceTable a').html();
	$
			.ajax(
					{
						type : 'GET',
						url : 'drillDownForChart',
						contentType : "application/json",
						data : {
							'sourceTable' : sourceTable,
							'jobName':jobName
						},
						cache : true,
						error : function(jqXHR, textStatus) {
							//
						},
						success : function(data) {
							$('.drillDownTable-container').html(data);
							var finalNavigationPath = "";
							var navigationPathSplit = navigationPath
									.split(' > ');
							for (var i = 1, l = navigationPathSplit.length; i < l; i++) {
								finalNavigationPath = finalNavigationPath
										+ " > <span class=\"clickDrillDownNavigation\" onclick=\"loadDrillDownAsPerSelectedSourceTable(this)\">"
										+ navigationPathSplit[i] + "</span>";
							}
							$(".navigation").prepend(finalNavigationPath + " ");
						}
					}).done();

	$('html, body').animate({
		scrollTop : totalPadding
	}, 800);
}

function loadDrillDownAsPerSelectedSourceTable(event) {
	var e = event;
	var navigationPath = $(".navigation").text();
	$('.drillDownTable-container').remove();

	$('body .tableScroll table')
			.append(
					'<div class="drillDownTable-container" style="top:'
							+ (totalPadding + 230)
							+ 'px;"><div class="drillDownCenter"><img alt="" src="images/loading.gif"/></div></div>');
	$
			.ajax(
					{
						type : 'GET',
						url : 'drillDown',
						contentType : "application/json",
						data : {
							'sourceTable' : e.textContent
						},
						cache : true,
						error : function(jqXHR, textStatus) {
							//
						},
						success : function(data) {
							$('.drillDownTable-container').html(data);
							var finalNavigationPath = "";
							var navigationPathSplit = navigationPath
									.split(' > ');
							for (var i = 1, l = navigationPathSplit.length; i < l; i++) {
								if (navigationPathSplit[i] == e.textContent) {
									break;
								}
								finalNavigationPath = finalNavigationPath
										+ " > <span class=\"clickDrillDownNavigation\" onclick=\"loadDrillDownAsPerSelectedSourceTable(this)\">"
										+ navigationPathSplit[i] + "</span>";

							}
							$(".navigation").prepend(finalNavigationPath + " ");
						}
					}).done();

	$('html, body').animate({
		scrollTop : totalPadding
	}, 800);
}

function showJobAsPerSelectedFrequency(event) {
	var frequency = $('.timeFrequency :selected').text();
	var contentPack = $(".menu-tab li.selected").text();
	$('.tooltip').show();
	$
			.ajax(
					{
						type : 'GET',
						url : 'jobs',
						contentType : "application/json",
						data : {
							'contentPack' : contentPack,
							'frequency' : frequency
						},
						cache : true,
						error : function(jqXHR, textStatus) {
							//
						},
						success : function(data) {
							$('#table-container').html(data);
							$(".tableScroll .jobTable tr td").css("max-width",
									"400px");
							$("#headerOnly th:nth-child(1)")
									.attr(
											'width',
											$(
													".tableScroll .jobTable tr:nth-child(1) td:nth-child(1)")
													.width());
							$("#headerOnly th:nth-child(2)")
									.attr(
											'width',
											$(
													".tableScroll .jobTable tr:nth-child(1) td:nth-child(2)")
													.width());
							$("#headerOnly th:nth-child(3)")
									.attr(
											'width',
											$(
													".tableScroll .jobTable tr:nth-child(1) td:nth-child(3)")
													.width());
							$("#headerOnly th:nth-child(4)")
									.attr(
											'width',
											$(
													".tableScroll .jobTable tr:nth-child(1) td:nth-child(4)")
													.width());
						},
						beforeSend : function() {
							// show gif here, eg:
							$("#loading").show();
						},
						complete : function() {
							// hide gif here, eg:
							$("#loading").hide();
						}
					}).done();

}

function closeDrillDown(event) {
	totalPadding = 0;
	event.parentElement.parentElement.remove(event.parentNode.parentNode);
}

function loadTrace(event) {
	traceFilePath = $(event).parent().prev().find("#traceFile").val();
	traceFilePath = traceFilePath + ","
			+ $(event).parent().next().next().text() + ","
			+ $(event).parent().next().text();
	$('body table')
			.append(
					'<div class="drillDownTable-trace" style="top:'
							+ (event.parentNode.parentNode.offsetTop + 190)
							+ 'px;"><div class="drillDownCenter"><img alt="" src="images/loading.gif"/></div></div>');
	$.ajax({
		type : 'GET',
		url : 'traceFileDisplay',
		contentType : "application/json",
		data : {
			'processId' : traceFilePath
		},
		cache : true,
		error : function(jqXHR, textStatus) {
			//
		},
		success : function(data) {
			$('.drillDownTable-trace').html(data);
		},
	}).done();
	$('html, body').animate({
		scrollTop : event.parentNode.parentNode.offsetTop
	}, 800);
}

function loadJobDetails(event) {

	$('.tooltip').hide();
	var contentPack = $('.menu-tab ul .selected').text();
	var frequency = $('.timeFrequency option:selected').val();
	var jobName = (event.text);
	$.ajax({
		type : 'GET',
		url : 'jobTrace',
		contentType : "application/json",
		data : {
			'jobName' : jobName,
			'contentPack' : contentPack,
			'frequency' : frequency
		},
		cache : true,
		error : function(jqXHR, textStatus) {
			//
		},
		success : function(data) {
			$('#table-container').html(data);
		},
		beforeSend : function() {
			$(".timeFrequency").hide();
			$("#loading").show();
		},
		complete : function() {
			// hide gif here, eg:
			$("#loading").hide();
		}
	}).done();
}
function populateOptions(data) {
	var rows = $('.csv-table tr');
	rows = $(rows.get(rows.length - 1));
	rows.find('#fieldName option').remove();
	rows.find('#fieldName').append('<option>None</option>');
	$.each(data, function(index, value) {
		rows.find('#fieldName').append('<option>' + value + '</option>');
	});
}
function changed(event) {
	var selectedRow = $(event.parentNode.parentNode);
	var fieldName = selectedRow.find('#fieldName option:selected').valueOf(0)
			.text();
	if (fieldName != 'None') {
		$.ajax({
			type : 'GET',
			url : 'fields',
			contentType : "application/json",
			dataType : 'json',
			data : {
				'fieldName' : fieldName
			},
			cache : true,
			error : function(jqXHR, textStatus) {
				console.log(jqXHR);
				console.log(textStatus);
			},
			success : function(data) {
				populateOtherFields(data, selectedRow);
			}
		}).done();
	} else {
		for (var int = 1; int <= 6; int++) {
			selectedRow.find('input[name=field' + (int) + ']').val("");
			selectedRow.find('input[name=field' + (int) + ']').attr('disabled',
					'disabled');
		}
	}
}

function populateOtherFields(data, selectedRow) {
	for (var int = 1; int <= 6; int++) {
		selectedRow.find('input[name=field' + (int) + ']').removeAttr(
				'disabled');
		selectedRow.find('input[name=field' + (int) + ']').val("");
	}

	$.each(data, function(index, value) {
		selectedRow.find('input[name=field' + (index + 1) + ']').val(value);
	});
}

function manageTabs() {
	$('#menu li').click(function(event) {
		$('#menu li').removeClass('selected');
		$(event.target).addClass('selected');
		var text = event.target.innerHTML;
		if (text == 'Create CSV') {
			$('#create-csv').show();
			$('#add-row').hide();
		} else {
			$('#create-csv').hide();
			$('#add-row').show();
		}
	});
	$('#create-csv-msg').empty();
}
function addRow() {
	$('#add-rows').click(function() {
		var inputs = $('#csv-table-dummy').html();
		$('.csv-table').append(inputs);
		loadOptions();
		$('#create-csv-msg').empty();
	});
}
function deleteRow(event) {
	var r = confirm("Are you sure you want to delete this row?");
	if (r == true) {
		event.parentNode.parentNode.parentNode.remove();
		$('#create-csv-msg').empty();
	}
}

function addNewDropDown() {
	var key = $('#key').val().trim();
	if (key != null && key != '') {
		$
				.ajax(
						{
							type : 'GET',
							url : 'add',
							contentType : "application/json",
							data : {
								'key' : key,
								'field1' : $('#addfield1').val(),
								'field2' : $('#addfield2').val(),
								'field3' : $('#addfield3').val(),
								'field4' : $('#addfield4').val(),
								'field5' : $('#addfield5').val(),
								'field6' : $('#addfield6').val()
							},
							cache : true,
							error : function(jqXHR, textStatus) {
								console.log(jqXHR);
								console.log(textStatus);
							},
							success : function() {
								$('#menu>ul>li:nth-child(1)').click();
								$('#create-csv-msg')
										.html(
												'<p style="color:green;">New option added successfully..</p>');
							}
						}).done();
	} else
		alert("Field name cannot be blank !");
}

function exportCSV() {

	var tableRows = $('.tableScroll tbody tr');
	var data = new Array(tableRows.size() + 1);
	data[0] = new Array(6);
	data[0][1] = "Job Name";
	data[0][2] = "Boundary";
	data[0][3] = "Source Table";
	data[0][4] = "TargetTable";
	data[0][5] = "Status";
	$.each(tableRows, function(index, row) {
		data[index + 1] = new Array(5);
		for (var int = 1; int <= 5; int++) {
			if (int == 5) {

				if ($(row).attr('class') == "greenColor") {
					data[index + 1][int] = "Successfull";
				} else if ($(row).attr('class') == "redColor") {
					data[index + 1][int] = "Unsuccessfull";
				}
			} else {
				data[index + 1][int] = $(row).find(
						'td:nth-child(' + (int) + ')').text();
			}
		}
	});

	var csvContent = "data:text/csv;charset=utf-8,";
	data.forEach(function(infoArray, index) {
		dataString = infoArray.join(",");
		csvContent += index < data.length ? dataString + "\n" : dataString;
	});
	var encodedUri = encodeURI(csvContent);
	var link = document.createElement("a");
	link.setAttribute("href", encodedUri);
	link.setAttribute("download", "jobStatus_NOKIA.csv");
	document.body.appendChild(link);
	link.click();
}

function loadHomePage(contentPackName) {
	$(".timeFrequency").show();
	$('.tooltip').show();
	var contentPack = contentPackName.textContent;
	var frequency = $('.timeFrequency :selected').text();
	$(".timeFrequency").show();
	$
			.ajax(
					{
						type : 'GET',
						url : 'jobs',
						contentType : "application/json",
						data : {
							'contentPack' : contentPack,
							'frequency' : frequency
						},
						cache : true,
						error : function(jqXHR, textStatus) {
							//
						},
						success : function(data) {
							$('#table-container').html(data);
							$(".tableScroll .jobTable tr td").css("max-width",
									"400px");
							$("#headerOnly th:nth-child(1)")
									.attr(
											'width',
											$(
													".tableScroll .jobTable tr:nth-child(1) td:nth-child(1)")
													.width());
							$("#headerOnly th:nth-child(2)")
									.attr(
											'width',
											$(
													".tableScroll .jobTable tr:nth-child(1) td:nth-child(2)")
													.width());
							$("#headerOnly th:nth-child(3)")
									.attr(
											'width',
											$(
													".tableScroll .jobTable tr:nth-child(1) td:nth-child(3)")
													.width());
							$("#headerOnly th:nth-child(4)")
									.attr(
											'width',
											$(
													".tableScroll .jobTable tr:nth-child(1) td:nth-child(4)")
													.width());
						},
						beforeSend : function() {
							// show gif here, eg:
							$("#loading").show();
						},
						complete : function() {
							// hide gif here, eg:
							$("#loading").hide();
						}
					}).done();
}