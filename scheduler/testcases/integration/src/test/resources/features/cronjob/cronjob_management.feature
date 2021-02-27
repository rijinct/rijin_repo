Feature: Check the cronjob configuration in kubernetes

	Background: User logs in to cluster
		Given the bcmt controller
		When user logs in with configured credentials
		Then user should be logged in

	Scenario: Use the ingress path and get all cron jobs
		Given a namespace default for cronjobs
		When Get all cronjobs details from all namespaces
		Then Cron job details are retrieved successfully

	Scenario Outline: Create cronjobs using scheduler engine endpoint
		Given the ingress /se-test/schedulerengine
		And using json content type application/vnd.rijin-se-schedules-v1+json and request body <jobdefinition>
		When api post /api/manager/v1/scheduler/jobs
		Then the status code is 200

		Examples: 
		| jobdefinition                                                          |
		| [{"jobGroup": "group_1","jobName": "Perf_GROUP_1_DAY_AggregateJob"}]   |
		| [{"jobGroup": "group_2","jobName": "Entity_GROUP_2_DAY_DimensionJob"}] |
		| [{"jobGroup": "group_3","jobName": "Usage_GROUP_3_DAY_UsageJob"}]      |

	Scenario: Delete cronjobs using scheduler engine endpoint
		Given the ingress /se-test/schedulerengine
		And using json content type application/vnd.rijin-se-schedules-v1+json and request body [{"jobGroup": "jobtobedeleted_1","jobName": "Perf_JOBTODELETE_1_DAY_AggregateJob"}]
		When api post /api/manager/v1/scheduler/jobs
		Then the status code is 200
		Given the ingress /se-test/schedulerengine
		And using json content type application/vnd.rijin-se-schedules-v1+json and request body [{"jobGroup": "jobtobedeleted_1","jobName": "Perf_JOBTODELETE_1_DAY_AggregateJob"}]
		When api delete /api/manager/v1/scheduler/jobs
		Then the status code is 200
