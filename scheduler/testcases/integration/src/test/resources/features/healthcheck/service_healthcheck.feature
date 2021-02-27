Feature: Health check of the service

	Background: User logs in to cluster
		Given the bcmt controller
		When user logs in with configured credentials
		Then user should be logged in

	Scenario: Check if pod is running and is ready to serve requests
		Given a namespace default for pods
		When Get pod with name se-test-schedulerengine
		Then pod must be running and ready with startup time less than 60 seconds
		And pod has http liveness probe /application/health at port http
		And pod has http readiness probe /application/health at port http

	Scenario: Check if service exists
		Given a namespace default for services
		When Get service with name se-test-schedulerengine
		Then service has port with name http and value 80
		And service has selector app=se-test-schedulerengine
		And service has annotations prometheus.io/path=/application/prometheus,prometheus.io/scrape=true

	Scenario: Check ingress configuration
		Given a namespace default for ingresses
		When Get ingress with name se-test-schedulerengine
		Then ingress should exist
		And ingress should have path /se-test/schedulerengine and backend servicename se-test-schedulerengine and serviceport 80

	Scenario: Check existence of configmap
		Given a namespace default for configmaps
		When Get configmap with name se-test-schedulerengine-configuration
		Then configmap should exist

	Scenario: Check the volume mapping for configmaps
		Given a namespace default for pods
		When Get pod with name se-test-schedulerengine
		Then configmap must be mapped to volume
		| apaas-cdlk-hdfs                            | hadoop-client-properties |
		| apaas-cdlk-access                          | apaas-cdlk-access        |
		| ca4ci-ext-endpoints                        | ca4ci-ext-endpoints      |
		| se-test-schedulerengine-configuration  | query-settings           |

	Scenario: Check the volume mapping for GFS
		Given a namespace default for pods
		When Get pod with name se-test-schedulerengine
		Then gfs must be mapped to volume
		| ca4ci-glusterfs | glusterca4ci |
