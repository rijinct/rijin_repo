apiVersion: batch/v1beta1
kind: CronJob
metadata:
  labels:
spec:
  suspend: true
  successfulJobsHistoryLimit: 0
  failedJobsHistoryLimit: 1
  jobTemplate:
    spec:
      backoffLimit: 1
      template:
        spec:
            resources:
              requests:
                cpu: "0.05"
                memory: "5Mi"
              limits:
                cpu: "0.1"
                memory: "20Mi"
            command: ["/bin/sh", "-c"]
            - name: JOB_NAME
          restartPolicy: Never