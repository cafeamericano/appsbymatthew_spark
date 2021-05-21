FROM bitnami/spark:latest
COPY . ./
CMD spark-submit --class com.appsbymatthew.main target/scala-2.12/myApp.jar