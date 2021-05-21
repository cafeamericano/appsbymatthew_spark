FROM bitnami/spark:latest
COPY . ./
CMD spark-submit --class com.matthew.alternate target/scala-2.12/myApp.jar