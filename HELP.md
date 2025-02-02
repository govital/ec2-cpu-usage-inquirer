# Read Me First
running the jar file:

java -jar target/ec2-cpu-usages-0.0.1-SNAPSHOT.jar

example get request:

http://localhost:8080/api/v1/cpu-usage?arn=arn:aws:ec2:eu-central-1:488052111706:instance/i-0e34ad753d2ea802c&start=2020-10-01 11:00:00.000&end=2025-12-29 12:00:00.000

example request to refresh the caching data:

http://localhost:8080/api/v1/refresh-processed-data


## Objective

Create a basic REST API endpoint that retrieves the average CPU usage for a
specified EC2 instance ARN over a given timeframe. The data is provided in two
Parquet files.

## Instructions

1. ### Data Ingestion
   Use the following Parquet files loaded from a third party source (provided
   in the coding environment):
   metrics.parquet: Contains time-series data of EC2 instance metrics.
   resources.parquet: Contains metadata for EC2 instances.

2. ### API Endpoint
   Implement a single API endpoint with the following requirements:
   Accepts query parameters:
   arn: EC2 instance identifier (required)
   start: Start timestamp of the timeframe (optional)
   end: End timestamp of the timeframe (optional)
   Responds with the average CPU usage within the specified timeframe
   for the given ARN.
   If no start or end is provided, calculate the average over all available
   records for that ARN.
   If no arn is provided, calculate the average over all available ARNs.

3. ### Example Usage

4. #### Request:

   GET /api/v1/cpu-usage?arn=<EC2_ARN>&start=<start_timestamp>&e
   nd=<end_timestamp>
   Response:
   {
   "average_cpu_utilization": <average_cpu_value>
   }
