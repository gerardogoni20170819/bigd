Main Class: /bigd/sparkStreaming/src/main/java/SparkStreaming.java

Account names of students separated by commas. Each list of names is separated by a \n.

 Example based on the official documentation of Spark streaming.

 Usage: SparkStreaming <master> <hostname> <port>
    <master> is the Spark master URL. In local mode, <master> should be 'local[n]' with n > 1.
    <hostname> and <port> of the TCP server that Spark Streaming would connect to receive data.