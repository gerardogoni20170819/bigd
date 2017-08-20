import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.ConnectException;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.regex.Pattern;
import org.apache.spark.SparkConf;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import com.google.common.io.Closeables;
import org.apache.spark.streaming.receiver.Receiver;
import scala.Tuple2;


/*
	Based on oficial apache/spark documentation
 */


public class SparkStreaming extends Receiver<String> {
	private static final Pattern COMMA = Pattern.compile(",");
	private final static int ONE = 1;
	String host = null;
	int port = -1;


	public static void main(String[] args) throws Exception {
		SparkConf sparkConf = new SparkConf().setAppName("SparkStreaming");
		JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, new Duration(1000));
		JavaReceiverInputDStream<String> lines = ssc.receiverStream(new SparkStreaming(args[0], Integer.parseInt(args[1])));
		JavaDStream<String> studentsNames = lines.flatMap(x -> Arrays.asList(COMMA.split(x)).iterator());
		JavaPairDStream<String, Integer> nameCounts = studentsNames.mapToPair(s -> new Tuple2<>(s, ONE)).reduceByKey((i1, i2) -> i1 + i2);
		nameCounts.print();
		ssc.start();
		ssc.awaitTermination();
	}

	public SparkStreaming(String host_ , int port_) {
		super(StorageLevel.MEMORY_AND_DISK_2());
		host = host_;
		port = port_;
	}
	@Override
	public void onStart() {
		new Thread(this::receive).start();
	}
	@Override
	public void onStop() {
	}
	private void receive() {
		try {
			Socket socket = null;
			BufferedReader reader = null;
			try {
				socket = new Socket(host, port);
				reader = new BufferedReader(
						new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
				// Until stopped or connection broken continue reading
				String userInput;
				while (!isStopped() && (userInput = reader.readLine()) != null) {
					System.out.println("Received data '" + userInput + "'");
					store(userInput);
				}
			} finally {
				Closeables.close(reader,true);
				Closeables.close(socket,true);
			}
			// Restart in an attempt to connect again when server is active again
			restart("Trying to connect again");
		} catch(ConnectException ce) {
			// restart if could not connect to server
			restart("Could not connect", ce);
		} catch(Throwable t) {
			restart("Error receiving data", t);
		}
	}
}