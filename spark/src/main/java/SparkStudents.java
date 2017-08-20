import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
public class SparkStudents {
    public static void main(String args[]) {
		SparkConf conf = new SparkConf().setAppName("bestsWorstGrades").setMaster(args[0]);
		JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lines = sc.textFile("/ggonihdfs/schoolGrade.txt");
		System.out.println("Total students " + lines.count());
		System.out.println("Number of times the students got the worst grade: " + lines.filter(oneLine -> oneLine.contains("1")).count());
		System.out.println("Number of times the students got the best grade:"+ lines.filter(oneLine -> oneLine.contains("6")).count());
		sc.close();
    }
}
