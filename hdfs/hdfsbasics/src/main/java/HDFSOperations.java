
        import org.apache.hadoop.fs.FSDataInputStream;
        import java.io.PrintWriter;
        import java.net.URI;
        import org.apache.hadoop.hdfs.DistributedFileSystem;
        import org.apache.hadoop.fs.FSDataOutputStream;
        import java.util.Scanner;
        import org.apache.hadoop.conf.Configuration;
        import org.apache.hadoop.fs.Path;



public class HDFSOperations {

    public static void main(String[] args) throws Exception{
        String pathFile ="/ggonihdfs/fileExample.txt";
        String userName="ggoni";
        String url ="hdfs://192.168.56.1:50050";
        writeExample(pathFile,userName,url);
        readExample(pathFile,userName,url);
    }

    public static void writeExample(String pathFile,String userName,String url )throws Exception{
        System.setProperty("HADOOP_USER_NAME", userName);
        final Path path = new Path(pathFile);
            try(final DistributedFileSystem dFS = new DistributedFileSystem() {
            {
                initialize(new URI(url), new Configuration());
            }
            };
            final FSDataOutputStream streamWriter = dFS.create(path);
            final PrintWriter writer = new PrintWriter(streamWriter);) {
            writer.println("hello");
            writer.println("bye");
            System.out.println("File OK");
        }
    }

    public static void readExample(String pathFile,String userName,String url )throws Exception{
        System.setProperty("HADOOP_USER_NAME", userName);
        final Path path = new Path(pathFile);
             try(final DistributedFileSystem dFS = new DistributedFileSystem() {
            {
                initialize(new URI(url), new Configuration());
            }
             };
            final FSDataInputStream streamReader = dFS.open(path);
            final Scanner scanner = new Scanner(streamReader);) {
            System.out.println("File is: ");
            while(scanner.hasNextLine()) {
                System.out.println(scanner.nextLine());
            }

        }
    }

}