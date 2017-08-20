package bigtable;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import com.google.cloud.bigtable.hbase.BigtableConfiguration;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import java.io.IOException;

public class BigTableAplication {


  public static void main(String[] args) {
    byte[] TABLE = Bytes.toBytes("StudentsBestsGrades");
    byte[] COLUMN_FAMILY = Bytes.toBytes("studentname");
    byte[] COLUMN = Bytes.toBytes("name");
    final String[] NAMES =  { "Jorge Paoli", "Fernando Fernadez", "Jose Cardozo" };

    // Consult system properties to get project/instance
    String projectId = System.getProperty("bigtable.projectID");
    String instanceId = System.getProperty("bigtable.instanceID");
    if (projectId == null || instanceId == null) {
      try (Connection connection = BigtableConfiguration.connect(projectId, instanceId)) {
        Admin admin = connection.getAdmin();
        HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(TABLE));
        descriptor.addFamily(new HColumnDescriptor(COLUMN_FAMILY));
        System.out.println("Create the table " + descriptor.getNameAsString());
        admin.createTable(descriptor);
        Table table = connection.getTable(TableName.valueOf(TABLE));
        System.out.println("Load names to the table");
        for (int i = 0; i < NAMES.length; i++) {
          String rowKey = "name" + i;
          Put put = new Put(Bytes.toBytes(rowKey));
          put.addColumn(COLUMN_FAMILY, COLUMN, Bytes.toBytes(NAMES[i]));
          table.put(put);
        }
        Scan scan = new Scan();
        System.out.println("Bests students:");
        ResultScanner scanner = table.getScanner(scan);
        for (Result row : scanner) {
          byte[] valueBytes = row.getValue(COLUMN_FAMILY, COLUMN);
          System.out.println('\t' + Bytes.toString(valueBytes));
        }
        System.out.println("Deleting the table....");
        admin.disableTable(table.getName());
        admin.deleteTable(table.getName());
      } catch (IOException e) {
        System.err.println(e.getMessage());
        e.printStackTrace();
        System.exit(1);
      }
      System.exit(0);
    }
  }


}
