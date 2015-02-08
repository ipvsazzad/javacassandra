import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

/**
 * Created by Sazzad on 08-Feb-15.
 */
public class CassandraConnector {
    private static Cluster cluster;
    private static Session session;

    public static Cluster connect(String node){
        return Cluster.builder().addContactPoint(node).build();
    }

    public static void main(String[] args) {
        cluster = connect("localhost");





        //getAllFromTable("myks", "users");
        //createDatabase("db2");
        //insertDataToTable("myks", "users");


    }

    private static void createDatabase(String databaseName) {
        session = cluster.connect();
        session.execute("CREATE KEYSPACE "+databaseName+" WITH REPLICATION = "
            + "{ 'class' : 'SimpleStrategy', 'replication_factor': 1};"
        );
        session.execute("USE "+databaseName);
        session.close();
        cluster.close();
    }

    private static void getAllFromTable(String databaseName, String tableName){
        session = cluster.connect(databaseName);
        ResultSet results = session.execute("SELECT * FROM "+tableName);
        for (Row row : results) {
            System.out.println(row.getString("lname"));
        }
        session.close();
        cluster.close();
    }

    private static void insertDataToTable(String databaseName, String tableName){
        session = cluster.connect(databaseName);
        session.execute("INSERT INTO "+tableName+" (user_id,  fname, lname) VALUES (9945, 'sazzad', 'Islam');");

        session.close();
        cluster.close();
    }
}







