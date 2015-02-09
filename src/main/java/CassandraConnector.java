import com.datastax.driver.core.*;

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
        cluster.getConfiguration().getPoolingOptions().setMaxConnectionsPerHost(HostDistance.LOCAL,100);


        //createTable("myks");
        getAllFromTable("myks", "users");
        //createDatabase("db2");
        //insertDataToTable("myks", "users");
        //updateTable("myks", "users");
        //deleteDataFromTable("myks", "users");


    }

    private static void createTable(String databaseName) {
        String createTableQuery = "CREATE TABLE users_3 ("
                +"user_id int PRIMARY KEY,"
                +"fname varchar,"
                +"lname varchar);";
        session = cluster.connect(databaseName);
        session.execute(createTableQuery);
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

    private static void updateTable(String databaseName, String tableName){
        session = cluster.connect(databaseName);
        session.execute("update "+tableName+" set fname='md sazzad' where user_id=9945;");

        session.close();
        cluster.close();
    }

    private static void deleteDataFromTable(String databaseName, String tableName){
        session = cluster.connect(databaseName);
        session.execute("delete from "+tableName+" where user_id=1745;");

        session.close();
        cluster.close();
    }
}







