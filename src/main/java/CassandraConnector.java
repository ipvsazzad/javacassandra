import com.datastax.driver.core.Cluster;
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
        session = cluster.connect();
        session.execute("CREATE KEYSPACE myks WITH REPLICATION = "
            + "{ 'class' : 'SimpleStrategy', 'replication_factor': 1};"
        );
        session.execute("USE mykey");

        session.close();
        cluster.close();
    }
}
