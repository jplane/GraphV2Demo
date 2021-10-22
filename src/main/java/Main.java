
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.driver.ser.GraphSONMessageSerializerV2d0;
import org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONMapper;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONXModuleV2d0;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class Main
{
    static final String nodesPath = "data/nodes.gremlin";
    static final String edgesPath = "data/edges.gremlin";

    public static void main( String[] args ) {

        final String graphName = System.getenv("GRAPH_NAME");
        final String traversalSourceName = String.format("%s_traversal", graphName);

        Client client = getClient();

        try {

            final Boolean create = Boolean.parseBoolean(System.getenv("CREATE_GRAPH"));

            if (create) {
                createGraph(client, graphName, traversalSourceName);
            }

            try (
                    // create a "bytecode" friendly connection
                    DriverRemoteConnection conn = DriverRemoteConnection.using(client, traversalSourceName);

                    // create a traversal source we can use to issue queries
                    GraphTraversalSource g = AnonymousTraversalSource.traversal().withRemote(conn)
            ) {
                // query the server to return the vertices we just created
                List<Map<Object, Object>> results = g.V().hasLabel("airport").limit(50).valueMap(true).toList();

                // iterate the results
                for (Map m : results) {
                    System.out.println(m);
                }

                final Boolean drop = Boolean.parseBoolean(System.getenv("DROP_GRAPH"));

                if (drop) {
                    g.V().drop().iterate();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        finally {
            client.getCluster().close();
        }
    }

    private static Client getClient() {

        // initialize serializer
        GraphSONMapper.Builder builder = GraphSONMapper.build()
                .addCustomModule(GraphSONXModuleV2d0.build().create(false));
        GraphSONMessageSerializerV2d0 serializer = new GraphSONMessageSerializerV2d0(builder);

        final String endpoint = System.getenv("GRAPH_ENDPOINT");
        final String username = System.getenv("GRAPH_USERNAME");
        final String password = System.getenv("GRAPH_PASSWORD");

        // initialize cluster config
        Cluster cluster = Cluster.build()
                                 .addContactPoint(endpoint)
                                 .port(8182)
                                 .serializer(serializer)
                                 .enableSsl(false)
                                 .sslSkipCertValidation(true)
                                 .credentials(username, password)
                                 .create();

        // connect to cluster
        return cluster.connect();
    }

    private static void createGraph(Client client, String graphName, String traversalSourceName) throws IOException, InterruptedException {

        // create a new graph + schema + indices
        //  there are no strongly-typed ("bytecode") management APIs yet, so this must be pushed to server as script
        StringJoiner s = new StringJoiner(" ");

        s.add(String.format("cosmos.create('%s').throughput(Throughput.manual(1000)).options([option:2]).commit();", graphName));

        s.add(String.format("cosmos.get('%s').schema().makeVertexLabel('country').commit();", graphName));
        s.add(String.format("cosmos.get('%s').schema().makeVertexLabel('continent').commit();", graphName));
        s.add(String.format("cosmos.get('%s').schema().makeVertexLabel('airport').commit();", graphName));

        s.add(String.format("cosmos.get('%s').schema().makeEdgeLabel('route').commit();", graphName));
        s.add(String.format("cosmos.get('%s').schema().makeEdgeLabel('contains').commit();", graphName));

        s.add(String.format("cosmos.get('%s').schema().addPropertyKey(PropertyKeyBuilder.build('_id').dataType(DataType.String)).commit();", graphName));
        s.add(String.format("cosmos.get('%s').schema().addPropertyKey(PropertyKeyBuilder.build('type').dataType(DataType.String)).commit();", graphName));
        s.add(String.format("cosmos.get('%s').schema().addPropertyKey(PropertyKeyBuilder.build('code').dataType(DataType.String)).commit();", graphName));
        s.add(String.format("cosmos.get('%s').schema().addPropertyKey(PropertyKeyBuilder.build('icao').dataType(DataType.String)).commit();", graphName));
        s.add(String.format("cosmos.get('%s').schema().addPropertyKey(PropertyKeyBuilder.build('desc').dataType(DataType.String)).commit();", graphName));
        s.add(String.format("cosmos.get('%s').schema().addPropertyKey(PropertyKeyBuilder.build('region').dataType(DataType.String)).commit();", graphName));
        s.add(String.format("cosmos.get('%s').schema().addPropertyKey(PropertyKeyBuilder.build('runways').dataType(DataType.Long)).commit();", graphName));
        s.add(String.format("cosmos.get('%s').schema().addPropertyKey(PropertyKeyBuilder.build('longest').dataType(DataType.Long)).commit();", graphName));
        s.add(String.format("cosmos.get('%s').schema().addPropertyKey(PropertyKeyBuilder.build('elev').dataType(DataType.Long)).commit();", graphName));
        s.add(String.format("cosmos.get('%s').schema().addPropertyKey(PropertyKeyBuilder.build('country').dataType(DataType.String)).commit();", graphName));
        s.add(String.format("cosmos.get('%s').schema().addPropertyKey(PropertyKeyBuilder.build('city').dataType(DataType.String)).commit();", graphName));
        s.add(String.format("cosmos.get('%s').schema().addPropertyKey(PropertyKeyBuilder.build('lat').dataType(DataType.Double)).commit();", graphName));
        s.add(String.format("cosmos.get('%s').schema().addPropertyKey(PropertyKeyBuilder.build('lon').dataType(DataType.Double)).commit();", graphName));
        s.add(String.format("cosmos.get('%s').schema().addPropertyKey(PropertyKeyBuilder.build('dist').dataType(DataType.Long)).commit();", graphName));

        client.submit(s.toString()).one();

        addIndex(client, graphName,"_id", true);
        addIndex(client, graphName,"_id", false);

        addData(client, nodesPath, traversalSourceName);
        addData(client, edgesPath, traversalSourceName);
    }

    private static void addIndex(Client client, String graphName, String propName, Boolean isVertex) throws InterruptedException {

        String indexName = String.format("%s%s", isVertex ? "v" : "e", propName);

        StringJoiner s = new StringJoiner(" ");
        s.add(String.format("mgmt = ConfiguredGraphFactory.open('%s').openManagement();", graphName));
        s.add(String.format("prop = mgmt.getPropertyKey('%s');", propName));
        s.add(String.format("mgmt.buildIndex('%s', %s).addKey(prop).buildCompositeIndex();", indexName, isVertex ? "Vertex.class" : "Edge.class"));
        s.add("mgmt.commit();");
        client.submit(s.toString()).one();

        s = new StringJoiner(" ");
        s.add(String.format("mgmt = ConfiguredGraphFactory.open('%s').openManagement();", graphName));
        s.add(String.format("mgmt.updateIndex(mgmt.getGraphIndex('%s'), SchemaAction.REGISTER_INDEX).get();", indexName));
        s.add("mgmt.commit();");
        client.submit(s.toString()).one();

        String status = "";

        while (! status.equals("REGISTERED")) {
            TimeUnit.SECONDS.sleep(5);
            s = new StringJoiner(" ");
            s.add(String.format("mgmt = ConfiguredGraphFactory.open('%s').openManagement();", graphName));
            s.add(String.format("mgmt.getGraphIndex('%s').getIndexStatus(mgmt.getPropertyKey('%s')).toString();", indexName, propName));
            status = client.submit(s.toString()).one().getString();
        }

        s = new StringJoiner(" ");
        s.add(String.format("mgmt = ConfiguredGraphFactory.open('%s').openManagement();", graphName));
        s.add(String.format("mgmt.updateIndex(mgmt.getGraphIndex('%s'), SchemaAction.REINDEX).get();", indexName));
        s.add("mgmt.commit();");
        client.submit(s.toString()).one();

        while (! status.equals("ENABLED")) {
            TimeUnit.SECONDS.sleep(5);
            s = new StringJoiner(" ");
            s.add(String.format("mgmt = ConfiguredGraphFactory.open('%s').openManagement();", graphName));
            s.add(String.format("mgmt.getGraphIndex('%s').getIndexStatus(mgmt.getPropertyKey('%s')).toString();", indexName, propName));
            status = client.submit(s.toString()).one().getString();
        }
    }

    private static void addData(Client client, String path, String traversalSource) throws IOException {

        BufferedReader br = new BufferedReader(new FileReader(path));
        StringJoiner s = new StringJoiner("; ");
        int count = 0;
        String line;

        while ((line = br.readLine()) != null) {

            s.add(line);

            if (++count == 5) {
                client.submit(s.toString().replace("g.", String.format("%s.", traversalSource))).one();
                s = new StringJoiner("; ");
                count = 0;
            }
        }

        if (s.length() > 0) {
            client.submit(s.toString().replace("g.", String.format("%s.", traversalSource))).one();
        }
    }
}