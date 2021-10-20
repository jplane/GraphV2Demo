
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.ser.GraphSONMessageSerializerV2d0;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONMapper;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONXModuleV2d0;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.StringJoiner;

public class Main
{
    static final String graphName = "testgraph";
    static final String nodesPath = "data/nodes.gremlin";
    static final String edgesPath = "data/edges.gremlin";

    public static void main( String[] args ) throws IOException {
        // initialize serializer
        GraphSONMapper.Builder builder = GraphSONMapper.build()
                .addCustomModule(GraphSONXModuleV2d0.build().create(false));
        GraphSONMessageSerializerV2d0 serializer = new GraphSONMessageSerializerV2d0(builder);

        // initialize cluster config
        Cluster cluster = Cluster.build()
                                 .addContactPoint("localhost")
                                 .port(8182)
                                 .serializer(serializer)
                                 .create();

        // connect to cluster
        Client client = cluster.connect();

        String traversalSourceName = String.format("%s_traversal", graphName);

        // create a new graph + schema + indices
        //  there are no strongly-typed ("bytecode") management APIs yet, so this must be pushed to server as script
        StringJoiner s = new StringJoiner(" ");

        s.add(String.format("cosmos.create('%s').throughput(Throughput.manual(400)).options([option:2]).commit();", graphName));
        s.add(String.format("cosmos.get('%s').schema().makeVertexLabel('airport').commit();", graphName));
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
        s.add(String.format("cosmos.get('%s').schema().addHashIndex(HashIndexBuilder.build('v_id', ElementType.Vertex).addKey('_id').unique()).commit();", graphName));
        s.add(String.format("cosmos.get('%s').schema().makeEdgeLabel('route').commit();", graphName));
        s.add(String.format("cosmos.get('%s').schema().addPropertyKey(PropertyKeyBuilder.build('dist').dataType(DataType.Long)).commit();", graphName));
        s.add(String.format("cosmos.get('%s').schema().addHashIndex(HashIndexBuilder.build('e_id', ElementType.Edge).addKey('_id')).commit();", graphName));

        client.submit(s.toString()).one();

        AddData(client, nodesPath, traversalSourceName);

        AddData(client, edgesPath, traversalSourceName);

        /*
        // create a "bytecode" friendly connection
        DriverRemoteConnection conn = DriverRemoteConnection.using(client, traversalSourceName);

        // create a traversal source we can use to issue queries
        GraphTraversalSource g = AnonymousTraversalSource.traversal().withRemote(conn);

        // build up a batch of vertex additions
        GraphTraversal<Vertex, Vertex> t = g
                .addV("person").property("name","Stew").property("age",23L)
                .addV("person").property("name","Stevie").property("age",28L)
                .addV("person").property("name","Patrick").property("age",18L)
                .addV("person").property("name","Patricia").property("age",41L);

        // send batch to server
        t.iterate();

        // query the server to return the vertices we just created
        List<Map<Object, Object>> results = g.V().hasLabel("person").valueMap(true).toList();

        // iterate the results
        for(Map m : results)
        {
            System.out.println(m);
        }

        // drop the graph
        g.V().drop().iterate();
        */

        // close the connection
        cluster.close();
    }

    private static void AddData(Client client, String path, String traversalSource) throws IOException {
        BufferedReader br = new BufferedReader(new FileReader(path));
        String line;
        while ((line = br.readLine()) != null) {
            line = line.replace("g.", String.format("%s.", traversalSource));
            client.submit(line).one();
        }
    }
}