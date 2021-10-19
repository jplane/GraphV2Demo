
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.driver.ser.GraphSONMessageSerializerV2d0;
import org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONMapper;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONXModuleV2d0;

import java.util.List;
import java.util.Map;

public class Main
{
    static final String graphName = "testgraph";

    public static void main( String[] args )
    {
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

        // create a new graph
        //  there are no strongly-typed ("bytecode") management APIs yet, so this must be pushed to server as script
        client.submit(String.format("cosmos.create('%s').throughput(Throughput.manual(400)).options([option:2]).commit()", graphName)).one();

        String traversalSourceName = String.format("%s_traversal", graphName);

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

        // close the connection
        cluster.close();
    }
}