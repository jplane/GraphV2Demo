
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
    public static void main( String[] args )
    {
        GraphSONMapper.Builder builder = GraphSONMapper.build()
                .addCustomModule(GraphSONXModuleV2d0.build().create(false));
        GraphSONMessageSerializerV2d0 serializer = new GraphSONMessageSerializerV2d0(builder);

        Cluster cluster = Cluster.build()
                                 .addContactPoint("localhost")
                                 .port(8182)
                                 .serializer(serializer)
                                 .create();

        String graphName = "testgraph";

        Client client = cluster.connect();

        client.submit(String.format("cosmos.create('%s').throughput(Throughput.manual(400)).options([option:2]).commit()", graphName)).one();

        String traversalSourceName = String.format("%s_traversal", graphName);

        DriverRemoteConnection conn = DriverRemoteConnection.using(cluster, traversalSourceName);

        GraphTraversalSource g = AnonymousTraversalSource.traversal().withRemote(conn);

        GraphTraversal<Vertex, Vertex> t = g
                .addV("person").property("name","Stew").property("age",23L)
                .addV("person").property("name","Stevie").property("age",28L)
                .addV("person").property("name","Patrick").property("age",18L)
                .addV("person").property("name","Patricia").property("age",41L);

        t.iterate();

        List<Map<Object,Object>> results = g.V().hasLabel("person").valueMap(true).toList();

        for(Map m : results)
        {
            System.out.println(m);
        }

        g.V().drop().iterate();

        cluster.close();
    }
}