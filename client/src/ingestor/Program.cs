using System;
using System.Collections;
using Nest;
using System.Threading;
using System.Linq;
using System.IO.MemoryMappedFiles;
using System.Diagnostics;
using CommandLine;
using System.Collections.Generic;
using System.IO;
using Elasticsearch.Net;
using Newtonsoft.Json;

namespace Ingestor
{
    class Program
    {
        private static void Run(Options opts)
        {
            var elasticUri = "http://localhost:9200";

            Stopwatch sw = new Stopwatch();
            sw.Start();   

            var vertices = EpgVertexFactory.GetDocuments(opts.NumberOfDocuments,"questions");

 
            ElasticClient client = new ElasticClient(new Uri(elasticUri));

           
            Console.WriteLine("Indexing documents into elasticsearch...");
            var waitHandle = new CountdownEvent(1);

        

            var bulkAll = client.BulkAll(vertices, b => b
                .Refresh(Refresh.False)
                .Index("imdb-v4")
                .BackOffRetries(2)
                .BackOffTime("30s")
                .RefreshOnCompleted(false)
                .MaxDegreeOfParallelism(4)
                .Size(1000)              
            );

            bulkAll.Subscribe(new BulkAllObserver(
                onNext: (r) => { Console.WriteLine("Inserted {0}", r.Page * 800);},
                onError: (e) => { Console.WriteLine(e.Message); },
                onCompleted: () =>  waitHandle.Signal()
            ));
            
            waitHandle.Wait();
            Console.WriteLine("Done.");

            sw.Stop();
            Console.WriteLine($"Ellapsed is {sw.ElapsedMilliseconds / 1000} seconds");

            Console.ReadLine();
        }

        static void Main(string[] args)
        {
            var nodeLines = File.ReadAllLines(@".\..\..\..\..\..\..\stackoverflow-sample\all-nodes.json");
            var edgeLines = File.ReadAllLines(@".\..\..\..\..\..\..\stackoverflow-sample\all-edges.json");

            IEnumerable<Vertex> vertices = nodeLines.Select(JsonConvert.DeserializeObject<Node>)
                .Select(NodeToVertex)
                .ToList();


            foreach (var edgeLine in edgeLines)
            {
                Edge edge = JsonConvert.DeserializeObject<Edge>(edgeLine);
                var sourceVertex = vertices.Single(v => v.Id == edge.Source);
                var targetVertex = vertices.Single(v => v.Id == edge.Target);

                var forSource = EdgeToRelationShip(edge, targetVertex);
                var forTarget = EdgeToRelationShip(edge, sourceVertex);
                
                sourceVertex.AddOutRelationship(forSource);
                targetVertex.AddInRelationship(forTarget);
            }



            Console.WriteLine("Done");
            Console.ReadLine();


            //CommandLine.Parser.Default.ParseArguments<Options>(args)
            //    .WithParsed<Options>(opts => Run(opts))
            //    .WithNotParsed<Options>((errs) => Error(errs));           
        }

        private static Relationship EdgeToRelationShip(Edge e, Vertex v)
        {
            return new Relationship(e.Id, e.Meta.Label, e.Meta.Graphs, e.Data, v);
        }

        private static Vertex NodeToVertex(Node n)
        {
            return new Vertex(n.Id, n.Meta.Label, n.Meta.Graphs, n.Data);
        }

        private static void Error(IEnumerable<CommandLine.Error> errs)
        {
            foreach (var err in errs)
            {
                Console.WriteLine(err);
            }
        }
    }
}
