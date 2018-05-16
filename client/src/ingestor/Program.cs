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

        }

        static void Main(string[] args)
        {
            //var nodeLines = File.ReadAllLines(@"/Users/chinkit/00D2D-CRC/04-BigData/stackoverflow/step2/all-nodes.json");
            //var edgeLines = File.ReadAllLines(@"/Users/chinkit/00D2D-CRC/04-BigData/stackoverflow/step2/all-edges.json")

            // IEnumerable<Vertex> vertices = nodeLines.Select(JsonConvert.DeserializeObject<Node>)
            //     .Select(NodeToVertex);
            //     //.ToList();

            // System.Threading.Tasks.Parallel.ForEach(vertices,v => {
            //     var s = v.Id;
            // });


            //var vertices = EpgVertexFactory.GetDocuments(opts.NumberOfDocuments,"questions");

 

            // Console.WriteLine("Deserilization complete");
            // Console.WriteLine($"Ellapsed is {sw.ElapsedMilliseconds / 1000} seconds");

            // IEnumerable<Edge> edges = nodeLines.Select(JsonConvert.DeserializeObject<Edge>)
            //     .ToList();

            var elasticUri = "http://localhost:9200";           
            ElasticClient client = new ElasticClient(new Uri(elasticUri));         
            Console.WriteLine("Indexing documents into elasticsearch...");

            int noOfBatches = 30;
            int noOfDocuments = 100000;

            for (int i = 1; i <= noOfBatches; i++)
            {
                Stopwatch sw = new Stopwatch();
                sw.Start();  

                IEnumerable<Node> nodes = Enumerable
                    .Repeat(0, noOfDocuments)
                    .Select((s)=>new Node
                        (
                                EpgVertexFactory.GetGuid(),
                                new Meta("answer", new[]{"a"}),
                                new Dictionary<string,object>
                                {
                                    {"Description",WordList.GetRandomWords(25)},
                                    {"Long Description",WordList.GetRandomWords(75)},
                                    {"Date",DateTime.Now.ToShortDateString()},
                                    {"Rank",50}
                                }
                        ));
                
                var waitHandle = new CountdownEvent(1);    

                var bulkAll = client.BulkAll(nodes, b => b
                    .Refresh(Refresh.False)
                    .Index("sx-v3")
                    .BackOffRetries(2)
                    .BackOffTime("30s")
                    .RefreshOnCompleted(false)
                    .MaxDegreeOfParallelism(4)
                    .Size(800)              
                );

                bulkAll.Subscribe(new BulkAllObserver(
                    onNext: (r) => { },
                    onError: (e) => { Console.WriteLine(e.Message); },
                    onCompleted: () =>  waitHandle.Signal()
                ));
                
                waitHandle.Wait();

                sw.Stop();
                Console.WriteLine(nodes.First().Id);
                Console.WriteLine($"Batch {i} ellapsed is {sw.ElapsedMilliseconds / 1000} seconds");
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
