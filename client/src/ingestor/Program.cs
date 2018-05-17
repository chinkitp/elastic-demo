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
using System.Threading.Tasks.Dataflow;
using System.Threading.Tasks;
using System.Collections.Concurrent;
using Ingestor.Model;
using Ingestor.DataAccess;
using Microsoft.Extensions.Configuration;
using System.Reflection;

namespace Ingestor
{
    class Program
    {
        static void Produce(ITargetBlock<List<Vertex>> target)
        {
            ConcurrentDictionary<string,IEnumerable<Edge>> _sourceDictionary = new ConcurrentDictionary<string,IEnumerable<Edge>>();
            ConcurrentDictionary<string,IEnumerable<Edge>> _targedDictionary = new ConcurrentDictionary<string,IEnumerable<Edge>>();

            Console.WriteLine("Starting to load Edges"); 
            var edgeLines = File.ReadAllLines(@"/Users/chinkit/00D2D-CRC/04-BigData/stackoverflow/step1/all-edges.json");
            ConcurrentBag<Edge> edges = new ConcurrentBag<Edge>();
            Parallel.ForEach(edgeLines, edgeLine =>{
                var edge = JsonConvert.DeserializeObject<Edge>(edgeLine);
                edges.Add(edge);
            });   
     
            foreach (var item in  edges.GroupBy(e => e.Source, (a,b) => new{Source=a,Edges=b}))
            {
                 _sourceDictionary.AddOrUpdate(item.Source,item.Edges,(a,b)=>b);
            }
            foreach (var item in  edges.GroupBy(e => e.Target, (a,b) => new{Target=a,Edges=b}))
            {
                 _targedDictionary.AddOrUpdate(item.Target,item.Edges,(a,b)=>b);
            }  
            Console.WriteLine("Edges loaded");  

            var nodes = DataAccess.Nodes.GetAll().GetEnumerator();
            while (nodes.MoveNext())
            {
                var buffer = new List<Vertex>();
                for (int i = 0; i < 10000; i++)
                {
                    var node = nodes.Current;
                    
                    var vertex = NodeToVertex(node);

                    if(vertex != null)
                    {
                        var l = _sourceDictionary.GetValueOrDefault(vertex.Id);
                        if(l != null)
                        {
                            foreach (var sourceEdges in l)
                            {
                                var targetNode = DataAccess.Nodes.GetById(sourceEdges.Target);
                                if(targetNode != null)
                                {
                                    var targetVertex = NodeToVertex(targetNode);
                                    var relationship = EdgeToRelationShip(sourceEdges,targetVertex);
                                    vertex.AddOutRelationship(relationship);
                                }
                                
                            }
                        }


                        var r = _targedDictionary.GetValueOrDefault(vertex.Id);
                        if(r != null)
                        {
                            foreach (var targetEdge in r)
                            {
                                var sourceNode = DataAccess.Nodes.GetById(targetEdge.Source);
                                if(sourceNode != null)
                                {
                                    var sourceVertex = NodeToVertex(sourceNode);
                                    var relationship = EdgeToRelationShip(targetEdge,sourceVertex);
                                    vertex.AddInRelationship(relationship);
                                }
                                
                            }  
                        }
  

                        
                    }
                  
                    buffer.Add(vertex);                  
                    
                    if(!nodes.MoveNext())
                    {
                        break;
                    }
                    
                }

                target.Post(buffer);   
            } 
            

            // Set the target to the completed state to signal to the consumer
            // that no more data will be available.
            target.Complete();
        }

         // Demonstrates the consumption end of the producer and consumer pattern.
        static async Task<int> ConsumeAsync(ISourceBlock<List<Vertex>> source)
        {
            // Initialize a counter to track the number of bytes that are processed.
            int nodesProcessed = 0;
            int batch = 0;

            var elasticUri = "http://localhost:9200";           
            ElasticClient client = new ElasticClient(new Uri(elasticUri));         
            Console.WriteLine("Indexing documents into elasticsearch...");

            // Read from the source buffer until the source buffer has no 
            // available output data.
            while (await source.OutputAvailableAsync())
            {
                Stopwatch sw = new Stopwatch();
                sw.Start(); 

                List<Vertex> nodes = source.Receive();

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

                // Increment the count of bytes received.
                nodesProcessed += nodes.Count;
                Console.WriteLine($"Batch {batch++}     :   Documented {nodesProcessed} ellapsed is {sw.ElapsedMilliseconds / 1000} seconds");
            }

            return nodesProcessed;
        }


        private static void Run(Options opts)
        {

        }

        private static AppSettings _settings =  null;

        static void Main(string[] args)
        {
            LoadAppSettings();

            var n = DataAccess.Nodes.GetAll().ToList();

            // Create a BufferBlock<byte[]> object. This object serves as the 
            // target block for the producer and the source block for the consumer.
            var buffer = new BufferBlock<List<Vertex>>();

            // Start the consumer. The Consume method runs asynchronously. 
            var consumer = ConsumeAsync(buffer);

            // Post source data to the dataflow block.
            Produce(buffer);

            // Wait for the consumer to process all data.
            consumer.Wait();

            // Print the count of bytes processed to the console.
            Console.WriteLine("Total processed {0} documents.", consumer.Result);

            /* 

            var nodeLines = File.ReadAllLines(@"/Users/chinkit/00D2D-CRC/04-BigData/stackoverflow/step2/all-nodes.json");
            var edgeLines = File.ReadAllLines(@"/Users/chinkit/00D2D-CRC/04-BigData/stackoverflow/step2/all-edges.json");

            IEnumerable<Edge> edges = nodeLines.Select(JsonConvert.DeserializeObject<Edge>).ToList();

            System.Threading.Tasks.Parallel.ForEach(nodeLines, nodeLine => {
                var node = JsonConvert.DeserializeObject<Node>(nodeLine);

                var vertex = NodeToVertex(node);

                var outRelationships = edges.Where(e => e.Source == node.Id)
                        .Select(e => EdgeToRelationShip(e,vertex))
                        .ToList();

                var inRelationships = edges.Where(e => e.Target == node.Id)
                        .Select(e => EdgeToRelationShip(e,vertex))
                        .ToList();        

                vertex.AddOutRelationship(inRelationships);

            });

            // IEnumerable<Vertex> vertices = nodeLines.Select(JsonConvert.DeserializeObject<Node>)
            //     .Select(NodeToVertex)
            //     .ToList();

            // System.Threading.Tasks.Parallel.ForEach(vertices,v => {
            //     var s = v.Id;
            // });




            //var vertices = EpgVertexFactory.GetDocuments(opts.NumberOfDocuments,"questions");

 

            // Console.WriteLine("Deserilization complete");
            // Console.WriteLine($"Ellapsed is {sw.ElapsedMilliseconds / 1000} seconds");

            /* 


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

            */
            Console.WriteLine("Done");
            Console.ReadLine();


            //CommandLine.Parser.Default.ParseArguments<Options>(args)
            //    .WithParsed<Options>(opts => Run(opts))
            //    .WithNotParsed<Options>((errs) => Error(errs));           
        }

        private static void LoadAppSettings()
        {
            // Adding JSON file into IConfiguration.
            var builder = new ConfigurationBuilder()
                .SetBasePath(Path.GetDirectoryName(Assembly.GetEntryAssembly().Location))
                .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true);

            IConfigurationRoot configuration = builder.Build();

            var settings = new AppSettings();
            configuration.Bind(settings);
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
