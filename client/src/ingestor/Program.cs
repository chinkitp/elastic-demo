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

namespace Ingestor
{
    class Program
    {

// Demonstrates the production end of the producer and consumer pattern.
        static void Produce(ITargetBlock<byte[]> target)
        {
            // Create a Random object to generate random data.
            Random rand = new Random();

            // In a loop, fill a buffer with random data and
            // post the buffer to the target block.
            for (int i = 0; i < 100; i++)
            {
                // Create an array to hold random byte data.
                byte[] buffer = new byte[1024];

                // Fill the buffer with random bytes.
                rand.NextBytes(buffer);

                // Post the result to the message block.
                target.Post(buffer);
            }

            // Set the target to the completed state to signal to the consumer
            // that no more data will be available.
            target.Complete();
        }

         // Demonstrates the consumption end of the producer and consumer pattern.
        static async Task<int> ConsumeAsync(ISourceBlock<byte[]> source)
        {
            // Initialize a counter to track the number of bytes that are processed.
            int bytesProcessed = 0;

            // Read from the source buffer until the source buffer has no 
            // available output data.
            while (await source.OutputAvailableAsync())
            {
                byte[] data = source.Receive();

                // Increment the count of bytes received.
                bytesProcessed += data.Length;
            }

            return bytesProcessed;
        }


        private static void Run(Options opts)
        {

        }

        static void Main(string[] args)
        {
            // Create a BufferBlock<byte[]> object. This object serves as the 
            // target block for the producer and the source block for the consumer.
            var buffer = new BufferBlock<byte[]>();

            // Start the consumer. The Consume method runs asynchronously. 
            var consumer = ConsumeAsync(buffer);

            // Post source data to the dataflow block.
            Produce(buffer);

            // Wait for the consumer to process all data.
            consumer.Wait();

            // Print the count of bytes processed to the console.
            Console.WriteLine("Processed {0} bytes.", consumer.Result);

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
