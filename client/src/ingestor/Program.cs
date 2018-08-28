using System;
using System.Collections;
using Nest;
using System.Threading;
using System.Linq;
using System.IO.MemoryMappedFiles;
using System.Diagnostics;
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
using System.Text;

namespace Ingestor
{
    class Program
    {
        static void Produce(ITargetBlock<List<Vertex>> target)
        {

            var nodes = DataAccess.Nodes.GetAll().GetEnumerator();
            while (nodes.MoveNext())
            {
                var buffer = new List<Vertex>();
                for (int i = 0; i < 10000; i++)
                {
                    var node = nodes.Current;
                    
                    var vertex = NodeToVertex(node);

                    var l = Edges.SourceDictionary.GetValueOrDefault(vertex.Id);
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


                    var r = Edges.TargetDictionary.GetValueOrDefault(vertex.Id);
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

        static async Task<int> ConsumeAsync(ISourceBlock<List<Vertex>> source)
        {
            // Initialize a counter to track the number of bytes that are processed.
            int nodesProcessed = 0;
            int batch = 0;
   
            ElasticClient client = new ElasticClient(new Uri(AppSettings.Current.ElasticServerUrl));         
            Console.WriteLine("Indexing documents into elasticsearch...");

            // Read from the source buffer until the source buffer has no 
            // available output data.
            while (await source.OutputAvailableAsync())
            {
                Stopwatch sw = new Stopwatch();
                sw.Start(); 

                List<Vertex> nodes = source.Receive();

                var sb = new StringBuilder();
                foreach (var item in nodes)
                {
                    sb.AppendLine(JsonConvert.SerializeObject(item,Formatting.None));
                }
                
                File.AppendAllText("/Users/chinkit/00D2D-CRC/04-BigData/stackoverflow/step2/full-graph.json",sb.ToString());
             

                // Increment the count of bytes received.
                nodesProcessed += nodes.Count;
                Console.WriteLine($"Batch {batch++}     :   Documented {nodesProcessed} ellapsed is {sw.ElapsedMilliseconds / 1000} seconds");
            }

            return nodesProcessed;
        }

        private static AppSettings _settings =  null;

        static void Main(string[] args)
        {
            LoadAppSettings();

            Stopwatch sw = new Stopwatch();
            sw.Start();

            Nodes.Load();
            Edges.Load();

            var buffer = new BufferBlock<List<Vertex>>();
            var consumer = ConsumeAsync(buffer);
            Produce(buffer);
            consumer.Wait();
            Console.WriteLine("Total processed {0} documents.", consumer.Result);

            sw.Stop();
            Console.WriteLine($"Ingest completed {sw.ElapsedMilliseconds / 1000} seconds");

            Console.ReadLine();          
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
            AppSettings.Current = settings;
        }

        private static Relationship EdgeToRelationShip(Edge e, Vertex v)
        {
            return new Relationship(e.Id, e.Meta.Label, e.Meta.Graphs, e.Data, v);
        }

        private static Vertex NodeToVertex(Node n)
        {
            return new Vertex(n.Id, n.Meta.Label, n.Meta.Graphs, n.Data);
        }
    }
}
