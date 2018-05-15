using System;
using System.Collections;
using Nest;
using System.Threading;
using System.Linq;
using System.IO.MemoryMappedFiles;
using System.Diagnostics;
using CommandLine;
using System.Collections.Generic;
using Elasticsearch.Net;

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
            CommandLine.Parser.Default.ParseArguments<Options>(args)
                .WithParsed<Options>(opts => Run(opts))
                .WithNotParsed<Options>((errs) => Error(errs));           
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
