using System;
using System.Collections;
using Nest;
using System.Threading;
using System.Linq;
using Newtonsoft.Json;
using System.IO.MemoryMappedFiles;
using System.Diagnostics;

namespace Ingestor
{
    class Program
    {
        static void Main(string[] args)
        {
            // var filePath = "/graph-data";
            // var fileExtensionsToRead = "json";
            // var elasticUri = "http://elastic:9200";
            var filePath = "./stackoverflow-sample/nodes";
            var fileExtensionsToRead = ".json";
            var elasticUri = "http://localhost:9200";

            Stopwatch sw = new Stopwatch();
            sw.Start();   

            var vertices = FileReader.ReadLines(filePath,fileExtensionsToRead)
                .Select(v => JsonConvert.DeserializeObject<EpgVertex>(v));

            ElasticClient client = new ElasticClient(new Uri(elasticUri));
            
            Console.WriteLine("Indexing documents into elasticsearch...");
            var waitHandle = new CountdownEvent(1);

            var bulkAll = client.BulkAll(vertices, b => b
                .Index("imdb-v4")
                .BackOffRetries(2)
                .BackOffTime("30s")
                .RefreshOnCompleted(true)
                .MaxDegreeOfParallelism(2)
                .Size(10000)
            );

            bulkAll.Subscribe(new BulkAllObserver(
                onNext: (b) => { Console.Write(".");},
                onError: (e) => { Console.WriteLine(e.Message); },
                onCompleted: () =>  waitHandle.Signal()
            ));
            
            waitHandle.Wait();
            Console.WriteLine("Done.");

            sw.Stop();
            Console.WriteLine($"Ellapsed ms is {sw.ElapsedMilliseconds}");

            Console.ReadLine();
            
           
        }
            
    }
}
