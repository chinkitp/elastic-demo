using System;
using System.Collections.Generic;
using System.Collections;
using Nest;
using System.Threading;
using System.Linq;
using Newtonsoft.Json;


namespace ingestor
{
    public class EpgProperties : Dictionary<string, object>
    {

    }

    public class EpgVertex
    {
        public string Id {get;}
        public string Label {get;}
        public EpgProperties EpgProperties {get;}

        public EpgVertex(string id, string label, EpgProperties epgProperties)
        {
            Id = id;
            Label = label;
            EpgProperties = epgProperties;
        }
    }


    class Program
    {
        static void Main(string[] args)
        {
            ElasticClient client = new ElasticClient(new Uri("http://localhost:9200"));
            

            IEnumerable<string> vertices = System.IO.File
            .ReadAllLines("/Users/chinkit/00D2D-CRC/01-Projects/data61/github/stellar-search/search/src/test/resources/au/csiro/data61/stellar/search/epg/imdb-flat/imdb-vertices.json");

            var docs = vertices.Select(v => JsonConvert.DeserializeObject(v));

            // var result = client.IndexMany(docs,"imdb-v3");
            // Console.WriteLine(result.DebugInformation);
            Console.WriteLine("Indexing documents into elasticsearch...");
            var waitHandle = new CountdownEvent(1);



            var bulkAll = client.BulkAll(docs, b => b
                .Index("imdb-v4")
                //.Type("my-console")
                .BackOffRetries(2)
                .BackOffTime("30s")
                .RefreshOnCompleted(true)
                .MaxDegreeOfParallelism(4)
                .Size(2)
            );

            bulkAll.Subscribe(new BulkAllObserver(
                onNext: (b) => {
                     Console.Write("."); 
                     },
                onError: (e) => { throw e; },
                onCompleted: () =>  waitHandle.Signal()
            ));
            
            waitHandle.Wait();
            Console.WriteLine("Done.");

            Console.ReadLine();
            
           
        }
            
    }
}
