using System;
using Nest;

namespace ingestor
{
    class Program
    {
        static void Main(string[] args)
        {
            ElasticClient client = new ElasticClient(new Uri("http://elastic:9200"));

            
            var created = client.CreateIndex("hello-world-v1", i => i
				.Settings(s => s
					.NumberOfShards(2)
					.NumberOfReplicas(0)
				)

			);

            Console.WriteLine($"The index was {created}");
            


            Console.ReadLine();
            
           
        }
    }
}
