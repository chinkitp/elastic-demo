using System;
using System.Collections.Generic;
using System.IO;
using Newtonsoft.Json;
using System.Threading.Tasks;
using System.Collections.Concurrent;

namespace Ingestor.DataAccess
{
    static class Nodes 
    {
        private static ConcurrentDictionary<string,Node> _nodes = new ConcurrentDictionary<string,Node>();
        static Nodes()
        {
            Console.WriteLine("Starting to read nodes.");
            var nodeLines = File.ReadAllLines(@"/Users/chinkit/00D2D-CRC/04-BigData/stackoverflow/step1/all-nodes.json");
            ConcurrentBag<Node> nodes = new ConcurrentBag<Node>();
            Parallel.ForEach(nodeLines, nodeLine =>{
                var node = JsonConvert.DeserializeObject<Node>(nodeLine);
                nodes.Add(node);
            }); 
            foreach (var node in nodes)
            {
                _nodes.AddOrUpdate(node.Id,node,(a,b)=>b);
            }

            Console.WriteLine("Nodes loaded");                  
        }

        public static IEnumerable<Node> GetAll() => (IEnumerable<Node>) _nodes.Values;

        public static Node GetById(string id) => _nodes[id];
    }
}
