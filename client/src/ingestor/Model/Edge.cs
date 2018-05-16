using System.Collections.Generic;

namespace Ingestor.Model
{
    public class Edge
    {
        public string Id { get; }
        public Meta Meta { get; }
        public string Source { get; }
        public string Target { get; }
        public Dictionary<string, object> Data { get; }

        public Edge(string id, Meta meta, string source, string target, Dictionary<string, object> data)
        {
            Id = id;
            Meta = meta;
            Source = source;
            Target = target;
            Data = data;
        }
    }

}
