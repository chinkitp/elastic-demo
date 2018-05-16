using System.Collections.Generic;
using Nest;
using Ingestor.Model;

namespace Ingestor
{
    [ElasticsearchType(IdProperty = nameof(Id))]
    public class Node
    {
        public string Id { get; }
        public Meta Meta { get; }
        public Dictionary<string, object> Data { get; }

        public Node(string id, Meta meta, Dictionary<string,object> data)
        {
            Id = id;
            Meta = meta;
            Data = data;
        }
    }

}
