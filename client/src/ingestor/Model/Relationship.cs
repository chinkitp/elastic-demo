using System.Collections.Generic;

namespace Ingestor
{
    public class Relationship : Epm
    {
        public Vertex Node { get; }

        public Relationship(string id, string label, string[] graphs, Dictionary<string, object> props, Vertex node) : base(id, label, graphs, props)
        {
            Node = node;
        }
    }

}
