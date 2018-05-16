using System.Collections.Generic;
using Nest;

namespace Ingestor
{
    public abstract class Epm
    {
        private readonly Dictionary<string, object> _props;
        public string Id { get; }
        public string Label { get; }
        public IReadOnlyList<string> _graphs;
        public IReadOnlyDictionary<string, object> Data => _props;

        public Epm(string id, string label, string[] graphs, Dictionary<string, object> props)
        {
            Id = id;
            Label = label;
            _graphs = graphs;
            _props = props;
        }
    }

    public class Vertex : Epm
    {
        private readonly Epm _epm;
        private List<Relationship> _ins = new List<Relationship>();
        private List<Relationship> _outs = new List<Relationship>();

        public Vertex(string id, string label, string[] graphs, Dictionary<string, object> props) : base(id, label, graphs, props)
        {
        }

        public void AddInRelationship(IEnumerable<Relationship> inRelationships)
        {
            _ins.AddRange(inRelationships);
        }

        public void AddOutRelationship(IEnumerable<Relationship> outRelationships)
        {
            _outs.AddRange(outRelationships);
        }

        public IReadOnlyCollection<Relationship> In => (IReadOnlyCollection<Relationship>) _ins;
        public IReadOnlyCollection<Relationship> Out => (IReadOnlyCollection<Relationship>) _outs;

    }

    public class Relationship : Epm
    {
        public Vertex Node { get; }

        public Relationship(string id, string label, string[] graphs, Dictionary<string, object> props, Vertex node) : base(id, label, graphs, props)
        {
            Node = node;
        }
    }

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

    public class Meta
    {
        public string Label { get; }
        public string[] Graphs { get; }

        public Meta(string label, string[] graphs)
        {
            Label = label;
            Graphs = graphs;
        }
    }




    public class EpgVertex
    {
        private readonly Dictionary<string,string> _props;

        public string Id { get; }
        public string Label { get; }
        public IReadOnlyDictionary<string,string> Properties => _props;
        private IList<EpgVertex> _ins = new List<EpgVertex>();
        private Dictionary<string, string> _outs = new Dictionary<string, string>();

        public EpgVertex(string id, string label, Dictionary<string,string> props)
        {
            Id = id;
            Label = label;
            _props = props;
        }

        public void AddIn(EpgVertex inVertex)
        {
            _ins.Add(inVertex);
        }
    }
}
