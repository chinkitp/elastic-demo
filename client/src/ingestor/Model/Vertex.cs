using System.Collections.Generic;

namespace Ingestor
{
    public class Vertex : Epm
    {
        private readonly Epm _epm;
        private List<Relationship> _ins = new List<Relationship>();
        private List<Relationship> _outs = new List<Relationship>();

        public Vertex(string id, string label, string[] graphs, Dictionary<string, object> props) : base(id, label, graphs, props)
        {
        }

        public void AddInRelationship(Relationship inRelationship)
        {
            _ins.Add(inRelationship);
        }

        public void AddOutRelationship(Relationship outRelationship)
        {
            _outs.Add(outRelationship);
        }

        public IReadOnlyCollection<Relationship> In => (IReadOnlyCollection<Relationship>) _ins;
        public IReadOnlyCollection<Relationship> Out => (IReadOnlyCollection<Relationship>) _outs;

    }

}
