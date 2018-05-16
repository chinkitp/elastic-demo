using System.Collections.Generic;

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

}
