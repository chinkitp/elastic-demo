using System.Collections.Generic;


namespace Ingestor
{
    public class EpgVertex
    {
        private readonly Dictionary<string,string> _props;

        public string Id { get; }
        public string Label { get; }
        public IReadOnlyDictionary<string,string> Properties => _props;

        public EpgVertex(string id, string label, Dictionary<string,string> props)
        {
            Id = id;
            Label = label;
            _props = props;
        }
    }
}
