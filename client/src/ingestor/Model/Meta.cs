namespace Ingestor
{
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

}
