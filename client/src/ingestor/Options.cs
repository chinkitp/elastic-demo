using CommandLine;

namespace Ingestor
{
    class Options
    {
        [Option("Upcert", Default = false, HelpText = "Update documents in Elastic.")]
        public bool Upcert { get; set; }

        [Option('n', "number",Default = 100000, HelpText = "Number of Documents to insert into Elastic.")]
        public int NumberOfDocuments { get; set; }
    }
}
