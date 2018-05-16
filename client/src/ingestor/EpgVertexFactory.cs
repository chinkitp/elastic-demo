
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace   Ingestor
{
    public static class EpgVertexFactory
    {
        public static string GetGuid()
        {
            return Convert.ToBase64String(Guid.NewGuid().ToByteArray())
            .Substring(0,22);
        }

        public static IEnumerable<EpgVertex> GetDocuments(int number, string label)
        {
            return Enumerable
                .Repeat(0, number)
                .Select(i => {
                        var v = GetNewRandomEpgVertex(label);
                        DocumentsInserted.Add(v.Id);
                        return v;
                });
        }
        public static List<string> DocumentsInserted = new List<string>();

        static EpgVertex GetNewRandomEpgVertex(string label) =>       
        new EpgVertex(GetGuid(), label, new Dictionary<string, string>()
            {
                    {"description", WordList.GetRandomWords(25)},
                    {"short description", WordList.GetRandomWords(7)}
            });
    }
}
