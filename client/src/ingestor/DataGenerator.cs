
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace   Ingestor
{
    // public static class EpgVertexFactory
    // {
    //     public static string GetGuid()
    //     {
    //         return Convert.ToBase64String(Guid.NewGuid().ToByteArray())
    //         .Substring(0,22);
    //     }

    //     public static IEnumerable<EpgVertex> GetDocuments(int number, string label)
    //     {
    //         return Enumerable
    //             .Repeat(0, number)
    //             .Select(i => {
    //                     var v = GetNewRandomEpgVertex(label);
    //                     DocumentsInserted.Add(v.Id);
    //                     return v;
    //             });
    //     }
    //     public static List<string> DocumentsInserted = new List<string>();

    //     static EpgVertex GetNewRandomEpgVertex(string label) =>       
    //     new EpgVertex(GetGuid(), label, new Dictionary<string, string>()
    //         {
    //                 {"description", WordList.GetRandomWords(25)},
    //                 {"short description", WordList.GetRandomWords(7)}
    //         });
    // }

    //   public static class WordList
    // {
    //     static WordList()
    //     {
    //         if(_words == null)
    //         {
    //             _words = File.ReadAllLines("/Users/chinkit/00D2D-CRC/02-Dev/elastic-demo/client/src/ingestor/words.txt")
    //                 .Select(l => l.Split('\t').First())
    //                 .ToArray();
    //         }
    //     }

    //     private static string[] _words = null;

    //     public static string GetRandomWords(int count)
    //     {
    //         Random randNum = new Random();
    //         IEnumerable<string> words = Enumerable
    //             .Repeat(0, count)
    //             .Select(i => randNum.Next(0, _words.Length))
    //             .Select(r => _words[r]);

    //         return String.Join(" ",words);           
    //     }
    // }
}
