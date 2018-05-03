using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace Ingestor
{
    public static class FileReader
    {
        static FileReader()
        {
            if(_words == null)
            {
                _words = File.ReadAllLines("./client/src/ingestor/words.txt")
                    .Select(l => l.Split('\t').First())
                    .ToArray();
            }
        }

        public static IEnumerable<string> ReadLines(string directoryPath, string fileExtension)
        {
            var filesToRead = Directory.EnumerateFiles(directoryPath)
                .Where(f => Path.GetExtension(f) == fileExtension);
            
            return filesToRead.ToList()
                .SelectMany(f => ReadLines(f));
        }

        public static IEnumerable<string> ReadLines(string filePath)
        {
            using(var msm =  File.Open(filePath,FileMode.Open))
            {
                using(var sm = new StreamReader(msm))
                {
                    while(!sm.EndOfStream)
                    {
                        yield return sm.ReadLine();
                    }                   
                }
            }         
        }

        private static string[] _words = null;

        public static string GetRandomWords(int count)
        {
            Random randNum = new Random();
            IEnumerable<string> words = Enumerable
                .Repeat(0, count)
                .Select(i => randNum.Next(0, _words.Length))
                .Select(r => _words[r]);

            return String.Join(" ",words);           
        }

        public static string GetGuid()
        {
            return Convert.ToBase64String(Guid.NewGuid().ToByteArray())
            .Substring(0,22);
        }

        public static IEnumerable<EpgVertex> GetDocuments(int number, string label)
        {
            return Enumerable
                .Repeat(0, number)
                .Select(i => GetNewRandomEpgVertex(label));

        }

        static EpgVertex GetNewRandomEpgVertex(string label) =>       
        new EpgVertex(GetGuid(), label, new Dictionary<string, string>()
           {
                 {"description", GetRandomWords(25)},
                 {"short description", GetRandomWords(7)}
           });


    }
}
