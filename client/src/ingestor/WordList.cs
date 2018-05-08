using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace Ingestor
{
    public static class WordList
    {
        static WordList()
        {
            if(_words == null)
            {
                _words = File.ReadAllLines("./client/src/ingestor/words.txt")
                    .Select(l => l.Split('\t').First())
                    .ToArray();
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
    }
}
