using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace Ingestor
{
    public static class FileReader
    {
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
    }
}
