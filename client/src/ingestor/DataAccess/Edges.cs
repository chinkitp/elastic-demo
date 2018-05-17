

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Ingestor.Model;
using Newtonsoft.Json;

public static class Edges
{
    private static Dictionary<string,IEnumerable<Edge>> _sourceDictionary = new Dictionary<string,IEnumerable<Edge>>();
    private static Dictionary<string,IEnumerable<Edge>> _targedDictionary = new Dictionary<string,IEnumerable<Edge>>();

    static Edges()
    {
        Console.WriteLine("Starting to load Edges"); 
        var edgeLines = File.ReadAllLines(AppSettings.Current.EdgesFile);
        ConcurrentBag<Edge> edges = new ConcurrentBag<Edge>();
        Parallel.ForEach(edgeLines, edgeLine =>{
            var edge = JsonConvert.DeserializeObject<Edge>(edgeLine);
            edges.Add(edge);
        });   
    
        foreach (var item in  edges.GroupBy(e => e.Source, (a,b) => new{Source=a,Edges=b}))
        {
                _sourceDictionary.Add(item.Source,item.Edges);
        }
        foreach (var item in  edges.GroupBy(e => e.Target, (a,b) => new{Target=a,Edges=b}))
        {
                _targedDictionary.Add(item.Target,item.Edges);
        }  
        Console.WriteLine("Edges loaded");  
    }

    public static void Load(){}

    public static IReadOnlyDictionary<string,IEnumerable<Edge>> SourceDictionary => _sourceDictionary; 
    public static IReadOnlyDictionary<string,IEnumerable<Edge>> TargetDictionary => _targedDictionary; 
}
