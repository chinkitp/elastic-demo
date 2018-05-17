

public class AppSettings
{
    public string NodesFile { get; set; }
    public string EdgesFile { get; set; }   
    public string ElasticServerUrl {get; set;}

    public static AppSettings Current{ get; set;}
}