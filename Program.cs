using Gremlin.Net.Driver;
using Gremlin.Net.Driver.Remote;
using Gremlin.Net.Process.Remote;
using Gremlin.Net.Process.Traversal;

string mcu_hero_network_file = "hero-network.csv";

IRemoteConnection remoteConnection = new DriverRemoteConnection(
    new GremlinClient(new GremlinServer("localhost", 8182)), "g");

GraphTraversalSource g = AnonymousTraversalSource.Traversal().WithRemote(remoteConnection);

#region Gremlin Queries
//var r = g.V("BLACK PANTHER/T'CHAL").InE("knows").Count();
//var r = g.V().Has("id", "BLACK PANTHER/T'CHAL").BothE("knows").ToList().Count();
//var hero_1_id = g.V().Has("hero", "id", "PRINCESS ZANDA").Next();//.ValueMap<object, object>("id").Next();
//var hero_1_id = g.V().Has("hero", "id", "BLACK PANTHER/T'CHA").OutE("knows").ToList();
//var all_ids_edges = g.V().Has("id", "BLACK PANTHER/T'CHAL").InE("knows").ToList();
//var a = g.V().Has("id", "BLACK PANTHER/T'CHAL").OutE("knows").InV().Has("id", "PRINCESS ZANDA").Next();
//var o = g.V().Has("id", "BLACK PANTHER/T'CHAL").OutE("knows").ToList();
//var t = hero_edge_1.Where(x => checked((long)x.InV.Id) == checked((long)hero_2_id.Id));
//int out_edge_count = g.V().Has("id", hero_name_1).OutE("knows").ToList().Count();
//int in_edge_count = g.V().Has("id", hero_name_1).OutE("knows").ToList().Count();
#endregion

string[] all_heroes = File.ReadAllLines(mcu_hero_network_file);
bool header = true;

int all_heroes_count = all_heroes.Count();
int count = 1;

foreach (string hero in all_heroes)
{
    if (header) Console.WriteLine("Skipping file headers");

    if (!header)
    {
        string[] heroes = hero.Split("\",\"");
        string hero_name_1 = heroes[0].Replace("\"", " ").Trim();
        string hero_name_2 = heroes[1].Replace("\"", " ").Trim();

        Console.SetCursorPosition(0, Console.CursorTop - 1);
        Console.WriteLine($"Adding: {hero_name_1} <-> {hero_name_2}");
        Console.Write($"{count}/{all_heroes_count}");

        bool hero_1_added = g.V().Has("hero", "name", hero_name_1).HasNext();
        bool hero_2_added = g.V().Has("hero", "name", hero_name_2).HasNext();

        if (!hero_1_added && !hero_2_added)
        {
            // Add vertex
            await g.AddV("hero").Property("name", hero_name_1).Promise(i => i.Iterate());
            await g.AddV("hero").Property("name", hero_name_2).Promise(i => i.Iterate());

            // Add edge
            await g.V().Has("name", hero_name_1).AddE("knows").To(__.V().Has("name", hero_name_2)).Promise(i => i.Iterate());
        }
        else if (hero_1_added && !hero_2_added)
        {
            await g.AddV("hero").Property("name", hero_name_2).Promise(i => i.Iterate());
            await g.V().Has("name", hero_name_1).AddE("knows").To(__.V().Has("name", hero_name_2)).Promise(i => i.Iterate());
        }
        else if (!hero_1_added && hero_2_added)
        {
            await g.AddV("hero").Property("name", hero_name_1).Promise(i => i.Iterate());
            await g.V().Has("name", hero_name_2).AddE("knows").To(__.V().Has("name", hero_name_1)).Promise(i => i.Iterate());
        }
        else if (hero_1_added && hero_2_added)
        {

            var hero_1_id = g.V().Has("hero", "name", hero_name_1).Next(); //.ValueMap<object, object>("id").Next();
            var hero_2_id = g.V().Has("hero", "name", hero_name_2).Next();
            var hero_edge_out = g.V().Has("name", hero_name_1).OutE("knows").ToList().Where(e => checked((long)e.InV.Id) == checked((long)hero_2_id.Id));
            var hero_edge_in = g.V().Has("name", hero_name_1).InE("knows").ToList().Where(e => checked((long)e.OutV.Id) == checked((long)hero_2_id.Id));

            if (hero_edge_out.Count() == 0 && hero_edge_in.Count() == 0)
            {
                await g.V().Has("name", hero_name_1).AddE("knows").To(__.V().Has("name", hero_name_2)).Promise(i => i.Iterate());
            }

            if (hero_edge_out.Count() == 0 && hero_edge_in.Count() > 0)
            {
                await g.V().Has("name", hero_name_1).AddE("knows").To(__.V().Has("name", hero_name_2)).Promise(i => i.Iterate());
            }
        }

        count++;
    }

    header = false;
}