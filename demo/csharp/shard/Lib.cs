// shard, in C#: same behaviour as the Rust shard, using csharp/Idc.cs and csharp/Handoff.cs.
//
// One strip of a game world. Publish it twice (shard-a, shard-b) and point each at the other.
// Characters walk across the border with a handoff: never on both shards, never on neither.

#pragma warning disable STDB_UNSTABLE
#nullable enable

using System.Text.Json.Nodes;
using SpacetimeDB;

public static partial class Module
{
    /// <summary>
    /// Each shard is Width columns of the world. The shard whose name sorts first is the west
    /// half (x 0..8), the other one the east half (x 8..16).
    /// </summary>
    const int Width = 8;
    const int Height = 5;
    const string Live = "live";
    const string Pending = "pending";

    [SpacetimeDB.Table(Accessor = "Character", Public = true)]
    public partial struct Character
    {
        [SpacetimeDB.PrimaryKey]
        public string Name;
        /// <summary>World coordinates, not shard-local ones.</summary>
        public int X;
        public int Y;
        public uint Hp;
        public uint Gold;
        public List<string> Bag;
        /// <summary>"live", or "pending" while it's arriving and the source hasn't let go yet.</summary>
        public string State;
        public string Note;
        public Timestamp Updated;
    }

    [SpacetimeDB.Table(Accessor = "ShardConfig")]
    public partial struct ShardConfig
    {
        [SpacetimeDB.PrimaryKey]
        public byte Key;
        /// <summary>Arrivals are refused once this many characters are here.</summary>
        public uint Capacity;
    }

    [SpacetimeDB.Reducer(ReducerKind.Init)]
    public static void Init(ReducerContext ctx)
    {
        ctx.Db.ShardConfig.Insert(new ShardConfig { Key = 0, Capacity = 50 });
        IdcInit(ctx);
        HandoffInit(ctx);
    }

    /// <summary>The other shard: the first entry in IDC_PEERS.</summary>
    static string Neighbour() =>
        IdcPeers(IdcEnv.IDC_PEERS).Select(p => p.Name).FirstOrDefault() ?? throw new Exception("no neighbouring shard configured");

    /// <summary>The world columns this shard owns: Lo()..Lo() + Width.</summary>
    static int Lo()
    {
        var other = IdcPeers(IdcEnv.IDC_PEERS).Select(p => p.Name).FirstOrDefault();
        return other is not null && string.CompareOrdinal(other, IdcEnv.IDC_SELF) < 0 ? Width : 0;
    }

    static bool Owns(int x) => x >= Lo() && x < Lo() + Width;

    static Character LiveCharacter(ReducerContext ctx, string name)
    {
        var c = ctx.Db.Character.Name.Find(name) ?? throw new Exception($"no character `{name}` here");
        if (c.State != Live || HandoffIsLocked(ctx, name)) throw new Exception($"`{name}` is in transit");
        return c;
    }

    static JsonObject Data(Character c, int x) => new()
    {
        ["name"] = c.Name, ["x"] = x, ["y"] = c.Y, ["hp"] = c.Hp, ["gold"] = c.Gold,
        ["bag"] = new JsonArray(c.Bag.Select(i => (JsonNode)i).ToArray()),
    };

    [SpacetimeDB.Reducer]
    public static void Spawn(ReducerContext ctx, string name)
    {
        if (string.IsNullOrEmpty(name) || name.Length > 24) throw new Exception("name must be 1-24 characters");
        if (ctx.Db.Character.Name.Find(name) is not null) throw new Exception($"`{name}` already exists here");
        ctx.Db.Character.Insert(new Character
        {
            Name = name, X = Lo() + Width / 2, Y = Height / 2, Hp = 100, Gold = 10, Bag = new List<string> { "torch" },
            State = Live, Note = "", Updated = ctx.Timestamp,
        });
    }

    /// <summary>
    /// Walk one square. Stepping over the border starts a handoff to the neighbouring shard;
    /// the character waits at the border, locked, until the neighbour has it.
    /// </summary>
    [SpacetimeDB.Reducer]
    public static void Step(ReducerContext ctx, string name, int dx, int dy)
    {
        if (Math.Abs(dx) > 1 || Math.Abs(dy) > 1) throw new Exception("one square at a time");
        var c = LiveCharacter(ctx, name);
        var (x, y) = (c.X + dx, Math.Clamp(c.Y + dy, 0, Height - 1));
        if (Owns(x))
        {
            c.X = x;
            c.Y = y;
            c.Note = "";
            c.Updated = ctx.Timestamp;
            ctx.Db.Character.Name.Update(c);
            return;
        }
        if (x < 0 || x >= 2 * Width) throw new Exception("that's the edge of the world");
        c.Y = y;
        var peer = Neighbour();
        HandoffStart(ctx, peer, name, Data(c, x));
        c.Note = $"crossing to {peer}…";
        c.Updated = ctx.Timestamp;
        ctx.Db.Character.Name.Update(c);
    }

    /// <summary>Send a character to the neighbour without walking. timeout_ms = 0 uses the default.</summary>
    [SpacetimeDB.Reducer]
    public static void Transfer(ReducerContext ctx, string name, ulong timeoutMs)
    {
        var c = LiveCharacter(ctx, name);
        var x = Lo() == 0 ? Width : Width - 1;
        HandoffStart(ctx, Neighbour(), name, Data(c, x), timeoutMs == 0 ? null : TimeSpan.FromMilliseconds(timeoutMs));
    }

    [SpacetimeDB.Reducer]
    public static void CancelTransfer(ReducerContext ctx, string name)
    {
        var h = HandoffLatest(new IdcTx(ctx.Db, ctx.Timestamp), name);
        if (h is not { Role: "out" } out_) throw new Exception($"`{name}` isn't leaving");
        HandoffCancel(ctx, out_.Id, "cancelled by player");
    }

    /// <summary>Trading needs both characters live and here. Locked ones can't trade.</summary>
    [SpacetimeDB.Reducer]
    public static void Give(ReducerContext ctx, string from, string to, uint gold)
    {
        var a = LiveCharacter(ctx, from);
        var b = LiveCharacter(ctx, to);
        if (from == to || a.Gold < gold) throw new Exception("not enough gold");
        a.Gold -= gold;
        b.Gold += gold;
        ctx.Db.Character.Name.Update(a);
        ctx.Db.Character.Name.Update(b);
    }

    [SpacetimeDB.Reducer]
    public static void SetCapacity(ReducerContext ctx, uint capacity) =>
        ctx.Db.ShardConfig.Key.Update(new ShardConfig { Key = 0, Capacity = capacity });

    // -----------------------------------------------------------------------
    // Handoff hooks
    // -----------------------------------------------------------------------

    static long? Num(JsonNode? n) => n is JsonValue v && v.TryGetValue<long>(out var l) ? l
        : n is JsonValue d && d.TryGetValue<double>(out var f) && f == Math.Floor(f) ? (long)f : null;

    public static partial string? HandoffValidate(IdcTx tx, string from, string entity, JsonNode data)
    {
        var capacity = tx.Db.ShardConfig.Key.Find(0)?.Capacity ?? 0;
        if (tx.Db.Character.Count >= capacity) return $"{IdcEnv.IDC_SELF} is full";
        if (tx.Db.Character.Name.Find(entity) is not null) return $"the name `{entity}` is taken on {IdcEnv.IDC_SELF}";
        if (data["name"] is not JsonValue n || !n.TryGetValue<string>(out var dn) || dn != entity) return "data doesn't match the entity id";
        if (Num(data["x"]) is not { } x) return "missing x";
        if (!Owns((int)x)) return $"x = {x} isn't on {IdcEnv.IDC_SELF}";
        if (Num(data["hp"]) == 0) return "fallen characters can't travel";
        return null;
    }

    public static partial void HandoffImport(IdcTx tx, string from, string entity, JsonNode data)
    {
        var bag = data["bag"] is JsonArray arr
            ? arr.Select(i => i is JsonValue v && v.TryGetValue<string>(out var s) ? s : null).OfType<string>().ToList()
            : new List<string>();
        tx.Db.Character.Insert(new Character
        {
            Name = entity, X = (int)(Num(data["x"]) ?? 0), Y = (int)(Num(data["y"]) ?? 0),
            Hp = (uint)(Num(data["hp"]) ?? 0), Gold = (uint)(Num(data["gold"]) ?? 0), Bag = bag,
            State = Pending, Note = $"arriving from {from}…", Updated = tx.Timestamp,
        });
    }

    public static partial void HandoffActivate(IdcTx tx, string entity)
    {
        if (tx.Db.Character.Name.Find(entity) is not { } c) return;
        c.State = Live;
        c.Note = "";
        c.Updated = tx.Timestamp;
        tx.Db.Character.Name.Update(c);
    }

    public static partial void HandoffDiscard(IdcTx tx, string entity)
    {
        if (tx.Db.Character.Name.Find(entity) is { State: Pending }) tx.Db.Character.Name.Delete(entity);
    }

    public static partial void HandoffRemove(IdcTx tx, string entity) => tx.Db.Character.Name.Delete(entity);

    public static partial void HandoffReturned(IdcTx tx, string entity, string reason)
    {
        if (tx.Db.Character.Name.Find(entity) is not { } c) return;
        c.Note = $"stayed: {reason}";
        c.Updated = tx.Timestamp;
        tx.Db.Character.Name.Update(c);
    }

    public static partial void OnIdcMessage(IdcTx tx, IdcMessage msg)
    {
        if (HandoffOnIdcMessage(tx, msg)) return;
        throw new Exception($"shard doesn't handle `{msg.Kind}` messages");
    }

    // -----------------------------------------------------------------------
    // HTTP
    // -----------------------------------------------------------------------

    [SpacetimeDB.HttpHandler]
    public static HttpResponse State(HandlerContext ctx, HttpRequest req)
    {
        var json = ctx.WithTx(tx =>
        {
            var itx = new IdcTx(tx.Db, tx.Timestamp);
            var chars = new JsonArray();
            foreach (var c in tx.Db.Character.Iter().OrderBy(c => c.Name, StringComparer.Ordinal))
            {
                chars.Add((JsonNode)new JsonObject
                {
                    ["name"] = c.Name, ["x"] = c.X, ["y"] = c.Y, ["hp"] = c.Hp, ["gold"] = c.Gold,
                    ["bag"] = new JsonArray(c.Bag.Select(i => (JsonNode)i).ToArray()),
                    ["state"] = c.State, ["note"] = c.Note, ["locked"] = HandoffIsLocked(itx, c.Name),
                });
            }
            var handoffs = new JsonArray();
            foreach (var h in tx.Db.Handoff.Iter().OrderByDescending(h => h.Updated.MicrosecondsSinceUnixEpoch))
            {
                handoffs.Add((JsonNode)new JsonObject
                {
                    ["id"] = h.Id, ["entity"] = h.Entity, ["role"] = h.Role, ["peer"] = h.Peer, ["status"] = h.Status,
                    ["detail"] = h.Detail,
                    ["ms"] = (h.Updated.MicrosecondsSinceUnixEpoch - h.Started.MicrosecondsSinceUnixEpoch) / 1000.0,
                });
            }
            var lo = Lo();
            return new JsonObject
            {
                ["shard"] = new JsonObject
                {
                    ["lo"] = lo, ["hi"] = lo + Width, ["height"] = Height,
                    ["capacity"] = tx.Db.ShardConfig.Key.Find(0)?.Capacity ?? 0,
                },
                ["characters"] = chars,
                ["handoffs"] = handoffs,
                ["idc"] = IdcStateJson(itx),
            }.ToJsonString();
        });
        return new HttpResponse(200, HttpVersion.Http11,
            new List<HttpHeader> { new("content-type", "application/json"), new("cache-control", "no-store") },
            HttpBody.FromString(json));
    }

    [SpacetimeDB.HttpRouter]
    public static Router Routes() => IdcRoutes(Router.New()).Get("/api/state", Handlers.State);
}
