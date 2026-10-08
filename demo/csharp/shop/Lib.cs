// shop, in C#: same behaviour as the Rust and TypeScript shops, using csharp/Idc.cs.

#pragma warning disable STDB_UNSTABLE
#nullable enable

using System.Text.Json.Nodes;
using SpacetimeDB;

public static partial class Module
{
    const string Warehouse = "warehouse";

    [SpacetimeDB.Table(Accessor = "Order", Public = true)]
    public partial struct Order
    {
        [SpacetimeDB.PrimaryKey, SpacetimeDB.AutoInc]
        public ulong Id;
        public string Sku;
        public uint Qty;
        /// <summary>pending → confirmed | rejected</summary>
        public string Status;
        public string Note;
        public Timestamp Placed;
        public Timestamp? Settled;
    }

    /// <summary>The warehouse's stock as last pushed to us. Never polled.</summary>
    [SpacetimeDB.Table(Accessor = "StockMirror", Public = true)]
    public partial struct StockMirror
    {
        [SpacetimeDB.PrimaryKey]
        public string Sku;
        public uint Qty;
        public Timestamp Updated;
    }

    [SpacetimeDB.Reducer(ReducerKind.Init)]
    public static void Init(ReducerContext ctx) => IdcInit(ctx);

    /// <summary>The order row and the "reserve" message commit together.</summary>
    [SpacetimeDB.Reducer]
    public static void PlaceOrder(ReducerContext ctx, string sku, uint qty)
    {
        if (string.IsNullOrEmpty(sku) || qty == 0 || qty > 1000) throw new Exception("need a sku and 1..=1000 qty");
        var order = ctx.Db.Order.Insert(new Order { Sku = sku, Qty = qty, Status = "pending", Note = "", Placed = ctx.Timestamp, Settled = null });
        IdcSend(ctx, Warehouse, "reserve", new JsonObject { ["order_id"] = order.Id, ["sku"] = sku, ["qty"] = qty });
    }

    /// <summary>Called by Idc.cs for every incoming message, inside the receiving transaction.</summary>
    public static partial void OnIdcMessage(IdcTx tx, IdcMessage msg)
    {
        var p = msg.Payload ?? throw new Exception("missing payload");
        switch (msg.Kind)
        {
            case "reservation":
            {
                var id = p["order_id"]!.GetValue<ulong>();
                var order = tx.Db.Order.Id.Find(id) ?? throw new Exception($"no order {id}");
                order.Status = p["ok"]?.GetValue<bool>() == true ? "confirmed" : "rejected";
                order.Note = p["note"]?.GetValue<string>() ?? "";
                order.Settled = tx.Timestamp;
                tx.Db.Order.Id.Update(order);
                break;
            }
            case "stock":
            {
                var row = new StockMirror { Sku = p["sku"]!.GetValue<string>(), Qty = p["qty"]!.GetValue<uint>(), Updated = tx.Timestamp };
                if (tx.Db.StockMirror.Sku.Find(row.Sku) is not null) tx.Db.StockMirror.Sku.Update(row);
                else tx.Db.StockMirror.Insert(row);
                break;
            }
            default:
                throw new Exception($"shop doesn't handle `{msg.Kind}` messages");
        }
    }

    /// <summary>Request/response across databases. `spacetime call shop quote gear`</summary>
    [SpacetimeDB.Procedure]
    public static string Quote(ProcedureContext ctx, string sku)
    {
        try { return IdcRpc(ctx, Warehouse, "/rpc/stock", new JsonObject { ["sku"] = sku })?.ToJsonString() ?? "null"; }
        catch (Exception e) { return new JsonObject { ["error"] = e.Message }.ToJsonString(); }
    }

    /// <summary>SQL pull from the other database. `spacetime call shop peek_warehouse`</summary>
    [SpacetimeDB.Procedure]
    public static string PeekWarehouse(ProcedureContext ctx)
    {
        try { return IdcSql(ctx, Warehouse, "SELECT * FROM stock")?.ToJsonString() ?? "null"; }
        catch (Exception e) { return new JsonObject { ["error"] = e.Message }.ToJsonString(); }
    }

    /// <summary>JSON for the dashboard (served by the Rust warehouse at /route/).</summary>
    [SpacetimeDB.HttpHandler]
    public static HttpResponse State(HandlerContext ctx, HttpRequest req)
    {
        var json = ctx.WithTx(tx =>
        {
            var orders = new JsonArray();
            foreach (var o in tx.Db.Order.Iter().OrderByDescending(o => o.Id).Take(40))
            {
                orders.Add((JsonNode)new JsonObject
                {
                    ["id"] = o.Id, ["sku"] = o.Sku, ["qty"] = o.Qty, ["status"] = o.Status, ["note"] = o.Note,
                    ["round_trip_ms"] = o.Settled is { } s ? (s.MicrosecondsSinceUnixEpoch - o.Placed.MicrosecondsSinceUnixEpoch) / 1000.0 : null,
                });
            }
            var mirror = new JsonArray();
            foreach (var s in tx.Db.StockMirror.Iter().OrderBy(s => s.Sku, StringComparer.Ordinal))
            {
                mirror.Add((JsonNode)new JsonObject { ["sku"] = s.Sku, ["qty"] = s.Qty });
            }
            return new JsonObject { ["orders"] = orders, ["stock_mirror"] = mirror, ["idc"] = IdcStateJson(new IdcTx(tx.Db, tx.Timestamp)) }.ToJsonString();
        });
        return new HttpResponse(200, HttpVersion.Http11,
            new List<HttpHeader> { new("content-type", "application/json"), new("cache-control", "no-store") },
            HttpBody.FromString(json));
    }

    [SpacetimeDB.HttpRouter]
    public static Router Routes() => IdcRoutes(Router.New()).Get("/api/state", Handlers.State);
}
