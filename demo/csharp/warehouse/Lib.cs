// warehouse, in C#: same behaviour as the Rust warehouse, using csharp/Idc.cs.

#pragma warning disable STDB_UNSTABLE
#nullable enable

using System.Text.Json.Nodes;
using SpacetimeDB;

public static partial class Module
{
    const string Shop = "shop";

    [SpacetimeDB.Table(Accessor = "Stock", Public = true)]
    public partial struct Stock
    {
        [SpacetimeDB.PrimaryKey]
        public string Sku;
        public uint Qty;
    }

    [SpacetimeDB.Table(Accessor = "Reservation", Public = true)]
    public partial struct Reservation
    {
        [SpacetimeDB.PrimaryKey, SpacetimeDB.AutoInc]
        public ulong Id;
        /// <summary>Which database asked, and its order id there.</summary>
        public string Peer;
        public ulong OrderId;
        public string Sku;
        public uint Qty;
        public Timestamp At;
    }

    [SpacetimeDB.Reducer(ReducerKind.Init)]
    public static void Init(ReducerContext ctx)
    {
        foreach (var (sku, qty) in new[] { ("gear", 25u), ("sprocket", 10u), ("rocket-boots", 1u) })
        {
            ctx.Db.Stock.Insert(new Stock { Sku = sku, Qty = qty });
        }
        IdcInit(ctx);
    }

    static void SetStock(IdcTx tx, string sku, uint qty)
    {
        var row = new Stock { Sku = sku, Qty = qty };
        if (tx.Db.Stock.Sku.Find(sku) is not null) tx.Db.Stock.Sku.Update(row);
        else tx.Db.Stock.Insert(row);
        // Replicate the change to the shop: same transaction, pushed right after commit.
        IdcSend(tx, Shop, "stock", new JsonObject { ["sku"] = sku, ["qty"] = qty });
    }

    [SpacetimeDB.Reducer]
    public static void Restock(ReducerContext ctx, string sku, uint qty)
    {
        if (string.IsNullOrEmpty(sku) || qty == 0) throw new Exception("need a sku and qty > 0");
        var current = ctx.Db.Stock.Sku.Find(sku)?.Qty ?? 0;
        SetStock(new IdcTx(ctx.Db, ctx.Timestamp), sku, current + qty);
    }

    /// <summary>Send the full stock list to the shop.</summary>
    [SpacetimeDB.Reducer]
    public static void SyncAll(ReducerContext ctx)
    {
        foreach (var s in ctx.Db.Stock.Iter().ToList())
        {
            IdcSend(ctx, Shop, "stock", new JsonObject { ["sku"] = s.Sku, ["qty"] = s.Qty });
        }
    }

    public static partial void OnIdcMessage(IdcTx tx, IdcMessage msg)
    {
        if (msg.Kind != "reserve") throw new Exception($"warehouse doesn't handle `{msg.Kind}` messages");
        var p = msg.Payload ?? throw new Exception("reserve: missing payload");
        var orderId = p["order_id"]!.GetValue<ulong>();
        var sku = p["sku"]!.GetValue<string>();
        var qty = p["qty"]!.GetValue<uint>();
        bool ok;
        string note;
        var have = tx.Db.Stock.Sku.Find(sku)?.Qty;
        if (have is null) { ok = false; note = $"unknown sku `{sku}`"; }
        else if (have < qty) { ok = false; note = $"only {have} left"; }
        else
        {
            SetStock(tx, sku, have.Value - qty);
            tx.Db.Reservation.Insert(new Reservation { Peer = msg.From, OrderId = orderId, Sku = sku, Qty = qty, At = tx.Timestamp });
            ok = true;
            note = $"reserved {qty} × {sku}";
        }
        // A business "no" is a normal reply, not an error: errors mean "retry me".
        IdcSend(tx, msg.From, "reservation", new JsonObject { ["order_id"] = orderId, ["ok"] = ok, ["note"] = note });
    }

    /// <summary>Synchronous, signed request/response: "how many X do you have?"</summary>
    [SpacetimeDB.HttpHandler]
    public static HttpResponse RpcStock(HandlerContext ctx, HttpRequest req)
    {
        var (from, payload, error) = IdcVerifyRpc(ctx, req);
        if (error is { } e) return e;
        var sku = payload?["sku"]?.GetValue<string>() ?? "";
        var qty = ctx.WithTx(tx => tx.Db.Stock.Sku.Find(sku)?.Qty);
        return IdcRpcReply(new JsonObject { ["sku"] = sku, ["qty"] = qty, ["answered_by"] = "warehouse" });
    }

    [SpacetimeDB.HttpHandler]
    public static HttpResponse State(HandlerContext ctx, HttpRequest req)
    {
        var json = ctx.WithTx(tx =>
        {
            var stock = new JsonArray();
            foreach (var s in tx.Db.Stock.Iter().OrderBy(s => s.Sku, StringComparer.Ordinal))
                stock.Add((JsonNode)new JsonObject { ["sku"] = s.Sku, ["qty"] = s.Qty });
            var reservations = new JsonArray();
            foreach (var r in tx.Db.Reservation.Iter().OrderByDescending(r => r.Id).Take(40))
                reservations.Add((JsonNode)new JsonObject { ["id"] = r.Id, ["peer"] = r.Peer, ["order_id"] = r.OrderId, ["sku"] = r.Sku, ["qty"] = r.Qty });
            return new JsonObject { ["stock"] = stock, ["reservations"] = reservations, ["idc"] = IdcStateJson(new IdcTx(tx.Db, tx.Timestamp)) }.ToJsonString();
        });
        return new HttpResponse(200, HttpVersion.Http11,
            new List<HttpHeader> { new("content-type", "application/json"), new("cache-control", "no-store") },
            HttpBody.FromString(json));
    }

    [SpacetimeDB.HttpRouter]
    public static Router Routes() => IdcRoutes(Router.New()).Post("/rpc/stock", Handlers.RpcStock).Get("/api/state", Handlers.State);
}
