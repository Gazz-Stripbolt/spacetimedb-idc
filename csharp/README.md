# Idc.cs: inter-database communication for C# modules

One file you add to a C# SpacetimeDB module. Your database can then push messages to other databases (C#, Rust or
TypeScript) and react to theirs, with no polling. C# submodules aren't supported by SpacetimeDB yet, so it's a source
file rather than a package.

Source: [`Idc.cs`](Idc.cs) · Complete examples: [`demo/csharp/shop`](../demo/csharp/shop/Lib.cs),
[`demo/csharp/warehouse`](../demo/csharp/warehouse/Lib.cs) · Protocol: [`docs/PROTOCOL.md`](../docs/PROTOCOL.md)

## Add it

1. Copy `Idc.cs` into your module project, or link it from your `.csproj`:
   ```xml
   <ItemGroup>
     <Compile Include="path/to/Idc.cs" Link="Idc.cs" />
   </ItemGroup>
   ```
   No extra packages: it only uses `SpacetimeDB.Runtime` and `System.Text.Json`.
2. Wire it up in your `Module`:
   ```csharp
   [SpacetimeDB.Reducer(ReducerKind.Init)]
   public static void Init(ReducerContext ctx) => IdcInit(ctx);

   [SpacetimeDB.HttpRouter]
   public static Router Routes() => IdcRoutes(Router.New())   // adds POST /idc/inbox and /idc/pair
       .Get("/api/state", Handlers.State);

   // Required: called for every incoming message, inside the receiving transaction.
   public static partial void OnIdcMessage(IdcTx tx, IdcMessage msg)
   {
       switch (msg.Kind)
       {
           case "reservation": /* tx.Db.Order.Id.Update(...) */ break;
           default: throw new Exception($"unknown kind {msg.Kind}");   // throw = permanent refusal (dead letter)
       }
   }

   [SpacetimeDB.Reducer]
   public static void PlaceOrder(ReducerContext ctx, string sku, uint qty)
   {
       var order = ctx.Db.Order.Insert(new Order { /* ... */ });
       IdcSend(ctx, "warehouse", "reserve", new JsonObject { ["order_id"] = order.Id, ["sku"] = sku, ["qty"] = qty });
   }
   ```
3. Publish with the four env vars (the file declares them in `IdcEnvironment`):
   ```bash
   IDC_SELF=shop IDC_SECRET=$SECRET IDC_TRANSPORT=route \
   IDC_PEERS=warehouse=https://maincloud.spacetimedb.com/v1/database/my-warehouse \
   spacetime publish my-shop
   ```

If your module already declares a `[SpacetimeDB.Env]` struct, move the four `IDC_*` fields into it and delete
`IdcEnvironment` (a module has one env declaration).

## API

| Member | What it does |
|---|---|
| `IdcInit(ctx)` | Idempotent setup. Call it from `Init`. |
| `IdcSend(ctx \| IdcTx, peer, kind, payload)` | Queues a message in the current transaction; delivery starts right after commit |
| `IdcRoutes(router)` | Adds the `/idc/inbox` and `/idc/pair` routes |
| `OnIdcMessage(IdcTx, IdcMessage)` | **You implement this** (a required partial method) |
| `IdcRpc(ctx, peer, path, payload)` | Signed synchronous call to a peer's route, from a procedure |
| `IdcVerifyRpc(ctx, req)` / `IdcRpcReply(value)` | For your RPC handlers |
| `IdcSql(ctx, peer, query)` | SQL against the peer's `/sql`, from a procedure |
| `IdcStateJson(IdcTx)` | Summary for dashboards |
| `IdcKick` (reducer) | Re-run setup, pairing and flushing |
| `IdcReceive` (reducer) | Receiving end of the reducer transport |

`IdcTx(Local Db, Timestamp)` works the same from reducers (`new IdcTx(ctx.Db, ctx.Timestamp)`), procedure transactions
and handler transactions.

## Notes

- About 2–3× slower than Rust/TS (Mono interpreting IL on wasm): roughly 60–90 ms round trips and ~150 orders/s
  C# ⇄ C#. Fine for most cross-database traffic.
- Ships a managed SHA-256/HMAC, because `System.Security.Cryptography` isn't available on wasi.
- System.Text.Json reflection metadata is trimmed, so `JsonArray.Add("text")` throws at runtime. Use `JsonValue.Create(...)`.
- `HttpMethod` clashes with `System.Net.Http.HttpMethod` under implicit usings. The file aliases it.
