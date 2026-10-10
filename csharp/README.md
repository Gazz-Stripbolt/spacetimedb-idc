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
   No extra packages: it only uses `SpacetimeDB.Runtime` and `System.Text.Json`. Works with .NET 8 (Mono or
   NativeAOT-LLVM) and .NET 10 (NativeAOT-LLVM). See [Performance](#performance-use-nativeaot-llvm).
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

## Handoffs

[`Handoff.cs`](Handoff.cs) sits on top of `Idc.cs` and moves an entity (say, a character) to another database with
no window where it's duplicated or lost. Add it next to `Idc.cs`, call `HandoffInit(ctx)` in `Init`, route
`HandoffOnIdcMessage(tx, msg)` first in `OnIdcMessage`, and implement the six `Handoff*` partial hooks. See
[`docs/HANDOFF.md`](../docs/HANDOFF.md) and the [shard demo](../demo/csharp/shard/Lib.cs).

## Performance: use NativeAOT-LLVM

| Build | C# ⇄ C# round trip | Route transport | Reducer transport |
|---|---|---|---|
| **NativeAOT-LLVM** (.NET 10 on Linux; .NET 8 + `--native-aot` on Windows) | **~24–32 ms** | **~270 orders/s** | ~43 orders/s |
| Mono JIT (`.NET 8`, `wasi-experimental` workload) | ~60–90 ms | ~154 orders/s | ~21 orders/s |

With NativeAOT-LLVM, C# runs on par with Rust and TypeScript. The demo modules target `net10.0`, and the SpacetimeDB CLI
then builds them with NativeAOT-LLVM automatically. The CLI only allows NativeAOT-LLVM on .NET 8 on Windows. On Linux it
needs .NET 10 (`spacetime publish --native-aot` with .NET 8 on Linux is refused). `Idc.cs` works on all three build paths.

## Notes

- Ships a managed SHA-256/HMAC, because `System.Security.Cryptography` isn't available on wasi.
- System.Text.Json is trimmed and AOT-compiled, so `Idc.cs` sticks to `JsonNode` APIs that need no reflection, e.g.
  `JsonValue.Create(...)` and `JsonArray.Add((JsonNode)obj)`, not the generic `Add<T>`. On the Mono path,
  `JsonArray.Add("text")` throws `NoMetadataForType` at runtime.
- `HttpMethod` clashes with `System.Net.Http.HttpMethod` under implicit usings. The file aliases it.
