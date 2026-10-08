// Idc.cs: inter-database communication for SpacetimeDB C# modules.
//
// The C# twin of rust/idc.rs and the spacetimedb-idc TypeScript submodule. Same wire
// protocol (docs/PROTOCOL.md), so all three talk to each other.
//
// Drop this file into your module project and implement one method:
//
//     public static partial void OnIdcMessage(IdcTx tx, IdcMessage msg) { ... }
//
// Then call IdcInit(ctx) from your Init reducer and add IdcRoutes(...) to your [HttpRouter].
// Config comes from four env vars: IDC_SELF, IDC_SECRET, IDC_PEERS, IDC_TRANSPORT.

#pragma warning disable STDB_UNSTABLE
#nullable enable

using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using SpacetimeDB;
using HttpMethod = SpacetimeDB.HttpMethod;

/// <summary>A transaction to send from or apply a message in: works from reducers, procedures and handlers.</summary>
public readonly record struct IdcTx(Local Db, Timestamp Timestamp);

/// <summary>An incoming message.</summary>
public readonly record struct IdcMessage(string Id, string From, string Kind, JsonNode? Payload);

public static partial class Module
{
    // -----------------------------------------------------------------------
    // Configuration
    // -----------------------------------------------------------------------

    /// <summary>
    /// Owner-set configuration. If your module already declares a [SpacetimeDB.Env] struct,
    /// move these four fields into it and delete this one (a module has one env declaration).
    /// </summary>
    [SpacetimeDB.Env]
    public partial struct IdcEnvironment
    {
        /// <summary>This database's name in the mesh, e.g. "shop".</summary>
        public string IDC_SELF;
        /// <summary>Shared HMAC secret (same on every peer).</summary>
        public string IDC_SECRET;
        /// <summary>"name=https://host/v1/database/&lt;db&gt;,other=..."</summary>
        public string IDC_PEERS;
        [SpacetimeDB.EnvValues("route", "reducer")]
        public string IDC_TRANSPORT;
    }

    static ModuleEnvironment IdcEnv => default;

    const long IdcMaxSkewUs = 5L * 60 * 1_000_000;
    const long IdcLeaseUs = 60L * 1_000_000;
    const int IdcBatch = 64;
    const long IdcMaxBackoffMs = 60_000;
    const ulong IdcLogKeep = 300;
    const long IdcSeenRetentionUs = 7L * 24 * 3600 * 1_000_000;
    const string IdcSignatureHeader = "x-idc-signature";
    static readonly TimeSpan IdcHttpTimeout = TimeSpan.FromSeconds(10);

    readonly record struct IdcPeer(string Name, string Base)
    {
        public string Host => Base.Contains("/v1/") ? Base[..Base.IndexOf("/v1/", StringComparison.Ordinal)] : Base;
    }

    static List<IdcPeer> IdcPeers(string raw) =>
        raw.Split(',')
            .Select(e => e.Trim())
            .Where(e => e.Contains('='))
            .Select(e => new IdcPeer(e[..e.IndexOf('=')].Trim(), e[(e.IndexOf('=') + 1)..].Trim().TrimEnd('/')))
            .ToList();

    static IdcPeer IdcFindPeer(string name) =>
        IdcPeers(IdcEnv.IDC_PEERS).FirstOrDefault(p => p.Name == name) is { Name: not null } p
            ? p
            : throw new Exception($"unknown peer `{name}`");

    // -----------------------------------------------------------------------
    // Tables
    // -----------------------------------------------------------------------

    [SpacetimeDB.Table(Accessor = "IdcOutbox")]
    public partial struct IdcOutbox
    {
        [SpacetimeDB.PrimaryKey, SpacetimeDB.AutoInc]
        public ulong Id;
        [SpacetimeDB.Index.BTree]
        public string Peer;
        public string Kind;
        public string Payload;
        public Timestamp Created;
        public uint Attempts;
        public Timestamp NextAttempt;
        public Timestamp? InFlightUntil;
        public bool Dead;
        public string LastError;
    }

    [SpacetimeDB.Table(Accessor = "IdcSeen")]
    public partial struct IdcSeen
    {
        [SpacetimeDB.PrimaryKey]
        public string Key;
        public Timestamp At;
    }

    [SpacetimeDB.Table(Accessor = "IdcKnownPeer")]
    public partial struct IdcKnownPeer
    {
        [SpacetimeDB.PrimaryKey]
        public Identity Identity;
        [SpacetimeDB.Index.BTree]
        public string Name;
        public Timestamp PairedAt;
    }

    [SpacetimeDB.Table(Accessor = "IdcPeerToken")]
    public partial struct IdcPeerToken
    {
        [SpacetimeDB.PrimaryKey]
        public string Peer;
        public Identity Identity;
        public string Token;
        public Timestamp PairedAt;
    }

    [SpacetimeDB.Table(Accessor = "IdcState")]
    public partial struct IdcState
    {
        [SpacetimeDB.PrimaryKey]
        public byte Key;
        public string Epoch;
    }

    /// <summary>Public activity feed: metadata only, never payloads.</summary>
    [SpacetimeDB.Table(Accessor = "IdcLog", Public = true)]
    public partial struct IdcLog
    {
        [SpacetimeDB.PrimaryKey, SpacetimeDB.AutoInc]
        public ulong Id;
        public Timestamp At;
        public string Direction;
        public string Peer;
        public string Kind;
        public string Transport;
        public string MsgId;
        public string Event;
        public string Detail;
        public long LatencyUs;
    }

    [SpacetimeDB.Table(Accessor = "IdcFlushJob", Scheduled = nameof(IdcFlush), ScheduledAt = nameof(IdcFlushJob.ScheduledAt))]
    public partial struct IdcFlushJob
    {
        [SpacetimeDB.PrimaryKey, SpacetimeDB.AutoInc]
        public ulong ScheduledId;
        public ScheduleAt ScheduledAt;
    }

    [SpacetimeDB.Table(Accessor = "IdcPairJob", Scheduled = nameof(IdcPair), ScheduledAt = nameof(IdcPairJob.ScheduledAt))]
    public partial struct IdcPairJob
    {
        [SpacetimeDB.PrimaryKey, SpacetimeDB.AutoInc]
        public ulong ScheduledId;
        public ScheduleAt ScheduledAt;
        public uint Attempt;
    }

    [SpacetimeDB.Table(Accessor = "IdcPruneJob", Scheduled = nameof(IdcPrune), ScheduledAt = nameof(IdcPruneJob.ScheduledAt))]
    public partial struct IdcPruneJob
    {
        [SpacetimeDB.PrimaryKey, SpacetimeDB.AutoInc]
        public ulong ScheduledId;
        public ScheduleAt ScheduledAt;
    }

    /// <summary>Your message handler, called inside the receiving transaction. Throw to refuse a message permanently.</summary>
    public static partial void OnIdcMessage(IdcTx tx, IdcMessage msg);

    // -----------------------------------------------------------------------
    // Setup
    // -----------------------------------------------------------------------

    /// <summary>Call from your Init reducer. Idempotent.</summary>
    public static void IdcInit(ReducerContext ctx)
    {
        var tx = new IdcTx(ctx.Db, ctx.Timestamp);
        if (ctx.Db.IdcState.Key.Find(0) is null)
        {
            ctx.Db.IdcState.Insert(new IdcState { Key = 0, Epoch = ctx.Timestamp.MicrosecondsSinceUnixEpoch.ToString("x") });
        }
        if (ctx.Db.IdcPruneJob.Count == 0)
        {
            ctx.Db.IdcPruneJob.Insert(new IdcPruneJob { ScheduledAt = new ScheduleAt.Interval(new TimeDuration(3600L * 1_000_000)) });
        }
        ctx.Db.IdcPairJob.Insert(new IdcPairJob { ScheduledAt = new ScheduleAt.Time(ctx.Timestamp), Attempt = 0 });
        IdcScheduleFlush(tx, ctx.Timestamp);
    }

    /// <summary>
    /// One-time setup for databases that predate idc, and a way to re-run pairing and
    /// flushing (e.g. after changing IDC_PEERS with `spacetime publish --env-only`).
    /// </summary>
    [SpacetimeDB.Reducer]
    public static void IdcKick(ReducerContext ctx) => IdcInit(ctx);

    /// <summary>Add the two routes every peer exposes to your [HttpRouter].</summary>
    public static Router IdcRoutes(Router router) =>
        router.Post("/idc/inbox", Handlers.IdcInbox).Post("/idc/pair", Handlers.IdcPairRoute);

    // -----------------------------------------------------------------------
    // Signing (managed SHA-256: System.Security.Cryptography isn't available on wasi)
    // -----------------------------------------------------------------------

    static string IdcHex(byte[] b) => Convert.ToHexString(b).ToLowerInvariant();

    static byte[] IdcMac(string secret, long t, byte[] body)
    {
        var prefix = Encoding.UTF8.GetBytes($"{t}.");
        var msg = new byte[prefix.Length + body.Length];
        prefix.CopyTo(msg, 0);
        body.CopyTo(msg, prefix.Length);
        return IdcSha256.Hmac(Encoding.UTF8.GetBytes(secret), msg);
    }

    /// <summary>t=&lt;micros&gt;,v1=&lt;hex hmac-sha256(secret, "&lt;micros&gt;.&lt;body&gt;")&gt;</summary>
    public static string IdcSign(string secret, Timestamp now, byte[] body) =>
        $"t={now.MicrosecondsSinceUnixEpoch},v1={IdcHex(IdcMac(secret, now.MicrosecondsSinceUnixEpoch, body))}";

    public static void IdcVerify(string secret, string? header, byte[] body, Timestamp now)
    {
        if (string.IsNullOrEmpty(header)) throw new Exception("missing X-IDC-Signature");
        long? t = null;
        string? v1 = null;
        foreach (var part in header.Split(','))
        {
            var kv = part.Trim().Split('=', 2);
            if (kv.Length != 2) continue;
            if (kv[0] == "t" && long.TryParse(kv[1], out var parsed)) t = parsed;
            if (kv[0] == "v1") v1 = kv[1];
        }
        if (t is null || v1 is null) throw new Exception("malformed X-IDC-Signature");
        if (Math.Abs(now.MicrosecondsSinceUnixEpoch - t.Value) > IdcMaxSkewUs)
            throw new Exception("signature timestamp outside the allowed window");
        var expected = IdcHex(IdcMac(secret, t.Value, body));
        var diff = expected.Length ^ v1.Length;
        for (var i = 0; i < expected.Length; i++) diff |= expected[i] ^ (i < v1.Length ? v1[i] : 0);
        if (diff != 0) throw new Exception("bad signature");
    }

    /// <summary>Join every value of a header, in case a client split one value across several lines.</summary>
    static string? IdcHeader(HttpRequest req, string name)
    {
        var values = req.Headers
            .Where(h => string.Equals(h.Name, name, StringComparison.OrdinalIgnoreCase))
            .Select(h => Encoding.ASCII.GetString(h.Value))
            .ToList();
        return values.Count == 0 ? null : string.Join(",", values);
    }

    static HttpResponse IdcJson(ushort status, JsonNode body) =>
        new(status, HttpVersion.Http11, new List<HttpHeader> { new("content-type", "application/json") },
            HttpBody.FromString(body.ToJsonString()));

    static JsonObject IdcError(string message) => new() { ["error"] = message };

    // -----------------------------------------------------------------------
    // Sending
    // -----------------------------------------------------------------------

    static string IdcMsgId(IdcTx tx, ulong outboxId) => $"{tx.Db.IdcState.Key.Find(0)?.Epoch ?? ""}-{outboxId}";

    static void IdcWriteLog(IdcTx tx, string direction, string peer, string kind, string transport, string msgId, string evt, string detail = "", long latencyUs = 0)
    {
        var row = tx.Db.IdcLog.Insert(new IdcLog
        {
            At = tx.Timestamp, Direction = direction, Peer = peer, Kind = kind, Transport = transport,
            MsgId = msgId, Event = evt, Detail = detail, LatencyUs = latencyUs,
        });
        if (row.Id > IdcLogKeep) tx.Db.IdcLog.Id.Delete(row.Id - IdcLogKeep);
    }

    static void IdcScheduleFlush(IdcTx tx, Timestamp at)
    {
        foreach (var j in tx.Db.IdcFlushJob.Iter())
        {
            if (j.ScheduledAt is ScheduleAt.Time(var t) && t.MicrosecondsSinceUnixEpoch <= at.MicrosecondsSinceUnixEpoch) return;
        }
        tx.Db.IdcFlushJob.Insert(new IdcFlushJob { ScheduledAt = new ScheduleAt.Time(at) });
    }

    /// <summary>Queue a message for `peer` in the current transaction. Delivery starts as soon as it commits.</summary>
    public static string IdcSend(IdcTx tx, string peer, string kind, JsonNode? payload)
    {
        var row = tx.Db.IdcOutbox.Insert(new IdcOutbox
        {
            Peer = peer, Kind = kind, Payload = payload?.ToJsonString() ?? "null", Created = tx.Timestamp,
            Attempts = 0, NextAttempt = tx.Timestamp, InFlightUntil = null, Dead = false, LastError = "",
        });
        var id = IdcMsgId(tx, row.Id);
        IdcWriteLog(tx, "out", peer, kind, IdcEnv.IDC_TRANSPORT, id, "queued");
        IdcScheduleFlush(tx, tx.Timestamp);
        return id;
    }

    public static string IdcSend(ReducerContext ctx, string peer, string kind, JsonNode? payload) =>
        IdcSend(new IdcTx(ctx.Db, ctx.Timestamp), peer, kind, payload);

    readonly record struct IdcClaimed(IdcOutbox Row, string MsgId, Timestamp SentAt);

    static List<IdcClaimed> IdcClaimBatch(IdcTx tx, int max)
    {
        var now = tx.Timestamp.MicrosecondsSinceUnixEpoch;
        var rows = tx.Db.IdcOutbox.Iter().Where(r => !r.Dead).OrderBy(r => r.Id).ToList();
        bool Eligible(IdcOutbox r) =>
            !(r.InFlightUntil is { } t && t.MicrosecondsSinceUnixEpoch > now) && r.NextAttempt.MicrosecondsSinceUnixEpoch <= now;
        var seen = new HashSet<string>();
        string? peer = null;
        foreach (var r in rows)
        {
            if (!seen.Add(r.Peer)) continue;
            if (Eligible(r)) { peer = r.Peer; break; }
        }
        var batch = new List<IdcClaimed>();
        if (peer is null) return batch;
        foreach (var r in rows.Where(x => x.Peer == peer))
        {
            if (batch.Count == max || !Eligible(r)) break;
            var leased = r;
            leased.InFlightUntil = new Timestamp(now + IdcLeaseUs);
            tx.Db.IdcOutbox.Id.Update(leased);
            batch.Add(new IdcClaimed(r, IdcMsgId(tx, r.Id), tx.Timestamp));
        }
        return batch;
    }

    readonly record struct IdcDelivery(bool Ok, bool Dead, bool Repair, string Detail);

    /// <summary>Scheduled procedure: deliver everything that's due, then reschedule for the next retry.</summary>
    [SpacetimeDB.Procedure]
    public static void IdcFlush(ProcedureContext ctx, IdcFlushJob job)
    {
        var max = ctx.WithTx(_ => IdcEnv.IDC_TRANSPORT == "reducer" ? 1 : IdcBatch);
        for (var i = 0; i < 200; i++)
        {
            var batch = ctx.WithTx(tx => IdcClaimBatch(new IdcTx(tx.Db, tx.Timestamp), max));
            if (batch.Count == 0) break;
            var results = IdcDeliver(ctx, batch);
            ctx.WithTx(tx =>
            {
                for (var j = 0; j < batch.Count; j++) IdcFinish(new IdcTx(tx.Db, tx.Timestamp), batch[j], results[j]);
                return 0;
            });
        }
        ctx.WithTx(tx =>
        {
            long? next = null;
            foreach (var r in tx.Db.IdcOutbox.Iter())
            {
                if (r.Dead) continue;
                var at = r.NextAttempt.MicrosecondsSinceUnixEpoch;
                if (r.InFlightUntil is { } t && t.MicrosecondsSinceUnixEpoch > at) at = t.MicrosecondsSinceUnixEpoch;
                if (next is null || at < next) next = at;
            }
            if (next is { } n)
            {
                var now = tx.Timestamp.MicrosecondsSinceUnixEpoch;
                IdcScheduleFlush(new IdcTx(tx.Db, tx.Timestamp), new Timestamp(Math.Max(n, now)));
            }
            return 0;
        });
    }

    static void IdcFinish(IdcTx tx, IdcClaimed c, IdcDelivery result)
    {
        if (tx.Db.IdcOutbox.Id.Find(c.Row.Id) is not { } row) return;
        var transport = IdcEnv.IDC_TRANSPORT;
        if (result.Ok)
        {
            tx.Db.IdcOutbox.Id.Delete(row.Id);
            var latency = tx.Timestamp.MicrosecondsSinceUnixEpoch - row.Created.MicrosecondsSinceUnixEpoch;
            IdcWriteLog(tx, "out", row.Peer, row.Kind, transport, c.MsgId, "delivered", result.Detail, latency);
        }
        else if (result.Dead)
        {
            row.Dead = true;
            row.InFlightUntil = null;
            row.LastError = result.Detail;
            tx.Db.IdcOutbox.Id.Update(row);
            IdcWriteLog(tx, "out", row.Peer, row.Kind, transport, c.MsgId, "dead", result.Detail);
        }
        else
        {
            if (result.Repair || result.Detail.Contains("is not a known peer"))
            {
                // The peer forgot us (e.g. its data was reset): pair again.
                tx.Db.IdcPeerToken.Peer.Delete(row.Peer);
                tx.Db.IdcPairJob.Insert(new IdcPairJob { ScheduledAt = new ScheduleAt.Time(tx.Timestamp), Attempt = 0 });
            }
            row.Attempts += 1;
            var backoffMs = Math.Min(250L << (int)Math.Min(row.Attempts, 16), IdcMaxBackoffMs);
            row.NextAttempt = new Timestamp(tx.Timestamp.MicrosecondsSinceUnixEpoch + backoffMs * 1000);
            row.InFlightUntil = null;
            row.LastError = result.Detail;
            tx.Db.IdcOutbox.Id.Update(row);
            IdcWriteLog(tx, "out", row.Peer, row.Kind, transport, c.MsgId, "retry",
                $"attempt {row.Attempts} failed, retrying in {backoffMs} ms: {result.Detail}");
        }
    }

    static List<IdcDelivery> IdcDeliver(ProcedureContext ctx, List<IdcClaimed> batch)
    {
        var peerName = batch[0].Row.Peer;
        var (self, secret, transport, peersRaw, token) = ctx.WithTx(tx =>
            (IdcEnv.IDC_SELF, IdcEnv.IDC_SECRET, IdcEnv.IDC_TRANSPORT, IdcEnv.IDC_PEERS, tx.Db.IdcPeerToken.Peer.Find(peerName)?.Token));
        List<IdcDelivery> All(IdcDelivery d) => batch.Select(_ => d).ToList();

        var peer = IdcPeers(peersRaw).FirstOrDefault(p => p.Name == peerName);
        if (peer.Name is null) return All(new(false, false, false, $"unknown peer `{peerName}`"));

        var envelopes = new JsonArray();
        foreach (var c in batch)
        {
            envelopes.Add((JsonNode)new JsonObject
            {
                ["id"] = c.MsgId, ["from"] = self, ["to"] = peer.Name, ["kind"] = c.Row.Kind,
                ["payload"] = JsonNode.Parse(c.Row.Payload), ["sent_at"] = c.SentAt.MicrosecondsSinceUnixEpoch,
            });
        }

        HttpRequest request;
        if (transport == "reducer")
        {
            if (token is null) return All(new(false, false, false, "not paired with peer yet"));
            // JsonArray.Add<T> needs reflection metadata (trimmed away on wasi), so add a JsonValue explicitly.
            var args = new JsonArray { (JsonNode?)JsonValue.Create(envelopes[0]!.ToJsonString()) };
            request = new HttpRequest
            {
                Uri = $"{peer.Base}/call/idc_receive", Method = HttpMethod.Post, Timeout = IdcHttpTimeout,
                Headers = new() { new("content-type", "application/json"), new("authorization", $"Bearer {token}") },
                Body = HttpBody.FromString(args.ToJsonString()),
            };
        }
        else
        {
            var body = envelopes.ToJsonString();
            request = new HttpRequest
            {
                Uri = $"{peer.Base}/route/idc/inbox", Method = HttpMethod.Post, Timeout = IdcHttpTimeout,
                Headers = new()
                {
                    new("content-type", "application/json"),
                    new(IdcSignatureHeader, IdcSign(secret, batch[0].SentAt, Encoding.UTF8.GetBytes(body))),
                },
                Body = HttpBody.FromString(body),
            };
        }

        return ctx.Http.Send(request).Match(
            response =>
            {
                var status = response.StatusCode;
                var text = response.Body.ToStringUtf8Lossy();
                if (status < 200 || status >= 300)
                {
                    var detail = $"{status} {(text.Length > 300 ? text[..300] : text)}";
                    var permanent = status >= 400 && status < 500 && status is not (401 or 404 or 408 or 429);
                    // 530 is how /call reports a reducer error: "not a known peer" means pair again.
                    permanent |= status == 530 && !text.Contains("is not a known peer");
                    return All(new(false, permanent, false, detail));
                }
                if (transport == "reducer") return All(new(true, false, false, $"{status} via idc_receive"));
                JsonArray? results = null;
                try { results = JsonNode.Parse(text)?["results"] as JsonArray; } catch { }
                return batch.Select((_, i) =>
                {
                    var r = results is not null && i < results.Count ? results[i] : null;
                    if (r is null) return new IdcDelivery(false, false, false, "peer returned no result for this message");
                    if (r["ok"]?.GetValue<bool>() == true)
                        return new IdcDelivery(true, false, false,
                            $"{status} batch of {batch.Count}{(r["duplicate"]?.GetValue<bool>() == true ? " (duplicate)" : "")}");
                    return new IdcDelivery(false, true, false, $"rejected: {r["error"]?.GetValue<string>() ?? "?"}");
                }).ToList();
            },
            error => All(new(false, false, false, $"transport error: {error.Message}")));
    }

    // -----------------------------------------------------------------------
    // Receiving
    // -----------------------------------------------------------------------

    sealed record IdcEnvelope(string Id, string From, string To, string Kind, JsonNode? Payload, long SentAt);

    static IdcEnvelope IdcParseEnvelope(JsonNode? node)
    {
        if (node is not JsonObject o) throw new Exception("bad envelope: not an object");
        string Str(string k) => o[k]?.GetValue<string>() ?? throw new Exception($"bad envelope: missing {k}");
        var sentAt = o["sent_at"] is JsonValue v ? (long)v.GetValue<double>() : 0;
        return new IdcEnvelope(Str("id"), Str("from"), Str("to"), Str("kind"), o["payload"]?.DeepClone(), sentAt);
    }

    /// <summary>Dedupe + apply inside the caller's transaction. Returns true for a duplicate.</summary>
    static bool IdcReceiveIn(IdcTx tx, IdcEnvelope env, string transport)
    {
        var me = IdcEnv.IDC_SELF;
        if (env.To != me) throw new Exception($"message addressed to `{env.To}`, this is `{me}`");
        var key = $"{env.From}|{env.Id}";
        if (tx.Db.IdcSeen.Key.Find(key) is not null)
        {
            IdcWriteLog(tx, "in", env.From, env.Kind, transport, env.Id, "duplicate", "already applied");
            return true;
        }
        tx.Db.IdcSeen.Insert(new IdcSeen { Key = key, At = tx.Timestamp });
        OnIdcMessage(tx, new IdcMessage(env.Id, env.From, env.Kind, env.Payload));
        IdcWriteLog(tx, "in", env.From, env.Kind, transport, env.Id, "applied", "", tx.Timestamp.MicrosecondsSinceUnixEpoch - env.SentAt);
        return false;
    }

    /// <summary>Transport "route": signed POST from a peer, one envelope or a batch.</summary>
    [SpacetimeDB.HttpHandler]
    public static HttpResponse IdcInbox(HandlerContext ctx, HttpRequest req)
    {
        var body = req.Body.ToBytes();
        var secret = ctx.WithTx(_ => IdcEnv.IDC_SECRET);
        try { IdcVerify(secret, IdcHeader(req, IdcSignatureHeader), body, ctx.Timestamp); }
        catch (Exception e) { return IdcJson(401, IdcError(e.Message)); }

        JsonNode? parsed;
        try { parsed = JsonNode.Parse(body); }
        catch (Exception e) { return IdcJson(400, IdcError($"bad JSON: {e.Message}")); }
        var single = parsed is not JsonArray;
        var items = parsed is JsonArray arr ? arr.ToList() : new List<JsonNode?> { parsed };

        var results = new JsonArray();
        foreach (var item in items)
        {
            string? id = null;
            try
            {
                var env = IdcParseEnvelope(item);
                id = env.Id;
                var duplicate = ctx.WithTx(tx => IdcReceiveIn(new IdcTx(tx.Db, tx.Timestamp), env, "route"));
                results.Add((JsonNode)new JsonObject { ["id"] = id, ["ok"] = true, ["duplicate"] = duplicate });
            }
            catch (Exception e)
            {
                results.Add((JsonNode)new JsonObject { ["id"] = id, ["ok"] = false, ["error"] = e.Message });
            }
        }
        if (single)
        {
            var r = results[0]!;
            return r["ok"]!.GetValue<bool>()
                ? IdcJson(200, new JsonObject { ["duplicate"] = r["duplicate"]!.GetValue<bool>(), ["ok"] = true })
                : IdcJson(422, IdcError(r["error"]!.GetValue<string>()));
        }
        return IdcJson(200, new JsonObject { ["results"] = results });
    }

    /// <summary>Transport "reducer": the caller must be an identity we paired with.</summary>
    [SpacetimeDB.Reducer]
    public static void IdcReceive(ReducerContext ctx, string envelope)
    {
        var known = ctx.Db.IdcKnownPeer.Identity.Find(ctx.Sender) ?? throw new Exception($"{ctx.Sender} is not a known peer");
        var env = IdcParseEnvelope(JsonNode.Parse(envelope));
        if (env.From != known.Name) throw new Exception($"identity belongs to `{known.Name}`, envelope claims `{env.From}`");
        IdcReceiveIn(new IdcTx(ctx.Db, ctx.Timestamp), env, "reducer");
    }

    // -----------------------------------------------------------------------
    // Pairing: automates "secrets table + known identities" in both directions
    // -----------------------------------------------------------------------

    /// <summary>Scheduled procedure: mint an identity on each peer's host and introduce it, signed.</summary>
    [SpacetimeDB.Procedure]
    public static void IdcPair(ProcedureContext ctx, IdcPairJob job)
    {
        var (self, secret, peersRaw) = ctx.WithTx(_ => (IdcEnv.IDC_SELF, IdcEnv.IDC_SECRET, IdcEnv.IDC_PEERS));
        var pending = false;
        foreach (var peer in IdcPeers(peersRaw))
        {
            if (ctx.WithTx(tx => tx.Db.IdcPeerToken.Peer.Find(peer.Name) is not null)) continue;
            try
            {
                var minted = ctx.Http.Send(new HttpRequest
                {
                    Uri = $"{peer.Host}/v1/identity", Method = HttpMethod.Post, Timeout = IdcHttpTimeout,
                }).UnwrapOrThrow();
                if (minted.StatusCode is < 200 or >= 300) throw new Exception($"mint identity: HTTP {minted.StatusCode}");
                var m = JsonNode.Parse(minted.Body.ToStringUtf8Lossy())!;
                var identityHex = m["identity"]!.GetValue<string>();
                var token = m["token"]!.GetValue<string>();

                var body = new JsonObject { ["from"] = self, ["identity"] = identityHex }.ToJsonString();
                var now = ctx.WithTx(tx => tx.Timestamp);
                var res = ctx.Http.Send(new HttpRequest
                {
                    Uri = $"{peer.Base}/route/idc/pair", Method = HttpMethod.Post, Timeout = IdcHttpTimeout,
                    Headers = new() { new("content-type", "application/json"), new(IdcSignatureHeader, IdcSign(secret, now, Encoding.UTF8.GetBytes(body))) },
                    Body = HttpBody.FromString(body),
                }).UnwrapOrThrow();
                if (res.StatusCode is < 200 or >= 300) throw new Exception($"pair: HTTP {res.StatusCode} {res.Body.ToStringUtf8Lossy()}");

                ctx.WithTx(tx =>
                {
                    tx.Db.IdcPeerToken.Peer.Delete(peer.Name);
                    tx.Db.IdcPeerToken.Insert(new IdcPeerToken
                    {
                        Peer = peer.Name, Identity = Identity.FromHexString(identityHex), Token = token, PairedAt = tx.Timestamp,
                    });
                    IdcWriteLog(new IdcTx(tx.Db, tx.Timestamp), "out", peer.Name, "pair", "reducer", "", "paired",
                        $"we are {identityHex[..16]} on {peer.Name}");
                    return 0;
                });
            }
            catch (Exception e)
            {
                pending = true;
                ctx.WithTx(tx =>
                {
                    IdcWriteLog(new IdcTx(tx.Db, tx.Timestamp), "out", peer.Name, "pair", "reducer", "", "retry", e.Message);
                    return 0;
                });
            }
        }
        if (pending)
        {
            var attempt = job.Attempt + 1;
            var backoffUs = Math.Min(500L << (int)Math.Min(attempt, 7), IdcMaxBackoffMs) * 1000;
            ctx.WithTx(tx =>
            {
                tx.Db.IdcPairJob.Insert(new IdcPairJob
                {
                    ScheduledAt = new ScheduleAt.Time(new Timestamp(tx.Timestamp.MicrosecondsSinceUnixEpoch + backoffUs)), Attempt = attempt,
                });
                return 0;
            });
        }
    }

    /// <summary>Peer side of pairing: trust `identity` as peer `from`.</summary>
    [SpacetimeDB.HttpHandler]
    public static HttpResponse IdcPairRoute(HandlerContext ctx, HttpRequest req)
    {
        var body = req.Body.ToBytes();
        var (secret, peersRaw) = ctx.WithTx(_ => (IdcEnv.IDC_SECRET, IdcEnv.IDC_PEERS));
        try { IdcVerify(secret, IdcHeader(req, IdcSignatureHeader), body, ctx.Timestamp); }
        catch (Exception e) { return IdcJson(401, IdcError(e.Message)); }
        string from;
        Identity identity;
        try
        {
            var p = JsonNode.Parse(body)!;
            from = p["from"]!.GetValue<string>();
            identity = Identity.FromHexString(p["identity"]!.GetValue<string>());
        }
        catch (Exception e) { return IdcJson(400, IdcError(e.Message)); }
        if (!IdcPeers(peersRaw).Any(p => p.Name == from)) return IdcJson(403, IdcError($"unknown peer `{from}`"));
        ctx.WithTx(tx =>
        {
            foreach (var k in tx.Db.IdcKnownPeer.Name.Filter(from).ToList()) tx.Db.IdcKnownPeer.Identity.Delete(k.Identity);
            tx.Db.IdcKnownPeer.Insert(new IdcKnownPeer { Identity = identity, Name = from, PairedAt = tx.Timestamp });
            IdcWriteLog(new IdcTx(tx.Db, tx.Timestamp), "in", from, "pair", "reducer", "", "paired", $"{identity.ToString()[..16]} is {from}");
            return 0;
        });
        return IdcJson(200, new JsonObject { ["ok"] = true });
    }

    // -----------------------------------------------------------------------
    // Request/response and SQL pulls (from procedures)
    // -----------------------------------------------------------------------

    /// <summary>Signed synchronous call to a peer's route, e.g. IdcRpc(ctx, "warehouse", "/rpc/stock", payload).</summary>
    public static JsonNode? IdcRpc(ProcedureContext ctx, string peerName, string path, JsonNode? payload)
    {
        var (self, secret, now) = ctx.WithTx(tx => (IdcEnv.IDC_SELF, IdcEnv.IDC_SECRET, tx.Timestamp));
        var peer = ctx.WithTx(_ => IdcFindPeer(peerName));
        var body = new JsonObject { ["from"] = self, ["payload"] = payload?.DeepClone() }.ToJsonString();
        var res = ctx.Http.Send(new HttpRequest
        {
            Uri = $"{peer.Base}/route{path}", Method = HttpMethod.Post, Timeout = IdcHttpTimeout,
            Headers = new() { new("content-type", "application/json"), new(IdcSignatureHeader, IdcSign(secret, now, Encoding.UTF8.GetBytes(body))) },
            Body = HttpBody.FromString(body),
        }).UnwrapOrThrow();
        var text = res.Body.ToStringUtf8Lossy();
        if (res.StatusCode is < 200 or >= 300) throw new Exception($"HTTP {res.StatusCode}: {text}");
        return JsonNode.Parse(text);
    }

    /// <summary>For RPC handlers: verify, then return (from, payload); on failure `error` is the response to return.</summary>
    public static (string From, JsonNode? Payload, HttpResponse? Error) IdcVerifyRpc(HandlerContext ctx, HttpRequest req)
    {
        var body = req.Body.ToBytes();
        var secret = ctx.WithTx(_ => IdcEnv.IDC_SECRET);
        try
        {
            IdcVerify(secret, IdcHeader(req, IdcSignatureHeader), body, ctx.Timestamp);
            var v = JsonNode.Parse(body)!;
            return (v["from"]?.GetValue<string>() ?? "", v["payload"]?.DeepClone(), null);
        }
        catch (Exception e) { return ("", null, IdcJson(401, IdcError(e.Message))); }
    }

    public static HttpResponse IdcRpcReply(JsonNode value) => IdcJson(200, value);

    /// <summary>SQL against a peer's /sql, as our paired identity if we have one.</summary>
    public static JsonNode? IdcSql(ProcedureContext ctx, string peerName, string query)
    {
        var token = ctx.WithTx(tx => tx.Db.IdcPeerToken.Peer.Find(peerName)?.Token);
        var peer = ctx.WithTx(_ => IdcFindPeer(peerName));
        var headers = new List<HttpHeader> { new("content-type", "text/plain") };
        if (token is not null) headers.Add(new("authorization", $"Bearer {token}"));
        var res = ctx.Http.Send(new HttpRequest
        {
            Uri = $"{peer.Base}/sql", Method = HttpMethod.Post, Timeout = IdcHttpTimeout, Headers = headers, Body = HttpBody.FromString(query),
        }).UnwrapOrThrow();
        var text = res.Body.ToStringUtf8Lossy();
        if (res.StatusCode is < 200 or >= 300) throw new Exception($"HTTP {res.StatusCode}: {text}");
        return JsonNode.Parse(text);
    }

    // -----------------------------------------------------------------------
    // Housekeeping and introspection
    // -----------------------------------------------------------------------

    [SpacetimeDB.Reducer]
    public static void IdcPrune(ReducerContext ctx, IdcPruneJob job)
    {
        var cutoff = ctx.Timestamp.MicrosecondsSinceUnixEpoch - IdcSeenRetentionUs;
        foreach (var s in ctx.Db.IdcSeen.Iter().Where(s => s.At.MicrosecondsSinceUnixEpoch < cutoff).ToList())
        {
            ctx.Db.IdcSeen.Key.Delete(s.Key);
        }
    }

    /// <summary>The same IDC summary the Rust and TypeScript sides serve, for dashboards.</summary>
    public static JsonObject IdcStateJson(IdcTx tx)
    {
        var log = new JsonArray();
        foreach (var l in tx.Db.IdcLog.Iter().OrderByDescending(l => l.Id).Take(40))
        {
            log.Add((JsonNode)new JsonObject
            {
                ["id"] = l.Id, ["at_us"] = l.At.MicrosecondsSinceUnixEpoch, ["direction"] = l.Direction, ["peer"] = l.Peer,
                ["kind"] = l.Kind, ["transport"] = l.Transport, ["msg_id"] = l.MsgId, ["event"] = l.Event,
                ["detail"] = l.Detail, ["latency_us"] = l.LatencyUs,
            });
        }
        var outbox = tx.Db.IdcOutbox.Iter().ToList();
        return new JsonObject
        {
            ["self"] = IdcEnv.IDC_SELF,
            ["transport"] = IdcEnv.IDC_TRANSPORT,
            ["outbox_pending"] = outbox.Count(r => !r.Dead),
            ["outbox_dead"] = outbox.Count(r => r.Dead),
            ["has_token_for"] = new JsonArray(tx.Db.IdcPeerToken.Iter().Select(t => (JsonNode)t.Peer).ToArray()),
            ["trusts"] = new JsonArray(tx.Db.IdcKnownPeer.Iter().Select(k => (JsonNode)k.Name).ToArray()),
            ["log"] = log,
        };
    }
}

/// <summary>Small managed SHA-256 / HMAC-SHA256 (System.Security.Cryptography isn't available on wasi).</summary>
static class IdcSha256
{
    static readonly uint[] K =
    {
        0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4, 0xab1c5ed5,
        0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174,
        0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da,
        0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7, 0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967,
        0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85,
        0xa2bfe8a1, 0xa81a664b, 0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070,
        0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
        0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7, 0xc67178f2,
    };

    static uint Rotr(uint x, int n) => (x >> n) | (x << (32 - n));

    public static byte[] Hash(byte[] data)
    {
        uint[] h = { 0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a, 0x510e527f, 0x9b05688c, 0x1f83d9ab, 0x5be0cd19 };
        var bitLen = (ulong)data.LongLength * 8;
        var padded = new byte[((data.Length + 9 + 63) / 64) * 64];
        data.CopyTo(padded, 0);
        padded[data.Length] = 0x80;
        for (var i = 0; i < 8; i++) padded[padded.Length - 1 - i] = (byte)(bitLen >> (8 * i));
        var w = new uint[64];
        for (var chunk = 0; chunk < padded.Length; chunk += 64)
        {
            for (var i = 0; i < 16; i++)
                w[i] = (uint)(padded[chunk + 4 * i] << 24 | padded[chunk + 4 * i + 1] << 16 | padded[chunk + 4 * i + 2] << 8 | padded[chunk + 4 * i + 3]);
            for (var i = 16; i < 64; i++)
            {
                var s0 = Rotr(w[i - 15], 7) ^ Rotr(w[i - 15], 18) ^ (w[i - 15] >> 3);
                var s1 = Rotr(w[i - 2], 17) ^ Rotr(w[i - 2], 19) ^ (w[i - 2] >> 10);
                w[i] = w[i - 16] + s0 + w[i - 7] + s1;
            }
            uint a = h[0], b = h[1], c = h[2], d = h[3], e = h[4], f = h[5], g = h[6], hh = h[7];
            for (var i = 0; i < 64; i++)
            {
                var t1 = hh + (Rotr(e, 6) ^ Rotr(e, 11) ^ Rotr(e, 25)) + ((e & f) ^ (~e & g)) + K[i] + w[i];
                var t2 = (Rotr(a, 2) ^ Rotr(a, 13) ^ Rotr(a, 22)) + ((a & b) ^ (a & c) ^ (b & c));
                hh = g; g = f; f = e; e = d + t1; d = c; c = b; b = a; a = t1 + t2;
            }
            h[0] += a; h[1] += b; h[2] += c; h[3] += d; h[4] += e; h[5] += f; h[6] += g; h[7] += hh;
        }
        var result = new byte[32];
        for (var i = 0; i < 8; i++)
        {
            result[4 * i] = (byte)(h[i] >> 24); result[4 * i + 1] = (byte)(h[i] >> 16);
            result[4 * i + 2] = (byte)(h[i] >> 8); result[4 * i + 3] = (byte)h[i];
        }
        return result;
    }

    public static byte[] Hmac(byte[] key, byte[] message)
    {
        if (key.Length > 64) key = Hash(key);
        var k = new byte[64];
        key.CopyTo(k, 0);
        var inner = new byte[64 + message.Length];
        var outer = new byte[64 + 32];
        for (var i = 0; i < 64; i++) { inner[i] = (byte)(k[i] ^ 0x36); outer[i] = (byte)(k[i] ^ 0x5c); }
        message.CopyTo(inner, 64);
        Hash(inner).CopyTo(outer, 64);
        return Hash(outer);
    }
}
