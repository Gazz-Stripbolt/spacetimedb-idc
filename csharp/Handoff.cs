// Handoff.cs: move an entity between databases safely, on top of Idc.cs.
//
// The C# twin of rust/handoff.rs, wire-compatible with it and the TypeScript submodule.
//
// A plain "copy to B, then delete on A" can't be made safe without distributed transactions:
// crash between the two steps and you get a duplicate or a lost entity. What can be made safe
// is an ownership state machine where every step is one transaction plus an idc message:
//
//     source A                                  target B
//     HandoffStart: lock entity ── offer ─────▶ validate → import as *pending*
//                                               (or ◀── reject: A unlocks)
//     remove our copy ◀────────── accept ────── (checksum echoed back)
//              └───────────────── release ────▶ activate: now live on B
//
// Invariant: at most one live copy, and never zero copies. Cancels and timeouts travel in the
// same ordered queue as the offer, so the target always sees offer before cancel.
//
// Drop this file next to Idc.cs, call HandoffInit(ctx) from your Init reducer, route handoff
// messages first in OnIdcMessage:
//
//     public static partial void OnIdcMessage(IdcTx tx, IdcMessage msg)
//     {
//         if (HandoffOnIdcMessage(tx, msg)) return;
//         ...
//     }
//
// and implement the six hooks declared below (HandoffValidate, HandoffImport, HandoffActivate,
// HandoffDiscard, HandoffRemove, HandoffReturned). Use HandoffIsLocked in your own reducers to
// refuse moves, trades and edits while an entity is in transit.

#pragma warning disable STDB_UNSTABLE
#nullable enable

using System.Text;
using System.Text.Json.Nodes;
using SpacetimeDB;

public static partial class Module
{
    /// <summary>How long the source waits for an answer before it asks the target to cancel.</summary>
    public static readonly TimeSpan HandoffDefaultTimeout = TimeSpan.FromSeconds(30);

    // Statuses. Source: offered → released, or offered → cancelling → cancelled, or offered → rejected.
    // Target: accepted → active, or accepted → cancelled, or rejected.
    public const string HandoffOffered = "offered";
    public const string HandoffCancelling = "cancelling";
    public const string HandoffCancelled = "cancelled";
    public const string HandoffReleased = "released";
    public const string HandoffRejected = "rejected";
    public const string HandoffAccepted = "accepted";
    public const string HandoffActive = "active";

    /// <summary>One row per transfer, on each side. No payloads, so it's public.</summary>
    [SpacetimeDB.Table(Accessor = "Handoff", Public = true)]
    public partial struct Handoff
    {
        [SpacetimeDB.PrimaryKey]
        public string Id;
        [SpacetimeDB.Index.BTree]
        public string Entity;
        /// <summary>"out" (we're the source) or "in" (we're the target).</summary>
        public string Role;
        public string Peer;
        public string Status;
        /// <summary>Lowercase hex SHA-256 of the data text, as sent.</summary>
        public string Checksum;
        public Timestamp Started;
        public Timestamp Updated;
        /// <summary>Why it ended the way it did.</summary>
        public string Detail;
    }

    [SpacetimeDB.Table(Accessor = "HandoffSeq")]
    public partial struct HandoffSeq
    {
        [SpacetimeDB.PrimaryKey]
        public byte Key;
        public ulong Next;
    }

    [SpacetimeDB.Table(Accessor = "HandoffTimeoutJob", Scheduled = nameof(HandoffTimeout), ScheduledAt = nameof(HandoffTimeoutJob.ScheduledAt))]
    public partial struct HandoffTimeoutJob
    {
        [SpacetimeDB.PrimaryKey, SpacetimeDB.AutoInc]
        public ulong ScheduledId;
        public ScheduleAt ScheduledAt;
        public string TransferId;
    }

    /// <summary>Target: may we take this entity? Read-only. Return null to accept, or the reason to reject.</summary>
    public static partial string? HandoffValidate(IdcTx tx, string from, string entity, JsonNode data);
    /// <summary>Target: store it as pending (not playable yet). Must not throw: validate first.</summary>
    public static partial void HandoffImport(IdcTx tx, string from, string entity, JsonNode data);
    /// <summary>Target: the source has let go, make it live.</summary>
    public static partial void HandoffActivate(IdcTx tx, string entity);
    /// <summary>Target: the transfer was cancelled after import, drop the pending copy.</summary>
    public static partial void HandoffDiscard(IdcTx tx, string entity);
    /// <summary>Source: the target has it, delete our copy.</summary>
    public static partial void HandoffRemove(IdcTx tx, string entity);
    /// <summary>Source: the transfer failed (rejected, cancelled, timed out); the entity is ours and unlocked again.</summary>
    public static partial void HandoffReturned(IdcTx tx, string entity, string reason);

    /// <summary>Call from your Init reducer. Idempotent.</summary>
    public static void HandoffInit(ReducerContext ctx)
    {
        if (ctx.Db.HandoffSeq.Key.Find(0) is null) ctx.Db.HandoffSeq.Insert(new HandoffSeq { Key = 0, Next = 1 });
    }

    public static string HandoffChecksum(string data) => IdcHex(IdcSha256.Hash(Encoding.UTF8.GetBytes(data)));

    /// <summary>Is `entity` in transit (locked on the source, or pending on the target)?</summary>
    public static bool HandoffIsLocked(IdcTx tx, string entity) =>
        tx.Db.Handoff.Entity.Filter(entity).Any(h => h.Status is HandoffOffered or HandoffCancelling or HandoffAccepted);

    public static bool HandoffIsLocked(ReducerContext ctx, string entity) => HandoffIsLocked(new IdcTx(ctx.Db, ctx.Timestamp), entity);

    /// <summary>The most recent transfer of `entity` this database took part in.</summary>
    public static Handoff? HandoffLatest(IdcTx tx, string entity) =>
        tx.Db.Handoff.Entity.Filter(entity).OrderByDescending(h => h.Started.MicrosecondsSinceUnixEpoch).Cast<Handoff?>().FirstOrDefault();

    /// <summary>
    /// Lock `entity` and offer it, with `data`, to `peer`. Returns the transfer id. Runs in your
    /// reducer's transaction: if your reducer fails afterwards, nothing was locked or sent.
    /// If the target hasn't answered within `timeout` (default 30 s), the transfer is cancelled.
    /// </summary>
    public static string HandoffStart(IdcTx tx, string peer, string entity, JsonNode data, TimeSpan? timeout = null)
    {
        if (string.IsNullOrEmpty(entity)) throw new Exception("entity id is empty");
        if (IdcDurabilityUnsafe)
            throw new Exception("handoffs need IDC_DURABILITY=confirmed: with `unsafe`, a crash can duplicate or lose the entity");
        var me = IdcEnv.IDC_SELF;
        if (peer == me) throw new Exception("can't hand off to ourselves");
        if (!IdcPeers(IdcEnv.IDC_PEERS).Any(p => p.Name == peer)) throw new Exception($"unknown peer `{peer}`");
        if (HandoffIsLocked(tx, entity)) throw new Exception($"`{entity}` is already in transit");
        HandoffForgetFinished(tx, entity);
        var seq = tx.Db.HandoffSeq.Key.Find(0) ?? throw new Exception("HandoffInit wasn't called");
        var epoch = tx.Db.IdcState.Key.Find(0)?.Epoch ?? "";
        var id = $"{me}-{epoch}-{seq.Next}";
        seq.Next += 1;
        tx.Db.HandoffSeq.Key.Update(seq);

        var text = data.ToJsonString();
        var sum = HandoffChecksum(text);
        tx.Db.Handoff.Insert(new Handoff
        {
            Id = id, Entity = entity, Role = "out", Peer = peer, Status = HandoffOffered, Checksum = sum,
            Started = tx.Timestamp, Updated = tx.Timestamp, Detail = "",
        });
        IdcSend(tx, peer, "handoff.offer", new JsonObject { ["id"] = id, ["entity"] = entity, ["data"] = text, ["checksum"] = sum });
        var due = tx.Timestamp.MicrosecondsSinceUnixEpoch + (long)(timeout ?? HandoffDefaultTimeout).TotalMicroseconds;
        tx.Db.HandoffTimeoutJob.Insert(new HandoffTimeoutJob { ScheduledAt = new ScheduleAt.Time(new Timestamp(due)), TransferId = id });
        return id;
    }

    public static string HandoffStart(ReducerContext ctx, string peer, string entity, JsonNode data, TimeSpan? timeout = null) =>
        HandoffStart(new IdcTx(ctx.Db, ctx.Timestamp), peer, entity, data, timeout);

    /// <summary>
    /// Ask the target to drop an outgoing transfer. Only possible until we've seen its accept;
    /// after that the entity already belongs to the target. The entity stays locked until the
    /// target confirms, then HandoffReturned runs.
    /// </summary>
    public static void HandoffCancel(IdcTx tx, string transferId, string reason)
    {
        var found = tx.Db.Handoff.Id.Find(transferId);
        if (found is not { Role: "out" } h) throw new Exception($"no outgoing transfer `{transferId}`");
        switch (h.Status)
        {
            case HandoffOffered:
                h.Status = HandoffCancelling;
                h.Detail = reason;
                h.Updated = tx.Timestamp;
                IdcSend(tx, h.Peer, "handoff.cancel", new JsonObject { ["id"] = h.Id, ["reason"] = reason });
                tx.Db.Handoff.Id.Update(h);
                return;
            case HandoffCancelling:
                return;
            case HandoffReleased:
                throw new Exception("too late: the target already has it");
            default:
                throw new Exception($"transfer is already {h.Status}");
        }
    }

    public static void HandoffCancel(ReducerContext ctx, string transferId, string reason) =>
        HandoffCancel(new IdcTx(ctx.Db, ctx.Timestamp), transferId, reason);

    /// <summary>Fires when an offer's timeout is up. A no-op unless the target still hasn't answered.</summary>
    [SpacetimeDB.Reducer]
    public static void HandoffTimeout(ReducerContext ctx, HandoffTimeoutJob job)
    {
        if (ctx.Sender != ctx.DatabaseIdentity) throw new Exception("scheduled only");
        if (ctx.Db.Handoff.Id.Find(job.TransferId) is { Status: HandoffOffered })
        {
            HandoffCancel(ctx, job.TransferId, "timed out");
        }
    }

    /// <summary>Route handoff.* messages here first. Returns false for kinds that aren't ours.</summary>
    public static bool HandoffOnIdcMessage(IdcTx tx, IdcMessage msg)
    {
        if (!msg.Kind.StartsWith("handoff.", StringComparison.Ordinal)) return false;
        var action = msg.Kind["handoff.".Length..];
        var p = msg.Payload as JsonObject;
        var id = HandoffStr(p, "id") ?? throw new Exception($"{msg.Kind}: missing id");
        switch (action)
        {
            case "offer": HandoffOnOffer(tx, msg.From, id, p!); break;
            case "accept": HandoffOnAccept(tx, msg.From, id, p!); break;
            case "reject": HandoffOnReturned(tx, msg.From, id, HandoffRejected, HandoffStr(p, "reason") ?? "rejected"); break;
            case "cancelled": HandoffOnReturned(tx, msg.From, id, HandoffCancelled, "cancelled"); break;
            case "release": HandoffOnRelease(tx, msg.From, id); break;
            case "cancel": HandoffOnCancel(tx, msg.From, id, HandoffStr(p, "reason") ?? "cancelled"); break;
            default: throw new Exception($"unknown handoff message `{action}`");
        }
        return true;
    }

    // Every handler below is idempotent and refuses with a *message*, never an exception, unless
    // the message itself is malformed. An exception would turn into a dead letter on the sender,
    // and a dead `release` would leave an entity with no live copy.

    static string? HandoffStr(JsonObject? o, string key) =>
        o?[key] is JsonValue v && v.TryGetValue<string>(out var s) ? s : null;

    static Handoff? HandoffFind(IdcTx tx, string id, string role, string peer) =>
        tx.Db.Handoff.Id.Find(id) is { } h && h.Role == role && h.Peer == peer ? h : null;

    static void HandoffSetStatus(IdcTx tx, Handoff h, string status, string detail)
    {
        h.Status = status;
        h.Detail = detail;
        h.Updated = tx.Timestamp;
        tx.Db.Handoff.Id.Update(h);
    }

    /// <summary>Drop finished transfers of `entity`, so the table stays one row per entity in the long run.</summary>
    static void HandoffForgetFinished(IdcTx tx, string entity)
    {
        var done = tx.Db.Handoff.Entity.Filter(entity)
            .Where(h => h.Status is HandoffReleased or HandoffRejected or HandoffCancelled or HandoffActive)
            .Select(h => h.Id)
            .ToList();
        foreach (var id in done) tx.Db.Handoff.Id.Delete(id);
    }

    /// <summary>Target: an entity is offered to us.</summary>
    static void HandoffOnOffer(IdcTx tx, string from, string id, JsonObject p)
    {
        var entity = HandoffStr(p, "entity") ?? throw new Exception("handoff.offer: missing entity");
        var text = HandoffStr(p, "data") ?? throw new Exception("handoff.offer: missing data");
        var sum = HandoffStr(p, "checksum") ?? throw new Exception("handoff.offer: missing checksum");
        // Seen it (or a cancel got here first and left a tombstone): the answer was already sent.
        if (tx.Db.Handoff.Id.Find(id) is not null) return;
        Handoff Record(string status, string detail) => new()
        {
            Id = id, Entity = entity, Role = "in", Peer = from, Status = status, Checksum = sum,
            Started = tx.Timestamp, Updated = tx.Timestamp, Detail = detail,
        };
        string? refusal;
        JsonNode? data = null;
        if (HandoffChecksum(text) != sum) refusal = "checksum mismatch";
        else if (HandoffIsLocked(tx, entity)) refusal = $"`{entity}` is already in transit here";
        else
        {
            try { data = JsonNode.Parse(text); refusal = data is null ? "bad data: null" : null; }
            catch (Exception e) { refusal = $"bad data: {e.Message}"; }
            if (data is not null) refusal = HandoffValidate(tx, from, entity, data);
        }
        if (refusal is null && data is not null)
        {
            HandoffForgetFinished(tx, entity);
            HandoffImport(tx, from, entity, data);
            tx.Db.Handoff.Insert(Record(HandoffAccepted, ""));
            IdcSend(tx, from, "handoff.accept", new JsonObject { ["id"] = id, ["checksum"] = sum });
            return;
        }
        refusal ??= "rejected";
        HandoffForgetFinished(tx, entity);
        tx.Db.Handoff.Insert(Record(HandoffRejected, refusal));
        IdcSend(tx, from, "handoff.reject", new JsonObject { ["id"] = id, ["reason"] = refusal });
    }

    /// <summary>Source: the target has a pending copy. Let go of ours.</summary>
    static void HandoffOnAccept(IdcTx tx, string from, string id, JsonObject p)
    {
        if (HandoffFind(tx, id, "out", from) is not { } h) return;
        // cancelling: the target will discard its copy and confirm. Anything else: a replay.
        if (h.Status != HandoffOffered) return;
        if (HandoffStr(p, "checksum") != h.Checksum)
        {
            HandoffCancel(tx, id, "checksum mismatch on accept");
            return;
        }
        HandoffRemove(tx, h.Entity);
        IdcSend(tx, from, "handoff.release", new JsonObject { ["id"] = id });
        HandoffSetStatus(tx, h, HandoffReleased, "");
    }

    /// <summary>Source: the transfer failed (reject) or the target confirmed our cancel (cancelled).</summary>
    static void HandoffOnReturned(IdcTx tx, string from, string id, string status, string reason)
    {
        if (HandoffFind(tx, id, "out", from) is not { } h) return;
        if (h.Status is not (HandoffOffered or HandoffCancelling)) return;
        if (h.Status == HandoffCancelling && !string.IsNullOrEmpty(h.Detail)) reason = h.Detail;
        HandoffSetStatus(tx, h, status, reason);
        HandoffReturned(tx, h.Entity, reason);
    }

    /// <summary>Target: the source has deleted its copy. Ours is the only one now.</summary>
    static void HandoffOnRelease(IdcTx tx, string from, string id)
    {
        if (HandoffFind(tx, id, "in", from) is not { Status: HandoffAccepted } h) return;
        HandoffActivate(tx, h.Entity);
        HandoffSetStatus(tx, h, HandoffActive, "");
    }

    /// <summary>
    /// Target: the source wants to call it off. It only asks before it has seen our accept,
    /// so whatever we hold for this transfer is still pending and can be dropped.
    /// </summary>
    static void HandoffOnCancel(IdcTx tx, string from, string id, string reason)
    {
        var found = tx.Db.Handoff.Id.Find(id);
        if (found is { } h)
        {
            if (h.Role != "in" || h.Peer != from) return;
            if (h.Status == HandoffAccepted)
            {
                HandoffDiscard(tx, h.Entity);
                HandoffSetStatus(tx, h, HandoffCancelled, reason);
            }
            // Already active can't happen (release only follows accept, and the source never
            // sends both release and cancel). Rejected/cancelled: just confirm again.
            else if (h.Status == HandoffActive)
            {
                Log.Warn($"handoff {id}: cancel after activation ignored");
                return;
            }
        }
        else
        {
            // The offer never got here (e.g. it was refused as malformed). Leave a tombstone so
            // a late offer with this id is ignored.
            tx.Db.Handoff.Insert(new Handoff
            {
                Id = id, Entity = "", Role = "in", Peer = from, Status = HandoffCancelled, Checksum = "",
                Started = tx.Timestamp, Updated = tx.Timestamp, Detail = reason,
            });
        }
        IdcSend(tx, from, "handoff.cancelled", new JsonObject { ["id"] = id });
    }
}
