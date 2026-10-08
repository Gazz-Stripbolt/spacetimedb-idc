# Wire protocol

Everything `idc.rs` (Rust) and `spacetimedb-idc` (TypeScript) send each other. Implement this and your module can
join the same mesh from any language.

## Envelope

```json
{ "id": "65d5040212b97-22", "from": "warehouse", "to": "shop", "kind": "reservation",
  "payload": { "order_id": 7, "ok": true, "note": "reserved 1 × gear" }, "sent_at": 1791448000123456 }
```

| Field | Meaning |
|---|---|
| `id` | Globally unique per sender: `<epoch hex>-<outbox id>`. The epoch is fixed when idc is first set up, so ids stay unique even if the database is wiped. |
| `from` / `to` | Mesh names (`IDC_SELF` on each side). The receiver rejects messages not addressed to it. |
| `kind` / `payload` | Yours. `payload` is any JSON. |
| `sent_at` | Sender clock in microseconds since the Unix epoch, as a JSON number. |

Receivers dedupe on `from|id`, in the same transaction as the effect.

## Signature (route transport, pairing, RPC)

```
X-IDC-Signature: t=<micros>,v1=<hex(HMAC-SHA256(IDC_SECRET, "<micros>." + raw body bytes))>
```

Receivers accept `|now - t| ≤ 5 min`, compare in constant time, and must cope with the header value arriving split
across several header lines (join them with `,`).

## Endpoints every peer exposes

| Method + path (under `/v1/database/<db>`) | Body | Response |
|---|---|---|
| `POST /route/idc/inbox` (signed) | one envelope, or an array of envelopes | single: `200 {"ok":true,"duplicate":bool}` or `422 {"error"}`. Array: `200 {"results":[{"id","ok","duplicate"?,"error"?}, …]}`, in order. `401` if the signature is bad. |
| `POST /route/idc/pair` (signed) | `{"from": "<name>", "identity": "<hex>"}` | `200 {"ok":true}`. `403` if `from` isn't in the receiver's `IDC_PEERS`. |
| `POST /call/idc_receive` (reducer transport) | `["<envelope as a JSON string>"]` with `Authorization: Bearer <token>` | `200`, or `530` with the reducer error. `… is not a known peer` means pair again. |

Sender rules: 2xx = delivered. 401/404/408/429/5xx/transport errors = retry with backoff. Any other 4xx, or a per-message
`ok:false` = dead letter. A 530 from `idc_receive` is a dead letter unless it says `is not a known peer`.

## Pairing (reducer transport)

1. `POST <peer host>/v1/identity` → `{ "identity", "token" }`. Keep the token privately.
2. Signed `POST <peer>/route/idc/pair` with `{ "from": self, "identity" }`. The peer stores identity → name.
3. Call `<peer>/call/idc_receive` with `Authorization: Bearer <token>`. The peer checks `ctx.sender` against its stored
   identities and that the name matches the envelope's `from`.

## RPC

Signed `POST <peer>/route<path>` with `{ "from": self, "payload": … }`. The response body is the answer, as JSON.
