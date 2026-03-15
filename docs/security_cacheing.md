
## Security Snapshot Cache

`SecurityRepository` holds an in-process snapshot of all `dim_security` rows in
two lookup maps (by ID and by ticker). The snapshot is an immutable `securitySnapshot`
value stored in an `atomic.Pointer`, so reads are lock-free and impose no mutex
contention on the hot path.

The reason to keep dim_security lookups is to avoid hitting the database every time
you need an ID or a security from ticker. The reason to cache it is because you don't
want a 150ms database fetch and populate every inbound API request. This is common data
modified at best once per day.

### Population

The snapshot is populated lazily on the first call to `GetAllSecurities`, but is
also warmed proactively at startup by `PrefetchService.runCatchup` before any
request is served (ahead of all price warm-up guards).

### TTL

The snapshot expires 1 hour after it was built (`securityCacheTTL`). On the next
`GetAllSecurities` call after expiry the maps are rebuilt from the database and a
fresh snapshot is stored.

### Invalidation

Write operations that mutate `dim_security` (`BulkCreateDimSecurities`,
`UpdateISINsForExisting`) call `ClearCache()` via `defer` before returning.
`ClearCache` stores `nil`, so the next `GetAllSecurities` call always rebuilds from
the database after a write — regardless of whether the TTL has elapsed.

### Fast-path coverage

When the snapshot is valid, these functions skip the database entirely:

| Function | Snapshot use |
|---|---|
| `GetAllSecurities` | returns `snap.byID`, `snap.byTicker` directly |
| `GetByID` | `snap.byID[id]` lookup |
| `GetByTicker` | `snap.byTicker[ticker]` → `PreferUSListing` |
| `GetMultipleByTickers` | per-ticker slice lookup from `snap.byTicker` |

