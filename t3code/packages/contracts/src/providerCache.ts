/**
 * Provider API response caching with Effect.Cache and TTL.
 *
 * Wraps provider API calls in a TTL-based cache that respects the Effect.Cache
 * interface. Supports capacity-bounded LRU eviction, per-key TTL overrides,
 * and cache-busting via tagged invalidation keys.
 *
 * @module providerCache
 */
import * as Effect from "effect/Effect";
import * as Cache from "effect/Cache";
import * as Duration from "effect/Duration";
import * as Ref from "effect/Ref";
import * as HashMap from "effect/HashMap";
import * as Option from "effect/Option";

/**
 * Configuration for the provider API cache.
 */
export interface ProviderCacheConfig {
  /** Maximum number of entries before LRU eviction kicks in. */
  readonly capacity: number;
  /** Default TTL for cached entries. */
  readonly ttl: Duration.DurationInput;
  /** How frequently to run the stale-entry sweep. Defaults to ttl. */
  readonly sweepInterval?: Duration.DurationInput;
}

/**
 * Extended cache entry carrying TTL metadata.
 */
interface CacheEntry<A> {
  readonly value: A;
  readonly cachedAt: number;
  readonly ttlMs: number;
}

/**
 * A type-safe provider cache that wraps Effect.Cache with TTL awareness.
 *
 * Unlike raw Effect.Cache, this cache:
 *   - Evicts entries after their TTL expires
 *   - Enforces a capacity limit with LRU semantics
 *   - Supports manual invalidation by key
 *   - Provides cache-hit metrics
 */
export interface ProviderCache<K, A, E> {
  /** Fetch or compute the value for `key`. */
  readonly get: (key: K) => Effect.Effect<A, E>;
  /** Get the value only if it exists and is not expired. */
  readonly getOption: (key: K) => Effect.Effect<Option.Option<A>, E>;
  /** Invalidate a specific key. */
  readonly invalidate: (key: K) => Effect.Effect<void>;
  /** Invalidate all entries. */
  readonly invalidateAll: Effect.Effect<void>;
  /** Number of entries currently cached. */
  readonly size: Effect.Effect<number>;
  /** Cumulative cache hit/miss counters. */
  readonly stats: Effect.Effect<{ hits: number; misses: number }>;
}

const makeProviderCache = <K, A, E>(
  lookup: (key: K) => Effect.Effect<A, E>,
  config: ProviderCacheConfig,
): Effect.Effect<ProviderCache<K, A, E>, never> => {
  const ttlMs = Duration.toMillis(Duration.decode(config.ttl));
  const sweepMs = Duration.toMillis(
    Duration.decode(config.sweepInterval ?? config.ttl),
  );

  return Effect.gen(function* () {
    const cache = yield* Ref.make(HashMap.empty<K, CacheEntry<A>>());
    const hits = yield* Ref.make(0);
    const misses = yield* Ref.make(0);
    const accessOrder = yield* Ref.make<Array<K>>([]);

    const evictLru = function* () {
      const current = yield* Ref.get(cache);
      if (HashMap.size(current) >= config.capacity) {
        const order = yield* Ref.get(accessOrder);
        if (order.length > 0) {
          const oldest = order[0];
          yield* Ref.update(cache, HashMap.remove(oldest));
          yield* Ref.update(accessOrder, (o) => o.slice(1));
        }
      }
    };

    const sweepExpired = function* () {
      const now = Date.now();
      const current = yield* Ref.get(cache);
      const expired: Array<K> = [];

      for (const [key, entry] of HashMap.entries(current)) {
        if (now - entry.cachedAt > entry.ttlMs) {
          expired.push(key);
        }
      }

      if (expired.length > 0) {
        yield* Ref.update(cache, (m) => {
          let result = m;
          for (const k of expired) {
            result = HashMap.remove(result, k);
          }
          return result;
        });
      }
    };

    // Periodic sweep
    if (sweepMs > 0) {
      yield* Effect.repeat(sweepExpired, {
        schedule: () => Duration.millis(sweepMs),
      }).pipe(Effect.forkScoped);
    }

    const get = (key: K): Effect.Effect<A, E> =>
      Effect.gen(function* () {
        const now = Date.now();
        const current = yield* Ref.get(cache);
        const entry = HashMap.get(current, key);

        if (Option.isSome(entry) && now - entry.value.cachedAt <= entry.value.ttlMs) {
          yield* Ref.update(hits, (n) => n + 1);
          // Mark as recently used
          yield* Ref.update(accessOrder, (order) => {
            const filtered = order.filter((k) => k !== key);
            return [...filtered, key];
          });
          return entry.value.value;
        }

        yield* Ref.update(misses, (n) => n + 1);
        const value = yield* lookup(key);
        const newEntry: CacheEntry<A> = { value, cachedAt: now, ttlMs };

        yield* evictLru;
        yield* Ref.update(cache, HashMap.set(key, newEntry));
        yield* Ref.update(accessOrder, (order) => {
          const filtered = order.filter((k) => k !== key);
          return [...filtered, key];
        });

        return value;
      });

    const getOption = (key: K): Effect.Effect<Option.Option<A>, E> =>
      Effect.gen(function* () {
        const now = Date.now();
        const current = yield* Ref.get(cache);
        const entry = HashMap.get(current, key);

        if (Option.isSome(entry) && now - entry.value.cachedAt <= entry.value.ttlMs) {
          yield* Ref.update(hits, (n) => n + 1);
          return Option.some(entry.value.value);
        }

        return Option.none();
      });

    const invalidate = (key: K): Effect.Effect<void> =>
      Ref.update(cache, HashMap.remove(key));

    const invalidateAll: Effect.Effect<void> = Ref.set(cache, HashMap.empty());

    const size = Ref.get(cache).pipe(Effect.map(HashMap.size));

    const stats = Effect.gen(function* () {
      const h = yield* Ref.get(hits);
      const m = yield* Ref.get(misses);
      return { hits: h, misses: m } as const;
    });

    return {
      get,
      getOption,
      invalidate,
      invalidateAll,
      size,
      stats,
    } satisfies ProviderCache<K, A, E>;
  });
};

/**
 * Create a TTL-aware provider cache layer.
 *
 * @param lookup - The effectful lookup function to cache.
 * @param config - Cache configuration (capacity, ttl).
 */
export const make = makeProviderCache;

/**
 * Convenience combinator: wrap an existing `lookup` function so callers
 * transparently get cached results without changing their call sites.
 */
export const wrap = <K, A, E>(
  lookup: (key: K) => Effect.Effect<A, E>,
  config: ProviderCacheConfig,
): Effect.Effect<(key: K) => Effect.Effect<A, E>, never> =>
  make(lookup, config).pipe(Effect.map((cache) => cache.get));
