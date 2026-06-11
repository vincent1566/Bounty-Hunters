/**
 * Tests for ProviderCache — TTL-aware API response caching with Effect.Cache.
 */
import { describe, it, expect, vi } from "vitest";
import * as Effect from "effect/Effect";
import * as Duration from "effect/Duration";
import { make, type ProviderCacheConfig } from "../src/providerCache.ts";

const config: ProviderCacheConfig = {
  capacity: 10,
  ttl: Duration.minutes(5),
};

describe("ProviderCache", () => {
  it("caches lookup results and returns them on subsequent calls", async () => {
    const fn = vi.fn((key: string) => Effect.succeed(`result-${key}`));

    const cache = await Effect.runPromise(make(fn, config));

    const r1 = await Effect.runPromise(cache.get("foo"));
    const r2 = await Effect.runPromise(cache.get("foo"));
    const r3 = await Effect.runPromise(cache.get("foo"));

    expect(r1).toBe("result-foo");
    expect(r2).toBe("result-foo");
    expect(r3).toBe("result-foo");
    expect(fn).toHaveBeenCalledTimes(1); // Only first call hits lookup
  });

  it("returns different results for different keys", async () => {
    const fn = (key: string) => Effect.succeed(`val-${key}`);
    const cache = await Effect.runPromise(make(fn, config));

    expect(await Effect.runPromise(cache.get("a"))).toBe("val-a");
    expect(await Effect.runPromise(cache.get("b"))).toBe("val-b");
  });

  it("getOption returns none for uncached key", async () => {
    const fn = (_key: string) => Effect.succeed("unused");
    const cache = await Effect.runPromise(make(fn, config));

    const result = await Effect.runPromise(cache.getOption("missing"));
    expect(result).toEqual(Effect.option.none());
  });

  it("getOption returns some for cached key", async () => {
    const fn = (key: string) => Effect.succeed(`cached-${key}`);
    const cache = await Effect.runPromise(make(fn, config));

    await Effect.runPromise(cache.get("hit"));
    const result = await Effect.runPromise(cache.getOption("hit"));
    expect(result._tag).toBe("Some");
  });

  it("invalidate removes a specific key", async () => {
    const fn = vi.fn((_key: string) => Effect.succeed("fresh"));
    const cache = await Effect.runPromise(make(fn, config));

    await Effect.runPromise(cache.get("x"));
    await Effect.runPromise(cache.invalidate("x"));
    await Effect.runPromise(cache.get("x"));

    expect(fn).toHaveBeenCalledTimes(2);
  });

  it("invalidateAll clears the entire cache", async () => {
    const fn = vi.fn((_key: string) => Effect.succeed("data"));
    const cache = await Effect.runPromise(make(fn, config));

    await Effect.runPromise(cache.get("1"));
    await Effect.runPromise(cache.get("2"));
    await Effect.runPromise(cache.invalidateAll);
    await Effect.runPromise(cache.get("1"));
    await Effect.runPromise(cache.get("2"));

    expect(fn).toHaveBeenCalledTimes(4); // 2 initial + 2 after clear
  });

  it("evicts oldest entry when capacity is reached (LRU)", async () => {
    const smallConfig: ProviderCacheConfig = { capacity: 2, ttl: Duration.hours(1) };
    const fn = (key: string) => Effect.succeed(key);
    const cache = await Effect.runPromise(make(fn, smallConfig));

    await Effect.runPromise(cache.get("a")); // [a]
    await Effect.runPromise(cache.get("b")); // [a, b]
    await Effect.runPromise(cache.get("c")); // [b, c] — a evicted

    const size = await Effect.runPromise(cache.size);
    expect(size).toBe(2);

    // a should miss cache now
    const result = await Effect.runPromise(cache.getOption("a"));
    expect(result._tag).toBe("None");
  });

  it("tracks cache hit/miss stats", async () => {
    const fn = (key: string) => Effect.succeed(key);
    const cache = await Effect.runPromise(make(fn, config));

    await Effect.runPromise(cache.get("a")); // miss + hit
    await Effect.runPromise(cache.get("a")); // hit
    await Effect.runPromise(cache.get("a")); // hit

    const stats = await Effect.runPromise(cache.stats);
    expect(stats.hits).toBe(2);
    expect(stats.misses).toBe(1);
  });

  it("expires entries after TTL", async () => {
    const shortConfig: ProviderCacheConfig = {
      capacity: 5,
      ttl: Duration.millis(50),
    };
    const fn = vi.fn((_key: string) => Effect.succeed("val"));
    const cache = await Effect.runPromise(make(fn, shortConfig));

    await Effect.runPromise(cache.get("k"));
    expect(fn).toHaveBeenCalledTimes(1);

    // Wait for TTL to expire
    await new Promise((r) => setTimeout(r, 60));

    await Effect.runPromise(cache.get("k"));
    expect(fn).toHaveBeenCalledTimes(2); // Expired, re-fetched
  });
});
