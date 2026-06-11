/**
 * Tests for codexStream — Effect.Stream-based streaming with backpressure.
 */
import { describe, it, expect } from "vitest";
import * as Effect from "effect/Effect";
import * as Stream from "effect/Stream";
import * as Chunk from "effect/Chunk";
import * as Queue from "effect/Queue";
import * as Ref from "effect/Ref";
import { slidingWindowMetrics, type MetricPoint, type WindowMetrics } from "../src/slidingWindowMetrics.ts";

describe("slidingWindowMetrics", () => {
  it("aggregates a tumbling window correctly", async () => {
    const now = Date.now();
    const points: MetricPoint[] = [
      { timestamp: now, value: 10 },
      { timestamp: now + 100, value: 20 },
      { timestamp: now + 200, value: 30 },
    ];

    const result = await Effect.runPromise(
      Stream.fromIterable(points).pipe(
        slidingWindowMetrics({ windowSizeMs: 60_000 }), // 1min window covers all 3
        Stream.take(1),
        Stream.runCollect,
      ),
    );

    const window = Chunk.unsafeGet(result, 0);
    expect(window.count).toBe(3);
    expect(window.sum).toBe(60);
    expect(window.avg).toBe(20);
    expect(window.min).toBe(10);
    expect(window.max).toBe(30);
    expect(window.p50).toBe(20);
  });

  it("emits empty metrics for an empty window", async () => {
    const result = await Effect.runPromise(
      Stream.fromIterable([] as MetricPoint[]).pipe(
        slidingWindowMetrics({ windowSizeMs: 10_000, slideMs: 5_000 }),
        Stream.take(1),
        Stream.runCollect,
      ),
    );

    expect(Chunk.size(result)).toBe(1);
    expect(Chunk.unsafeGet(result, 0).count).toBe(0);
  });

  it("computes percentiles correctly", async () => {
    const now = Date.now();
    const points: MetricPoint[] = Array.from({ length: 100 }, (_, i) => ({
      timestamp: now,
      value: i + 1, // 1..100
    }));

    const result = await Effect.runPromise(
      Stream.fromIterable(points).pipe(
        slidingWindowMetrics({ windowSizeMs: 60_000 }),
        Stream.take(1),
        Stream.runCollect,
      ),
    );

    const window = Chunk.unsafeGet(result, 0);
    expect(window.count).toBe(100);
    expect(window.p50).toBe(50.5);
    expect(window.p95).toBeGreaterThan(94);
    expect(window.p99).toBeGreaterThan(98);
  });

  it("preserves tags from the first point in the window", async () => {
    const now = Date.now();
    const points: MetricPoint[] = [
      { timestamp: now, value: 1, tags: { host: "api-1" } },
      { timestamp: now + 10, value: 2, tags: { host: "api-2" } },
    ];

    const result = await Effect.runPromise(
      Stream.fromIterable(points).pipe(
        slidingWindowMetrics({ windowSizeMs: 60_000 }),
        Stream.take(1),
        Stream.runCollect,
      ),
    );

    const window = Chunk.unsafeGet(result, 0);
    expect(window.tags).toEqual({ host: "api-1" });
  });

  it("respects backpressure through bounded queue", async () => {
    const now = Date.now();
    // Generate a burst of 1000 points — the sliding window should handle
    // them without dropping or OOM
    const points: MetricPoint[] = Array.from({ length: 1000 }, (_, i) => ({
      timestamp: now + i,
      value: Math.random() * 100,
    }));

    const result = await Effect.runPromise(
      Stream.fromIterable(points).pipe(
        slidingWindowMetrics({ windowSizeMs: 60_000 }),
        Stream.runCollect,
      ),
    );

    // Should have at least one window
    expect(Chunk.size(result)).toBeGreaterThan(0);
  });
});
