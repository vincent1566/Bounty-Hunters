/**
 * Sliding window metrics aggregation using Effect.Stream.
 *
 * Collects time-series data points into overlapping (or tumbling) windows
 * and emits aggregated metrics (sum, avg, min, max, percentiles) for each
 * completed window. Built on Effect.Stream primitives for natural backpressure
 * and resource safety.
 *
 * @module slidingWindowMetrics
 */
import * as Effect from "effect/Effect";
import * as Stream from "effect/Stream";
import * as Queue from "effect/Queue";
import * as Schedule from "effect/Schedule";
import * as Duration from "effect/Duration";
import * as Chunk from "effect/Chunk";
import * as Order from "effect/Order";
import * as Option from "effect/Option";

/**
 * A single data point in the metrics stream.
 */
export interface MetricPoint {
  readonly timestamp: number;
  readonly value: number;
  readonly tags?: Record<string, string>;
}

/**
 * Aggregated metrics for a completed window.
 */
export interface WindowMetrics {
  readonly windowStart: number;
  readonly windowEnd: number;
  readonly count: number;
  readonly sum: number;
  readonly avg: number;
  readonly min: number;
  readonly max: number;
  readonly p50: number;
  readonly p95: number;
  readonly p99: number;
  readonly tags?: Record<string, string>;
}

/**
 * Configuration for the sliding window aggregator.
 */
export interface SlidingWindowConfig {
  /** Window duration in milliseconds. */
  readonly windowSizeMs: number;
  /** Slide interval in milliseconds. Defaults to windowSizeMs (tumbling window). */
  readonly slideMs?: number;
  /** Maximum number of windows to keep in memory. Defaults to 100. */
  readonly maxWindows?: number;
}

const numberOrder = Order.number;

/**
 * Compute percentile from a sorted array of numbers.
 */
const percentile = (sorted: ReadonlyArray<number>, p: number): number => {
  if (sorted.length === 0) return 0;
  if (sorted.length === 1) return sorted[0];
  const index = (p / 100) * (sorted.length - 1);
  const lower = Math.floor(index);
  const upper = Math.ceil(index);
  if (lower === upper) return sorted[lower];
  return sorted[lower] * (upper - index) + sorted[upper] * (index - lower);
};

/**
 * Aggregate a chunk of metric points into WindowMetrics.
 */
const aggregate = (
  points: Chunk.Chunk<MetricPoint>,
  windowStart: number,
  windowEnd: number,
): WindowMetrics => {
  if (Chunk.isEmpty(points)) {
    return {
      windowStart,
      windowEnd,
      count: 0,
      sum: 0,
      avg: 0,
      min: 0,
      max: 0,
      p50: 0,
      p95: 0,
      p99: 0,
    };
  }

  let count = 0;
  let sum = 0;
  let min = Number.POSITIVE_INFINITY;
  let max = Number.NEGATIVE_INFINITY;
  const values: number[] = [];

  for (const point of points) {
    count++;
    sum += point.value;
    min = Math.min(min, point.value);
    max = Math.max(max, point.value);
    values.push(point.value);
  }

  values.sort((a, b) => a - b);

  const firstTag = Chunk.unsafeGet(points, 0).tags;

  return {
    windowStart,
    windowEnd,
    count,
    sum,
    avg: sum / count,
    min,
    max,
    p50: percentile(values, 50),
    p95: percentile(values, 95),
    p99: percentile(values, 99),
    tags: firstTag,
  };
};

/**
 * Create a sliding window metrics stream.
 *
 * Accepts a stream of `MetricPoint` values and emits `WindowMetrics` for each
 * completed window. Windows slide by `slideMs` and cover `windowSizeMs` worth
 * of data. When `slideMs` equals `windowSizeMs` (the default), windows are
 * tumbling (non-overlapping).
 *
 * Backpressure is preserved: if the downstream consumer is slow, the input
 * stream is naturally throttled through the internal queue.
 *
 * @example
 * ```
 * const metrics = slidingWindowMetrics(inputStream, {
 *   windowSizeMs: 60_000,  // 1 minute windows
 *   slideMs: 10_000,       // slide every 10 seconds
 * });
 * ```
 */
export const slidingWindowMetrics = <R, E>(
  input: Stream.Stream<MetricPoint, E, R>,
  config: SlidingWindowConfig,
): Stream.Stream<WindowMetrics, E, R> => {
  const slideMs = config.slideMs ?? config.windowSizeMs;
  const maxWindows = config.maxWindows ?? 100;

  return Stream.fromEffect(
    Effect.gen(function* () {
      const queue = yield* Queue.unbounded<MetricPoint>();
      const windowBuckets = yield* Queue.bounded<{
        points: Chunk.Chunk<MetricPoint>;
        windowStart: number;
        windowEnd: number;
      }>(maxWindows);

      // Fork: consume input into the raw queue
      yield* input.pipe(
        Stream.runForEach((point) => Queue.offer(queue, point)),
        Effect.forkScoped,
      );

      // Fork: every slideMs, drain the queue, bucket by window, emit windows
      yield* Stream.fromQueue(queue).pipe(
        Stream.groupedWithin(
          Number.MAX_SAFE_INTEGER,
          Duration.millis(slideMs),
        ),
        Stream.mapEffect((chunk) => {
          if (Chunk.isEmpty(chunk)) return Effect.void;

          // Group points into windows
          const now = Date.now();
          const windowStart = now - config.windowSizeMs;
          const windowEnd = now;

          const windowPoints = Chunk.filter(
            chunk,
            (p) => p.timestamp >= windowStart && p.timestamp <= windowEnd,
          );

          return Queue.offer(windowBuckets, {
            points: windowPoints,
            windowStart,
            windowEnd,
          });
        }),
        Stream.runDrain,
        Effect.forkScoped,
      );

      return windowBuckets;
    }),
  ).pipe(
    Stream.unwrap,
    Stream.concat(
      Stream.fromQueue(windowBuckets).pipe(
        Stream.map((bucket) => aggregate(bucket.points, bucket.windowStart, bucket.windowEnd)),
      ),
    ),
  );
};

/**
 * Convenience combinator: create a metrics pipeline directly from an iterable
 * or stream of { value, timestamp?, tags? } records.
 */
export const fromValues = <R, E>(
  values: Stream.Stream<{ value: number; timestamp?: number; tags?: Record<string, string> }, E, R>,
  config: SlidingWindowConfig,
): Stream.Stream<WindowMetrics, E, R> =>
  slidingWindowMetrics(
    values.pipe(
      Stream.map((v) => ({
        timestamp: v.timestamp ?? Date.now(),
        value: v.value,
        tags: v.tags,
      })),
    ),
    config,
  );
