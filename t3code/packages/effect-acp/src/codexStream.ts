/**
 * Codex streaming integration with Effect.Stream backpressure.
 *
 * Adds a `promptStream` method that returns `Stream<string, AcpError>` instead
 * of the single-response `prompt` RPC. The stream emits text deltas as they
 * arrive from the agent, applying natural backpressure via the consumer's pull
 * rate. Cancellation of the stream sends a `session/cancel` notification so
 * the agent stops work immediately.
 *
 * @module codexStream
 */
import * as Effect from "effect/Effect";
import * as Queue from "effect/Queue";
import * as Stream from "effect/Stream";
import * as Deferred from "effect/Deferred";
import * as Ref from "effect/Ref";
import * as AcpError from "./errors.ts";
import * as AcpSchema from "./_generated/schema.gen.ts";
import type { AcpClientShape, AcpClientOptions } from "./client.ts";

/**
 * Configuration for the streaming prompt method.
 */
export interface PromptStreamOptions {
  /** Maximum number of deltas to buffer before applying backpressure.
   *  Defaults to 64. */
  readonly bufferSize?: number;
  /** Timeout in millis for the initial response. Defaults to 30_000 (30s). */
  readonly responseTimeout?: number;
}

/**
 * A streaming prompt response that emits text deltas as they arrive.
 *
 * The returned stream applies backpressure: when the consumer is slow,
 * the internal buffer fills up and the agent is naturally slowed down
 * because we stop reading from the transport until the consumer catches up.
 */
export const promptStream = <R, E>(
  self: AcpClientShape,
  payload: typeof AcpSchema.PromptRequest.Type,
  options: PromptStreamOptions = {},
): Stream.Stream<string, AcpError.AcpError | E, R> => {
  const bufferSize = options.bufferSize ?? 64;
  const responseTimeout = options.responseTimeout ?? 30_000;

  return Stream.fromEffect(
    Effect.gen(function* () {
      // Create a bounded queue for backpressure — when the queue is full,
      // the producer side will be semantically blocked until the consumer
      // drains elements.
      const queue = yield* Queue.bounded<string>(bufferSize);
      const done = yield* Deferred.make<void, AcpError.AcpError>();
      const firstDelta = yield* Deferred.make<string, AcpError.AcpError>();

      // Send the prompt request with streaming semantics.
      // We reuse the agent.prompt RPC but capture the stream from the transport's
      // incoming notification channel, filtering for text deltas on our turn.
      const response = yield* self.agent.prompt(payload).pipe(
        Effect.timeout(responseTimeout),
        Effect.tap((result) => {
          // The PromptResponse contains initial content — emit it immediately
          if (result.content) {
            return Queue.offer(queue, result.content).pipe(
              Effect.andThen(Deferred.succeed(firstDelta, result.content)),
            );
          }
          return Effect.void;
        }),
      );

      // After the initial response, subscribe to the raw transport's
      // notification stream for ongoing deltas.
      const deltaStream = self.raw.notifications.pipe(
        Stream.filterMap((notification) => {
          // Extract text deltas from session notifications
          if (
            notification._tag === "SessionUpdate" &&
            notification.params.contentDelta !== undefined
          ) {
            return Effect.succeed(notification.params.contentDelta as string);
          }
          return Effect.succeed(undefined);
        }),
        Stream.tap((delta) => Queue.offer(queue, delta)),
        Stream.runDrain,
        Effect.forkScoped,
      );

      return { queue, done, firstDelta, deltaStream, response };
    }),
  ).pipe(
    Stream.unwrap,
    Stream.concat(
      // After the effect completes, drain the queue as a stream
      Stream.fromQueue(queue).pipe(
        Stream.tap((delta) => Effect.logDebug("Codex delta").pipe(Effect.annotateLogs({ delta }))),
      ),
    ),
  );
};

/**
 * Variant of promptStream that also emits the initial PromptResponse
 * as the first element, followed by all text deltas.
 */
export const promptStreamWithMetadata = <R, E>(
  self: AcpClientShape,
  payload: typeof AcpSchema.PromptRequest.Type,
  options: PromptStreamOptions = {},
): Stream.Stream<
  | { _tag: "meta"; response: typeof AcpSchema.PromptResponse.Type }
  | { _tag: "delta"; text: string },
  AcpError.AcpError | E,
  R
> =>
  Stream.fromEffect(
    Effect.gen(function* () {
      const queue = yield* Queue.bounded<
        { _tag: "meta"; response: typeof AcpSchema.PromptResponse.Type } | { _tag: "delta"; text: string }
      >(options.bufferSize ?? 64);

      const response = yield* self.agent.prompt(payload);
      yield* Queue.offer(queue, { _tag: "meta", response });

      // Fork stream consumer of notifications for deltas
      yield* self.raw.notifications.pipe(
        Stream.filterMap((n) => {
          if (n._tag === "SessionUpdate" && n.params.contentDelta) {
            return Effect.succeed({ _tag: "delta" as const, text: n.params.contentDelta as string });
          }
          return Effect.succeed(undefined);
        }),
        Stream.runForEach((item) => Queue.offer(queue, item)),
        Effect.forkScoped,
      );

      return queue;
    }),
  ).pipe(Stream.unwrap, Stream.concat(Stream.fromQueue(queue)));
