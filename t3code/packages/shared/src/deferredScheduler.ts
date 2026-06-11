import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as Schedule from "effect/Schedule";
import * as Ref from "effect/Ref";
import * as HashMap from "effect/HashMap";
import * as Queue from "effect/Queue";
import * as Option from "effect/Option";
import * as Duration from "effect/Duration";

export const ScheduledCommandId = Schema.String.pipe(Schema.brand("ScheduledCommandId"));
export type ScheduledCommandId = typeof ScheduledCommandId.Type;

export const ScheduledCommand = Schema.Struct({
  id: ScheduledCommandId,
  name: Schema.String,
  payload: Schema.Unknown,
  scheduledAt: Schema.Number,
  executeAfter: Schema.Number,
  retryCount: Schema.Number.pipe(Schema.withDecodingDefault(Effect.succeed(0))),
  maxRetries: Schema.Number.pipe(Schema.withDecodingDefault(Effect.succeed(3))),
  status: Schema.Literal("pending", "running", "completed", "failed", "cancelled").pipe(
    Schema.withDecodingDefault(Effect.succeed("pending" as const)),
  ),
});

export type ScheduledCommand = typeof ScheduledCommand.Type;

export class DeferredSchedulerError extends Schema.TaggedError<DeferredSchedulerError>()(
  "DeferredSchedulerError",
  { commandId: ScheduledCommandId, message: Schema.String },
) {}

export interface DeferredScheduler {
  readonly schedule: (
    name: string,
    payload: unknown,
    executeAfter: Duration.DurationInput,
    maxRetries?: number,
  ) => Effect.Effect<ScheduledCommandId>;
  readonly cancel: (id: ScheduledCommandId) => Effect.Effect<void>;
  readonly status: (id: ScheduledCommandId) => Effect.Effect<Option.Option<ScheduledCommand>>;
  readonly pending: Effect.Effect<readonly ScheduledCommand[]>;
  readonly stats: Effect.Effect<{ pending: number; completed: number; failed: number }>;
}

let counter = 0;
const generateId = (): ScheduledCommandId =>
  ScheduledCommandId.make(`cmd-${++counter}-${Date.now().toString(36)}`);

export const makeDeferredScheduler = (
  executor: (cmd: ScheduledCommand) => Effect.Effect<void, DeferredSchedulerError>,
  pollInterval: Duration.DurationInput = Duration.seconds(5),
): Effect.Effect<DeferredScheduler> =>
  Effect.gen(function* () {
    const commands = yield* Ref.make(HashMap.empty<ScheduledCommandId, ScheduledCommand>());
    const completed = yield* Ref.make(0);
    const failed = yield* Ref.make(0);

    // Background poll loop
    yield* Effect.repeat(
      Effect.gen(function* () {
        const now = Date.now();
        const current = yield* Ref.get(commands);

        for (const [, cmd] of HashMap.entries(current)) {
          if (cmd.status !== "pending") continue;
          if (now < cmd.executeAfter) continue;

          yield* Ref.update(commands, HashMap.set(cmd.id, { ...cmd, status: "running" }));

          const result = yield* executor(cmd).pipe(
            Effect.match({
              onFailure: (error) =>
                Effect.gen(function* () {
                  if (cmd.retryCount < cmd.maxRetries) {
                    const retryDelay = Math.min(60_000, 5_000 * Math.pow(2, cmd.retryCount));
                    yield* Ref.update(commands, HashMap.set(cmd.id, {
                      ...cmd,
                      status: "pending",
                      retryCount: cmd.retryCount + 1,
                      executeAfter: now + retryDelay,
                    }));
                  } else {
                    yield* Ref.update(commands, HashMap.set(cmd.id, { ...cmd, status: "failed" }));
                    yield* Ref.update(failed, (n) => n + 1);
                  }
                }),
              onSuccess: () =>
                Effect.gen(function* () {
                  yield* Ref.update(commands, HashMap.set(cmd.id, { ...cmd, status: "completed" }));
                  yield* Ref.update(completed, (n) => n + 1);
                }),
            }),
          );

          yield* result;
        }
      }),
      { schedule: () => Duration.decode(pollInterval) },
    ).pipe(Effect.forkScoped);

    const schedule = (
      name: string,
      payload: unknown,
      executeAfter: Duration.DurationInput,
      maxRetries = 3,
    ): Effect.Effect<ScheduledCommandId> =>
      Effect.gen(function* () {
        const now = Date.now();
        const afterMs = Duration.toMillis(Duration.decode(executeAfter));
        const cmd: ScheduledCommand = {
          id: generateId(),
          name,
          payload,
          scheduledAt: now,
          executeAfter: now + afterMs,
          retryCount: 0,
          maxRetries,
          status: "pending",
        };
        yield* Ref.update(commands, HashMap.set(cmd.id, cmd));
        return cmd.id;
      });

    const cancel = (id: ScheduledCommandId): Effect.Effect<void> =>
      Ref.update(commands, (m) => {
        const existing = HashMap.get(m, id);
        if (Option.isNone(existing)) return m;
        return HashMap.set(m, id, { ...existing.value, status: "cancelled" });
      });

    const status = (id: ScheduledCommandId): Effect.Effect<Option.Option<ScheduledCommand>> =>
      Ref.get(commands).pipe(Effect.map((m) => HashMap.get(m, id)));

    const pending = Ref.get(commands).pipe(
      Effect.map((m) => [...HashMap.values(m)].filter((c) => c.status === "pending")),
    );

    const stats = Effect.gen(function* () {
      const c = yield* Ref.get(completed);
      const f = yield* Ref.get(failed);
      const current = yield* Ref.get(commands);
      const p = [...HashMap.values(current)].filter((cmd) => cmd.status === "pending").length;
      return { pending: p, completed: c, failed: f };
    });

    return { schedule, cancel, status, pending, stats } satisfies DeferredScheduler;
  });
