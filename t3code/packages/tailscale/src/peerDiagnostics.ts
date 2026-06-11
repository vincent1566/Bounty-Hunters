import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as Queue from "effect/Queue";
import * as Ref from "effect/Ref";
import * as Duration from "effect/Duration";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";

export const PEER_PING_TIMEOUT_MS = 3_000;
export const PEER_PING_COUNT = 4;
export const LATENCY_HISTORY_MAX = 500;

export class PeerPingError extends Schema.TaggedError<PeerPingError>()("PeerPingError", {
  peer: Schema.String,
  message: Schema.String,
  exitCode: Schema.Number.pipe(Schema.optional),
}) {}

const PeerPingResult = Schema.Struct({
  peer: Schema.String,
  latencyMs: Schema.Number,
  lossRate: Schema.Number,
  timestamp: Schema.Number,
});

const PeerDiagnosticEntry = Schema.Struct({
  peer: Schema.String,
  minLatency: Schema.Number,
  maxLatency: Schema.Number,
  avgLatency: Schema.Number,
  lossRate: Schema.Number,
  lastSeen: Schema.Number,
  samples: Schema.Number,
});

export const pingPeer = (
  peer: string,
): Effect.Effect<number, PeerPingError, ChildProcessSpawner.ChildProcessSpawner> =>
  Effect.gen(function* () {
    const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
    const child = yield* spawner.spawn(
      ChildProcess.make("tailscale", ["ping", "--c", "1", "--timeout", "3s", peer]),
    );

    const stdout = yield* child.stdout.pipe(
      Stream.decodeText(),
      Stream.runFold("", (acc, c) => acc + c),
    );

    const exitCode = yield* child.exitCode.pipe(Effect.map(Number));

    if (exitCode !== 0) {
      return yield* new PeerPingError({ peer, message: `ping failed: code ${exitCode}`, exitCode });
    }

    const match = stdout.match(/time[=<]\s*(\d+\.?\d*)\s*ms/i);
    return match ? Number.parseFloat(match[1]) : 0;
  }).pipe(Effect.scoped, Effect.timeoutOption(PEER_PING_TIMEOUT_MS), Effect.flatMap((opt) => {
    if (opt._tag === "None") return Effect.fail(new PeerPingError({ peer, message: "ping timeout" }));
    return Effect.succeed(opt.value);
  }));

export const pingPeerWithStats = (
  peer: string,
): Effect.Effect<
  { min: number; max: number; avg: number; lossRate: number; samples: number[] },
  PeerPingError,
  ChildProcessSpawner.ChildProcessSpawner
> =>
  Effect.gen(function* () {
    const latencies: number[] = [];
    let lost = 0;

    for (let i = 0; i < PEER_PING_COUNT; i++) {
      const result = yield* pingPeer(peer).pipe(Effect.either);
      if (result._tag === "Right") {
        latencies.push(result.right);
      } else {
        lost++;
      }
    }

    if (latencies.length === 0) {
      return { min: 0, max: 0, avg: 0, lossRate: 1, samples: [] };
    }

    latencies.sort((a, b) => a - b);
    return {
      min: latencies[0],
      max: latencies[latencies.length - 1],
      avg: latencies.reduce((s, v) => s + v, 0) / latencies.length,
      lossRate: lost / PEER_PING_COUNT,
      samples: latencies,
    };
  });

export const listPeers = (): Effect.Effect<
  readonly string[],
  never,
  ChildProcessSpawner.ChildProcessSpawner
> =>
  Effect.gen(function* () {
    const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
    const child = yield* spawner.spawn(
      ChildProcess.make("tailscale", ["status", "--json"]),
    );

    const stdout = yield* child.stdout.pipe(
      Stream.decodeText(),
      Stream.runFold("", (acc, c) => acc + c),
    );

    const exitCode = yield* child.exitCode.pipe(Effect.map(Number));
    if (exitCode !== 0) return [] as const;

    try {
      const parsed = JSON.parse(stdout);
      const peers: string[] = [];
      if (parsed.Peer) {
        for (const [hostname, peer] of Object.entries(parsed.Peer)) {
          const p = peer as any;
          if (p.Online && p.TailscaleIPs?.length > 0) {
            peers.push(hostname);
          }
        }
      }
      return peers;
    } catch {
      return [] as const;
    }
  }).pipe(Effect.scoped);

export const collectPeerDiagnostics = (
  peers: readonly string[],
): Effect.Effect<
  ReadonlyArray<typeof PeerDiagnosticEntry.Type>,
  never,
  ChildProcessSpawner.ChildProcessSpawner
> =>
  Effect.gen(function* () {
    const results = yield* Effect.all(
      peers.map((peer) =>
        pingPeerWithStats(peer).pipe(
          Effect.match({
            onFailure: () => ({
              peer,
              minLatency: 0, maxLatency: 0, avgLatency: 0,
              lossRate: 1, lastSeen: Date.now(), samples: 0,
            }),
            onSuccess: (stats) => ({
              peer,
              minLatency: stats.min,
              maxLatency: stats.max,
              avgLatency: stats.avg,
              lossRate: stats.lossRate,
              lastSeen: Date.now(),
              samples: stats.samples.length,
            }),
          }),
        ),
      ),
      { concurrency: 4 },
    );
    return results;
  });
