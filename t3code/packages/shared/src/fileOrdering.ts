import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as Ref from "effect/Ref";
import * as HashMap from "effect/HashMap";
import * as Option from "effect/Option";

export const FileOrderEntry = Schema.Struct({
  filePath: Schema.String,
  order: Schema.Number,
  parentDir: Schema.String,
  updatedAt: Schema.Number,
});

export type FileOrderEntry = typeof FileOrderEntry.Type;

export const FileMoveResult = Schema.Struct({
  movedPath: Schema.String,
  newOrder: Schema.Number,
  oldOrder: Schema.Number,
  siblingPaths: Schema.Array(Schema.String),
});

export interface FileOrderStore {
  readonly move: (
    filePath: string,
    targetDir: string,
    position: number,
  ) => Effect.Effect<typeof FileMoveResult.Type, FileOrderError>;
  readonly reorder: (filePath: string, newOrder: number) => Effect.Effect<void>;
  readonly getOrder: (dirPath: string) => Effect.Effect<readonly typeof FileOrderEntry.Type[]>;
  readonly remove: (filePath: string) => Effect.Effect<void>;
  readonly snapshot: Effect.Effect<ReadonlyMap<string, number>>;
}

export class FileOrderError extends Schema.TaggedError<FileOrderError>()("FileOrderError", {
  path: Schema.String,
  message: Schema.String,
}) {}

export const makeFileOrderStore = (): Effect.Effect<FileOrderStore> =>
  Effect.gen(function* () {
    const entries = yield* Ref.make(HashMap.empty<string, FileOrderEntry>());

    const getOrder = (dirPath: string): Effect.Effect<readonly FileOrderEntry[]> =>
      Ref.get(entries).pipe(
        Effect.map((m) =>
          [...HashMap.values(m)]
            .filter((e) => e.parentDir === dirPath)
            .sort((a, b) => a.order - b.order),
        ),
      );

    const reorder = (filePath: string, newOrder: number): Effect.Effect<void> =>
      Ref.update(entries, (m) => {
        const existing = HashMap.get(m, filePath);
        if (Option.isNone(existing)) return m;
        return HashMap.set(m, filePath, { ...existing.value, order: newOrder, updatedAt: Date.now() });
      });

    const move = (
      filePath: string,
      targetDir: string,
      position: number,
    ): Effect.Effect<typeof FileMoveResult.Type, FileOrderError> =>
      Effect.gen(function* () {
        const current = yield* Ref.get(entries);
        const existing = HashMap.get(current, filePath);

        const oldOrder = Option.match(existing, {
          onNone: () => -1,
          onSome: (e) => e.order,
        });

        const siblings = [...HashMap.values(current)]
          .filter((e) => e.parentDir === targetDir && e.filePath !== filePath)
          .sort((a, b) => a.order - b.order);

        const newOrder = position >= 0 && position <= siblings.length ? position : siblings.length;

        // Shift siblings
        for (const sibling of siblings) {
          if (sibling.order >= newOrder) {
            yield* Ref.update(entries, HashMap.set(sibling.filePath, {
              ...sibling, order: sibling.order + 1, updatedAt: Date.now(),
            }));
          }
        }

        // Insert at position
        yield* Ref.update(entries, HashMap.set(filePath, {
          filePath,
          order: newOrder,
          parentDir: targetDir,
          updatedAt: Date.now(),
        }));

        return {
          movedPath: filePath,
          newOrder,
          oldOrder,
          siblingPaths: siblings.map((s) => s.filePath),
        };
      });

    const remove = (filePath: string): Effect.Effect<void> =>
      Ref.update(entries, HashMap.remove(filePath));

    const snapshot = Ref.get(entries).pipe(
      Effect.map((m) => {
        const map = new Map<string, number>();
        for (const [k, v] of HashMap.entries(m)) map.set(k, v.order);
        return map;
      }),
    );

    return { move, reorder, getOrder, remove, snapshot } satisfies FileOrderStore;
  });
