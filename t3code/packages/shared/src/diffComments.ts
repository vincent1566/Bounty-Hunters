import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as Ref from "effect/Ref";
import * as HashMap from "effect/HashMap";
import * as Option from "effect/Option";

export const DiffCommentId = Schema.String.pipe(Schema.brand("DiffCommentId"));
export type DiffCommentId = typeof DiffCommentId.Type;

export const DiffPosition = Schema.Struct({
  filePath: Schema.String,
  lineNumber: Schema.Number.pipe(Schema.int()),
  side: Schema.Literal("left", "right"),
});

export const DiffComment = Schema.Struct({
  id: DiffCommentId,
  position: DiffPosition,
  body: Schema.String,
  author: Schema.String,
  createdAt: Schema.Number,
  resolved: Schema.Boolean.pipe(Schema.withDecodingDefault(Effect.succeed(false))),
  replyTo: Schema.optional(DiffCommentId),
});

export type DiffComment = typeof DiffComment.Type;

export interface DiffCommentStore {
  readonly add: (comment: DiffComment) => Effect.Effect<void>;
  readonly remove: (id: DiffCommentId) => Effect.Effect<void>;
  readonly getByFile: (filePath: string) => Effect.Effect<readonly DiffComment[]>;
  readonly getByLine: (filePath: string, line: number) => Effect.Effect<readonly DiffComment[]>;
  readonly resolve: (id: DiffCommentId) => Effect.Effect<void>;
  readonly all: Effect.Effect<readonly DiffComment[]>;
}

let nextId = 0;
const generateId = (): DiffCommentId => DiffCommentId.make(`comment-${++nextId}-${Date.now()}`);

export const makeDiffCommentStore = (): Effect.Effect<DiffCommentStore> =>
  Effect.gen(function* () {
    const comments = yield* Ref.make(HashMap.empty<DiffCommentId, DiffComment>());
    const fileIndex = yield* Ref.make(HashMap.empty<string, DiffCommentId[]>());

    const add = (comment: DiffComment): Effect.Effect<void> =>
      Ref.update(comments, HashMap.set(comment.id, comment)).pipe(
        Effect.zipRight(
          Ref.update(fileIndex, (idx) => {
            const existing = HashMap.get(idx, comment.position.filePath).pipe(Option.getOrElse(() => []));
            return HashMap.set(idx, comment.position.filePath, [...existing, comment.id]);
          }),
        ),
      );

    const remove = (id: DiffCommentId): Effect.Effect<void> =>
      Ref.update(comments, HashMap.remove(id));

    const getByFile = (filePath: string): Effect.Effect<readonly DiffComment[]> =>
      Effect.gen(function* () {
        const all = yield* Ref.get(comments);
        const fileIds = yield* Ref.get(fileIndex).pipe(
          Effect.map((idx) => HashMap.get(idx, filePath).pipe(Option.getOrElse(() => [] as DiffCommentId[]))),
        );
        return fileIds
          .map((id) => HashMap.get(all, id))
          .filter(Option.isSome)
          .map((opt) => opt.value)
          .filter((c) => !c.resolved);
      });

    const getByLine = (filePath: string, line: number): Effect.Effect<readonly DiffComment[]> =>
      getByFile(filePath).pipe(
        Effect.map((cs) => cs.filter((c) => c.position.lineNumber === line)),
      );

    const resolve = (id: DiffCommentId): Effect.Effect<void> =>
      Ref.update(comments, (m) => {
        const existing = HashMap.get(m, id);
        if (Option.isNone(existing)) return m;
        return HashMap.set(m, id, { ...existing.value, resolved: true });
      });

    const all = Ref.get(comments).pipe(
      Effect.map((m) => [...HashMap.values(m)].filter((c) => !c.resolved)),
    );

    return { add, remove, getByFile, getByLine, resolve, all } satisfies DiffCommentStore;
  });
