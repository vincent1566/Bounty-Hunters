import * as Effect from "effect/Effect";
import * as Ref from "effect/Ref";
import * as HashMap from "effect/HashMap";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";

export const KeyVersion = Schema.Number.pipe(Schema.int(), Schema.brand("KeyVersion"));
export type KeyVersion = typeof KeyVersion.Type;

const EncryptedBlob = Schema.Struct({
  keyVersion: KeyVersion,
  ciphertext: Schema.String,
  iv: Schema.String,
  algorithm: Schema.String.pipe(Schema.withDecodingDefault(Effect.succeed("aes-256-gcm"))),
});

export class KeyRotationError extends Schema.TaggedError<KeyRotationError>()("KeyRotationError", {
  message: Schema.String,
}) {}

export interface KeyStore {
  readonly encrypt: (plaintext: string) => Effect.Effect<typeof EncryptedBlob.Type, KeyRotationError>;
  readonly decrypt: (blob: typeof EncryptedBlob.Type) => Effect.Effect<string, KeyRotationError>;
  readonly rotate: () => Effect.Effect<KeyVersion, KeyRotationError>;
  readonly currentVersion: Effect.Effect<KeyVersion>;
  readonly versions: Effect.Effect<readonly KeyVersion[]>;
}

export const makeKeyStore = (
  initialKey: CryptoKey,
): Effect.Effect<KeyStore, never> =>
  Effect.gen(function* () {
    const keys = yield* Ref.make(HashMap.empty<KeyVersion, CryptoKey>());
    const current = yield* Ref.make(KeyVersion.make(1));
    yield* Ref.update(keys, HashMap.set(KeyVersion.make(1), initialKey));

    const getKey = (version: KeyVersion) =>
      Ref.get(keys).pipe(
        Effect.map((m) => HashMap.get(m, version)),
        Effect.flatMap((opt) =>
          Option.match(opt, {
            onNone: () => Effect.fail(new KeyRotationError({ message: `Key version ${version} not found` })),
            onSome: Effect.succeed,
          }),
        ),
      );

    const encrypt = (plaintext: string): Effect.Effect<typeof EncryptedBlob.Type, KeyRotationError> =>
      Effect.gen(function* () {
        const v = yield* Ref.get(current);
        const key = yield* getKey(v);
        const encoder = new TextEncoder();
        const iv = crypto.getRandomValues(new Uint8Array(12));

        try {
          const cipherBuffer = yield* Effect.tryPromise(() =>
            crypto.subtle.encrypt(
              { name: "AES-GCM", iv },
              key,
              encoder.encode(plaintext),
            ),
          );

          return {
            keyVersion: v,
            ciphertext: btoa(String.fromCharCode(...new Uint8Array(cipherBuffer))),
            iv: btoa(String.fromCharCode(...iv)),
            algorithm: "aes-256-gcm",
          };
        } catch (err) {
          return yield* new KeyRotationError({ message: `Encryption failed: ${String(err)}` });
        }
      });

    const decrypt = (blob: typeof EncryptedBlob.Type): Effect.Effect<string, KeyRotationError> =>
      Effect.gen(function* () {
        const key = yield* getKey(blob.keyVersion);

        try {
          const ciphertext = Uint8Array.from(atob(blob.ciphertext), (c) => c.charCodeAt(0));
          const iv = Uint8Array.from(atob(blob.iv), (c) => c.charCodeAt(0));

          const plainBuffer = yield* Effect.tryPromise(() =>
            crypto.subtle.decrypt({ name: "AES-GCM", iv }, key, ciphertext),
          );

          return new TextDecoder().decode(plainBuffer);
        } catch (err) {
          return yield* new KeyRotationError({ message: `Decryption failed: ${String(err)}` });
        }
      });

    const rotate = (): Effect.Effect<KeyVersion, KeyRotationError> =>
      Effect.gen(function* () {
        try {
          const newKey = yield* Effect.tryPromise(() =>
            crypto.subtle.generateKey(
              { name: "AES-GCM", length: 256 },
              true,
              ["encrypt", "decrypt"],
            ),
          );

          const v = yield* Ref.get(current);
          const newVersion = KeyVersion.make(v + 1);

          yield* Ref.update(keys, HashMap.set(newVersion, newKey));
          yield* Ref.set(current, newVersion);

          return newVersion;
        } catch (err) {
          return yield* new KeyRotationError({ message: `Key generation failed: ${String(err)}` });
        }
      });

    const currentVersion = Ref.get(current);
    const versions = Ref.get(keys).pipe(Effect.map((m) => [...HashMap.keys(m)]));

    return { encrypt, decrypt, rotate, currentVersion, versions } satisfies KeyStore;
  });
