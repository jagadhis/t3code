import {
  CommandId,
  MessageId,
  type OrchestrationV2AppThread,
  type OrchestrationV2Run,
  type RunId,
  type ThreadId,
} from "@t3tools/contracts";
import { latestRootProviderFailure, runRanAfter } from "@t3tools/shared/orchestrationV2ThreadError";
import {
  PROVIDER_RETRY_WAITS_MS,
  providerRetryContinuePrompt,
} from "@t3tools/shared/providerFailureVerdict";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import type * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import type * as Path from "effect/Path";
import * as Schema from "effect/Schema";

import type * as ProcessRunner from "../processRunner.ts";
import type * as ProjectStore from "./ProjectStore.ts";
import {
  providerRetryCommandId,
  providerRetryMessageId,
  providerRetryPlan,
} from "./DelegatedProviderRetry.ts";
import { cancelledBeforeStart } from "./RestartBackgroundNote.ts";
import * as ThreadManagementService from "./ThreadManagementService.ts";

export class ResumeGuardError extends Schema.TaggedError<ResumeGuardError>()("ResumeGuardError", {
  detail: Schema.String,
  cause: Schema.optional(Schema.Defect()),
}) {}

/**
 * The outward steps (comments, reviews, PRs, Linear or Slack writes, thread launches) a cut-off run completed before
 * the cut, in words; null when nothing can list them for this thread. A failure means they could not be recorded,
 * and the run is then not continued.
 */
export type ListCompletedOutwardSteps = (input: {
  readonly thread: OrchestrationV2AppThread;
  readonly source: OrchestrationV2Run;
}) => Effect.Effect<ReadonlyArray<string> | null, ResumeGuardError>;

const RESUME_GUARD_SCRIPT = ".agents/scripts/resume-guard.ts";
const decodeArmOutput = Schema.decodeUnknownEffect(
  Schema.fromJsonString(Schema.Struct({ done: Schema.Array(Schema.String) })),
);

/**
 * A workspace may carry `.agents/scripts/resume-guard.ts`, as Cockpit's station runs use it: `arm` reads the thread's
 * completed tool calls from T3's state, keeps the outward ones and records them, so the workspace's tool hook denies an
 * exact repeat in that thread, and prints them for the continue message. It is looked up in the main checkout, where
 * the hook reads it for every worktree.
 */
export const workspaceResumeGuard =
  (services: {
    readonly processRunner: ProcessRunner.ProcessRunner["Service"];
    readonly fileSystem: FileSystem.FileSystem;
    readonly path: Path.Path;
    readonly projects: ProjectStore.ProjectStoreV2["Service"];
    /** This server's state database, which the script reads; it defaults to the live install's. */
    readonly stateDbPath?: string | undefined;
  }): ListCompletedOutwardSteps =>
  ({ thread, source }) =>
    Effect.gen(function* () {
      const cwd =
        thread.worktreePath ??
        Option.getOrUndefined(
          yield* services.projects
            .get(thread.projectId)
            .pipe(Effect.orElseSucceed(() => Option.none())),
        )?.workspaceRoot;
      if (cwd === undefined) return null;
      const commonDir = yield* services.processRunner
        .run({
          command: "git",
          args: ["rev-parse", "--path-format=absolute", "--git-common-dir"],
          cwd,
          timeout: "10 seconds",
        })
        .pipe(Effect.option);
      if (Option.isNone(commonDir) || commonDir.value.code !== 0) return null;
      const root = services.path.dirname(commonDir.value.stdout.trim());
      const script = services.path.join(root, RESUME_GUARD_SCRIPT);
      if (!(yield* services.fileSystem.exists(script).pipe(Effect.orElseSucceed(() => false)))) {
        return null;
      }
      const armed = yield* services.processRunner
        .run({
          command: "bun",
          args: [script, "arm", thread.id, String(source.ordinal)],
          cwd: root,
          ...(services.stateDbPath === undefined
            ? {}
            : { env: { T3_STATE_PATH: services.stateDbPath } }),
          timeout: "30 seconds",
        })
        .pipe(
          Effect.mapError(
            (cause) => new ResumeGuardError({ detail: "resume-guard arm did not run", cause }),
          ),
        );
      if (armed.code !== 0) {
        return yield* new ResumeGuardError({
          detail: `resume-guard arm exited ${armed.code}: ${armed.stderr.trim()}`,
        });
      }
      const { done } = yield* decodeArmOutput(armed.stdout).pipe(
        Effect.mapError(
          (cause) =>
            new ResumeGuardError({ detail: "resume-guard arm printed no list of steps", cause }),
        ),
      );
      return done;
    });

/**
 * Continues a delegated child's run that a passing provider failure cut off, once its wait is over. Whatever it
 * decides, the task is then finalized from the source run unless a continuation run took over.
 */
export const continueAfterProviderFailure = Effect.fn(
  "ProviderRetryContinuation.continueAfterProviderFailure",
)(
  function* (input: {
    readonly threadId: ThreadId;
    readonly sourceRunId: RunId;
    readonly listCompletedOutwardSteps?: ListCompletedOutwardSteps;
  }) {
    const threads = yield* ThreadManagementService.ThreadManagementService;
    const messageId = MessageId.make(providerRetryMessageId(input.sourceRunId));
    const projection = yield* threads.getThreadRecords(
      input.threadId,
      ["messages", "runs", "turnItems"],
      {
        messageIds: [messageId],
        turnItemRunIds: [input.sourceRunId],
        turnItemTypes: ["error", "run_interrupt_request"],
      },
    );
    if (projection.messages.some((message) => message.id === messageId)) return;
    const source = projection.runs.find((run) => run.id === input.sourceRunId);
    if (
      source === undefined ||
      projection.thread.archivedAt !== null ||
      projection.thread.deletedAt !== null ||
      projection.runs.some(
        (run) =>
          run.id !== source.id &&
          run.status !== "queued" &&
          !cancelledBeforeStart(run) &&
          runRanAfter(run, source),
      )
    )
      return;
    const plan = providerRetryPlan({
      run: source,
      runs: projection.runs,
      failure: latestRootProviderFailure(source, projection.turnItems),
      stopped: projection.turnItems.some((item) => item.type === "run_interrupt_request"),
      now: yield* DateTime.now,
    });
    if (plan === null) return;
    const done =
      input.listCompletedOutwardSteps === undefined
        ? null
        : yield* input.listCompletedOutwardSteps({ thread: projection.thread, source }).pipe(
            Effect.map((steps) => ({ steps })),
            Effect.catch((error) =>
              Effect.logWarning("orchestration-v2.provider-retry.steps-unrecorded", {
                threadId: input.threadId,
                sourceRunId: input.sourceRunId,
                error,
              }).pipe(Effect.as(undefined)),
            ),
          );
    // Without the record of what it already did, the continued turn could
    // repeat a comment or a PR, so it fails as it is instead.
    if (done === undefined) return;
    const text = providerRetryContinuePrompt(plan.reason, done?.steps ?? null);
    yield* threads
      .dispatch({
        type: "message.dispatch",
        commandId: CommandId.make(providerRetryCommandId(input.sourceRunId)),
        threadId: input.threadId,
        messageId,
        text,
        notification: {
          source: { kind: "system" },
          outcome: "updated",
          summary: `${plan.reason}; T3 Code continued this turn (try ${plan.cuts + 1} of ${PROVIDER_RETRY_WAITS_MS.length + 1})`,
          detail: text,
        },
        attachments: [],
        modelSelection: source.modelSelection,
        dispatchMode: { type: "start_immediately" },
        createdBy: "agent",
        creationSource: "server",
        providerRetryOfRunId: input.sourceRunId,
      })
      .pipe(
        Effect.catchTags({
          ThreadManagementProviderInstanceDisabledError: (error) =>
            Effect.logInfo("orchestration-v2.provider-retry.instance-disabled", {
              threadId: input.threadId,
              sourceRunId: input.sourceRunId,
              instanceId: error.instanceId,
            }),
        }),
      );
  },
  (effect, input) =>
    effect.pipe(
      Effect.andThen(
        Effect.gen(function* () {
          const threads = yield* ThreadManagementService.ThreadManagementService;
          yield* threads.recoverDelegatedTask(input.threadId, input.sourceRunId);
        }),
      ),
    ),
);
