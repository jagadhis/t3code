import type { OrchestrationV2ProviderFailure, OrchestrationV2Run, RunId } from "@t3tools/contracts";
import {
  providerFailureVerdict,
  providerRetryWaitMs,
} from "@t3tools/shared/providerFailureVerdict";
import * as DateTime from "effect/DateTime";

/**
 * A delegated child whose turn a passing provider failure cut off is continued in place, with the same
 * `delegatedTaskId`, after 1, 2, 4 and 8 minutes. While the continuation is due, the task and the parent's wake are
 * held as they are for a restart continuation, so the parent hears once: the result of the turn that finally ends.
 */
export const providerRetryEffectId = (runId: RunId) => `effect:provider-retry:${runId}`;
export const providerRetryCommandId = (runId: RunId) => `command:provider-retry:${runId}`;
export const providerRetryReleaseEffectId = (runId: RunId) =>
  `effect:provider-retry-release:${runId}`;
export const providerRetryMessageId = (runId: RunId) => `message:provider-retry:${runId}`;

export interface ProviderRetryPlan {
  /** This run's place in the chain of cut-off turns in a row, from 1. */
  readonly cuts: number;
  readonly reason: string;
  readonly availableAt: DateTime.Utc;
}

/**
 * When to continue a failed run of a delegated task, or null when its failure is final: lasting or unrecognised, the
 * fifth cut in a row, or a run Stop already reached.
 */
export function providerRetryPlan(input: {
  readonly run: OrchestrationV2Run;
  readonly runs: ReadonlyArray<OrchestrationV2Run>;
  readonly failure: Pick<OrchestrationV2ProviderFailure, "class" | "code" | "message"> | null;
  readonly stopped: boolean;
  readonly now: DateTime.Utc;
}): ProviderRetryPlan | null {
  const { run, runs, failure } = input;
  if (run.status !== "failed" || failure === null || input.stopped) return null;
  const verdict = providerFailureVerdict(failure);
  let cuts = 1;
  const seen = new Set<RunId>([run.id]);
  for (
    let previousId = run.providerRetryOfRunId;
    previousId !== undefined && !seen.has(previousId);
    previousId = runs.find((candidate) => candidate.id === previousId)?.providerRetryOfRunId
  ) {
    seen.add(previousId);
    cuts += 1;
  }
  const waitMs = providerRetryWaitMs(verdict, cuts);
  if (waitMs === null) return null;
  return {
    cuts,
    reason: verdict.reason,
    availableAt: DateTime.add(run.completedAt ?? input.now, { milliseconds: waitMs }),
  };
}
