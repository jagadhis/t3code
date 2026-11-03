import type { OrchestrationV2ProviderFailure } from "@t3tools/contracts";

/** Whether a provider failure passes by itself, so waiting and continuing the turn can fix it, and why, in a few words. */
export interface ProviderFailureVerdict {
  readonly passing: boolean;
  readonly reason: string;
}

// Kept rule for rule with Cockpit's `providerVerdict` (jagadhis/cockpit src/server/runner/schedule.ts), which resumes
// station runs the same way; change both together. After shepherdr's server/live/retry.ts (MIT): the CLI's code decides
// where it has one, its words decide for the rest, and anything not recognised does not pass, so it surfaces at once.
const SIGNED_OUT_CODES = new Set([
  "unauthorized",
  "authentication_failed",
  "authentication_error",
  "permission_error",
]);
const ALLOWANCE_CODES = new Set(["usage_limit_exceeded", "billing_error", "blocking_limit"]);
const LASTING_CODES = new Set([
  "bad_request",
  "invalid_request",
  "invalid_request_error",
  "not_found_error",
  "context_window_exceeded",
  "prompt_too_long",
  "model_not_found",
  "max_output_tokens",
  "provider.no-route",
  "provider.invalid-request",
]);
const LASTING_CLASSES = new Set(["validation_error", "permission_error"]);
const PASSING_CODES: Readonly<Record<string, string>> = {
  overloaded: "Provider overloaded",
  overloaded_error: "Provider overloaded",
  server_overloaded: "Provider overloaded",
  model_cap: "Model at capacity",
  rate_limit: "Rate limited",
  rate_limit_error: "Rate limited",
  http_connection_failed: "Connection dropped",
  response_stream_connection_failed: "Connection dropped",
  response_stream_disconnected: "Connection dropped",
  "provider.transport": "Connection dropped",
  internal_server_error: "Provider server error",
  server_error: "Provider server error",
  "provider.internal": "Provider server error",
};
const SIGNED_OUT_WORDS =
  /\b(?:status|error|HTTP)\b[^\n]{0,12}\b40[13]\b|\b401 Unauthorized\b|authentication failed|please run \/login|login expired|not logged in|oauth token/i;
const ALLOWANCE_WORDS =
  /usage limit|(?<!rate.?)limit reached|hit your [\w ]*limit|\bquota\b|billing|credit balance|payment/i;
const LASTING_WORDS =
  /not supported|unsupported|does not exist|may not exist|not found|unknown (?:provider|model)|model unavailable|invalid|context (?:window|length)|too long|flagged|content policy/i;
const PASSING_WORDS: ReadonlyArray<readonly [RegExp, string]> = [
  [/at capacity/i, "Model at capacity"],
  [/rate.?limit|too many requests|\b429\b/i, "Rate limited"],
  [/overloaded|\b529\b/i, "Provider overloaded"],
  [
    /socket|connection|network|\bE(?:CONN\w+|TIMEDOUT|AI_AGAIN|PIPE)\b|timed? ?out|fetch failed|stream (?:ended|closed|disconnected|error)|disconnect|went to sleep/i,
    "Connection dropped",
  ],
  [
    /\b(?:API error|status|HTTP)\b[^\n]{0,12}\b5\d\d\b|internal server error|bad gateway|service unavailable|upstream|server error|you can retry your request|try again later/i,
    "Provider server error",
  ],
];

/**
 * Signed out, out of allowance, and refused requests (unknown model, context too large) are lasting. Overloaded, rate
 * limited, a dropped connection and a 5xx pass. A usage limit is never waited out here: T3 has its own reset-time
 * recovery for it.
 */
export function providerFailureVerdict(
  failure: Pick<OrchestrationV2ProviderFailure, "class" | "code" | "message">,
): ProviderFailureVerdict {
  const code = failure.code?.toLowerCase() ?? "";
  const status = Number(/^api_error_(\d{3})$/.exec(code)?.[1] ?? 0);
  const words = failure.message;
  const lasting = (reason: string): ProviderFailureVerdict => ({ passing: false, reason });
  if (
    SIGNED_OUT_CODES.has(code) ||
    status === 401 ||
    status === 403 ||
    SIGNED_OUT_WORDS.test(words)
  )
    return lasting("Signed out");
  if (ALLOWANCE_CODES.has(code) || failure.class === "usage_limit" || ALLOWANCE_WORDS.test(words))
    return lasting("Out of allowance");
  if (
    LASTING_CODES.has(code) ||
    LASTING_CLASSES.has(failure.class) ||
    (status >= 400 && status < 500 && status !== 408 && status !== 429) ||
    LASTING_WORDS.test(words)
  ) {
    const text = `${code} ${words}`;
    return lasting(
      /context|too long/i.test(text)
        ? "Context window full"
        : /model|no-route/i.test(text)
          ? "Model not available"
          : "Provider refused it",
    );
  }
  const said = PASSING_WORDS.find(([pattern]) => pattern.test(words))?.[1];
  const passing = (reason: string): ProviderFailureVerdict => ({
    passing: true,
    reason: said ?? reason,
  });
  const known = PASSING_CODES[code];
  if (known !== undefined) return passing(known);
  if (status === 429) return passing("Rate limited");
  if (status === 408 || status >= 500)
    return passing(status === 529 ? "Provider overloaded" : "Provider server error");
  if (failure.class === "transport_error") return passing("Connection dropped");
  return said === undefined ? lasting("Provider error") : passing(said);
}

const MINUTE_MS = 60_000;

/** The waits before each continuation: four over 15 minutes, so the fifth cut-off turn in a row is final. */
export const PROVIDER_RETRY_WAITS_MS: ReadonlyArray<number> = [1, 2, 4, 8].map(
  (minutes) => minutes * MINUTE_MS,
);

/** How long to wait after a turn's `cuts`-th cut-off in a row before continuing it, or null when it fails now. */
export function providerRetryWaitMs(verdict: ProviderFailureVerdict, cuts: number): number | null {
  if (!verdict.passing || cuts < 1) return null;
  return PROVIDER_RETRY_WAITS_MS[cuts - 1] ?? null;
}

/**
 * What a cut-off turn is told when its wait is over: the outward steps it completed before the cut, and to check the
 * rest before redoing any of it. `done` is null when nothing could list those steps.
 */
export function providerRetryContinuePrompt(
  reason: string,
  done: ReadonlyArray<string> | null,
): string {
  const cut = `Your last turn was cut off by the model's provider (${reason.toLowerCase()}). Carry on where you left off.`;
  const rest = "Check what else you already did (commits, pushes, files) before repeating it.";
  if (done === null)
    return `${cut} Before repeating any comment, review, PR, card, Linear or Slack write, or thread launch, check whether you already did it before the cut. ${rest}`;
  if (done.length === 0)
    return `${cut} Before the cut you completed no comment, review, PR, card, Linear or Slack write, or thread launch. ${rest}`;
  const listed = done.map((step) => `- ${step}`).join("\n");
  return `${cut} Before the cut you already completed these outward steps; they are done, and an exact repeat is blocked:\n${listed}\n${rest}`;
}
