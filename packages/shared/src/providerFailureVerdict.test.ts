import { describe, expect, it } from "vite-plus/test";

import {
  PROVIDER_RETRY_WAITS_MS,
  providerFailureVerdict,
  providerRetryContinuePrompt,
  providerRetryWaitMs,
} from "./providerFailureVerdict.ts";

const failure = (
  input: Partial<{ class: Parameters<typeof providerFailureVerdict>[0]["class"]; code: string }> & {
    message?: string;
  },
) => ({
  class: input.class ?? "provider_error",
  code: input.code ?? null,
  message: input.message ?? "The request failed.",
});

describe("providerFailureVerdict", () => {
  it.each([
    [failure({ code: "overloaded_error" }), "Provider overloaded"],
    [failure({ code: "api_error_529" }), "Provider overloaded"],
    [failure({ code: "rate_limit_error" }), "Rate limited"],
    [failure({ code: "api_error_429" }), "Rate limited"],
    [failure({ code: "api_error_500" }), "Provider server error"],
    [failure({ code: "api_error_503" }), "Provider server error"],
    [failure({ class: "transport_error" }), "Connection dropped"],
    [failure({ message: "socket hang up" }), "Connection dropped"],
    [failure({ message: "API Error: 502 Bad Gateway" }), "Provider server error"],
  ])("passes %o as %s", (input, reason) => {
    expect(providerFailureVerdict(input)).toEqual({ passing: true, reason });
  });

  it.each([
    [failure({ code: "authentication_failed" }), "Signed out"],
    [failure({ message: "Not logged in. Please run /login" }), "Signed out"],
    [failure({ class: "usage_limit", message: "You've hit your limit" }), "Out of allowance"],
    [failure({ code: "billing_error" }), "Out of allowance"],
    [failure({ code: "model_not_found" }), "Model not available"],
    [failure({ message: "Unknown model claude-x" }), "Model not available"],
    [failure({ code: "prompt_too_long", message: "Prompt is too long" }), "Context window full"],
    [failure({ code: "prompt_too_long" }), "Provider refused it"],
    [failure({ code: "api_error_400", message: "bad" }), "Provider refused it"],
    [failure({ class: "validation_error" }), "Provider refused it"],
  ])("does not pass %o: %s", (input, reason) => {
    expect(providerFailureVerdict(input)).toEqual({ passing: false, reason });
  });

  it("does not pass an error it does not recognise", () => {
    expect(providerFailureVerdict(failure({ class: "unknown", message: "Something odd" }))).toEqual(
      { passing: false, reason: "Provider error" },
    );
  });
});

describe("providerRetryWaitMs", () => {
  const passing = { passing: true, reason: "Provider overloaded" };

  it("waits 1, 2, 4 and 8 minutes, then gives up on the fifth cut", () => {
    expect([1, 2, 3, 4, 5].map((cuts) => providerRetryWaitMs(passing, cuts))).toEqual([
      60_000,
      120_000,
      240_000,
      480_000,
      null,
    ]);
    expect(PROVIDER_RETRY_WAITS_MS.reduce((total, wait) => total + wait, 0)).toBe(15 * 60_000);
  });

  it("never waits on a lasting failure", () => {
    expect(providerRetryWaitMs({ passing: false, reason: "Signed out" }, 1)).toBeNull();
  });
});

describe("providerRetryContinuePrompt", () => {
  it("lists the steps already taken", () => {
    const text = providerRetryContinuePrompt("Provider overloaded", [
      "gh pr comment 12 --body done",
    ]);
    expect(text).toContain("cut off by the model's provider (provider overloaded)");
    expect(text).toContain("- gh pr comment 12 --body done");
    expect(text).toContain("an exact repeat is blocked");
  });

  it("says when nothing outward was done, and when nothing could tell", () => {
    expect(providerRetryContinuePrompt("Rate limited", [])).toContain(
      "you completed no comment, review, PR",
    );
    const unknown = providerRetryContinuePrompt("Rate limited", null);
    expect(unknown).toContain("check whether you already did it");
    expect(unknown).not.toContain("blocked");
  });
});
