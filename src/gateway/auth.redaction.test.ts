import { describe, expect, it } from "vitest";
import { REDACTED_SENTINEL } from "../config/redact-sentinel.js";
import { refResolutionError } from "../secrets/resolve-errors.js";
import {
  assertGatewayAuthConfigured,
  authorizeHttpGatewayConnect,
  authorizeControlUiReadHttpGatewayConnect,
  authorizeWsControlUiGatewayConnect,
} from "./auth.js";
import { createRuntimeSecretsActivator } from "./server-startup-config.js";

describe.each([
  ["HTTP", authorizeHttpGatewayConnect],
  ["Control UI HTTP read", authorizeControlUiReadHttpGatewayConnect],
  ["WebSocket", authorizeWsControlUiGatewayConnect],
] as const)("%s shared-secret fields", (_surface, authorize) => {
  it.each(["token", "password"] as const)(
    "rejects a redacted %s before shared-secret or Tailscale authentication",
    async (mode) => {
      const auth = { mode, [mode]: REDACTED_SENTINEL, allowTailscale: true };
      expect(() => assertGatewayAuthConfigured(auth)).toThrow(/redaction sentinel.*doctor --fix/);
      for (const connectAuth of [{ [mode]: REDACTED_SENTINEL }, null]) {
        await expect(
          authorize({
            auth,
            connectAuth,
            ingressAttribution: {
              kind: "tailscale-serve",
              clientIp: "100.64.0.1",
              rateLimit: { subject: { key: "synthetic-tailnet-user" }, resetOnSuccess: true },
              verifyIdentity: async () => ({ login: "operator@example.test", name: "Operator" }),
            },
            browserOriginPolicy: { fetchSite: "same-origin" },
          }),
        ).resolves.toEqual({ ok: false, reason: `${mode}_redacted_config` });
      }
    },
  );
});

it("keeps the corrupted store entry and Doctor remedy in the startup refusal", async () => {
  const ref = { source: "store", provider: "default", id: "OPENCLAW_GATEWAY_TOKEN" } as const;
  const activate = createRuntimeSecretsActivator({
    logSecrets: { info() {}, warn() {}, error() {} },
    emitStateEvent() {},
    activateRuntimeSecretsSnapshot() {},
    prepareRuntimeSecretsSnapshot: async () => {
      throw refResolutionError({
        code: "SECRET_REF_REDACTED_VALUE",
        source: ref.source,
        provider: ref.provider,
        refId: ref.id,
        message: "synthetic placeholder failure",
      });
    },
  });
  const failure = await activate(
    { gateway: { auth: { mode: "token", token: ref } } },
    { reason: "startup", activate: false, env: {} },
  ).catch((error: unknown) => error);
  expect(failure).toBeInstanceOf(Error);
  expect(String(failure)).toContain(ref.id);
  expect(String(failure)).toContain("redaction placeholder");
  expect(String(failure)).toContain("openclaw doctor --fix");
});
