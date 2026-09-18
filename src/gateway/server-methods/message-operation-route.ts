import { readStringValue } from "@openclaw/normalization-core/string-coerce";
import { ErrorCodes, errorShape } from "../../../packages/gateway-protocol/src/index.js";
import type { ConversationReadInvocationOrigin } from "../../channels/plugins/conversation-read-origin.js";
import { resolveChannelDefaultAccountId } from "../../channels/plugins/helpers.js";
import type { ChannelPlugin } from "../../channels/plugins/types.public.js";
import type { OpenClawConfig } from "../../config/types.openclaw.js";
import { validateExplicitMessageAccountSelection } from "../../infra/outbound/message-account-selection.js";
import { KeyedAsyncQueue } from "../../plugin-sdk/keyed-async-queue.js";
import { normalizeAccountId, normalizeOptionalAccountId } from "../../routing/session-key.js";
import { normalizeMessageChannel } from "../../utils/message-channel.js";
import { DEDUPE_MAX, DEDUPE_TTL_MS } from "../server-constants.js";
import { formatForLog } from "../ws-log.js";
import {
  resolveGatewayInflightRequest as resolveIdempotentGatewayRequest,
  runGatewayInflightWork,
  type GatewayInflightResult as InflightResult,
} from "./inflight.js";
import type { GatewayRequestContext, RespondFn } from "./types.js";

type MessageOperationPrefix = "message.action" | "poll" | "send";

type MessageOperationRoute = {
  channel: string;
  accountId: string;
  requestScope: string;
};

type MessageOperationRouteBinding = {
  key: string;
  reservedRoute?: MessageOperationRoute;
};

type MessageOperationRouteBindingEntry = {
  requestScope: string;
  retainUntilSettled: boolean;
  ts: number;
};

// Send and poll callers can spell one canonical route four ways by omitting or
// supplying channel/account defaults. Preserve every alias for the full result budget.
const MESSAGE_OPERATION_ROUTE_BINDING_MAX = DEDUPE_MAX * 4;
const messageOperationRouteBindings = new WeakMap<
  GatewayRequestContext,
  Map<string, MessageOperationRouteBindingEntry>
>();
const messageOperationRouteBindingQueues = new WeakMap<GatewayRequestContext, KeyedAsyncQueue>();

function pruneMessageOperationRouteBindings(
  bindings: Map<string, MessageOperationRouteBindingEntry>,
  now: number,
): void {
  for (const [key, entry] of bindings) {
    if (!entry.retainUntilSettled && now - entry.ts > DEDUPE_TTL_MS) {
      bindings.delete(key);
    }
  }
  const excess = bindings.size - MESSAGE_OPERATION_ROUTE_BINDING_MAX;
  if (excess <= 0) {
    return;
  }
  const oldestSettledKeys = [...bindings.entries()]
    .filter(([, entry]) => !entry.retainUntilSettled)
    .toSorted(([, left], [, right]) => left.ts - right.ts)
    .slice(0, excess)
    .map(([key]) => key);
  for (const key of oldestSettledKeys) {
    bindings.delete(key);
  }
}

function getMessageOperationRouteBindings(
  context: GatewayRequestContext,
): Map<string, MessageOperationRouteBindingEntry> {
  let bindings = messageOperationRouteBindings.get(context);
  if (!bindings) {
    bindings = new Map();
    messageOperationRouteBindings.set(context, bindings);
  }
  pruneMessageOperationRouteBindings(bindings, Date.now());
  return bindings;
}

function getMessageOperationRouteBindingQueue(context: GatewayRequestContext): KeyedAsyncQueue {
  let queue = messageOperationRouteBindingQueues.get(context);
  if (!queue) {
    queue = new KeyedAsyncQueue();
    messageOperationRouteBindingQueues.set(context, queue);
  }
  return queue;
}

async function acquireMessageOperationRouteBindingLock(params: {
  context: GatewayRequestContext;
  binding: MessageOperationRouteBinding | undefined;
}): Promise<() => void> {
  if (!params.binding) {
    return () => undefined;
  }

  let signalAcquired: (() => void) | undefined;
  let signalRelease: (() => void) | undefined;
  const acquired = new Promise<void>((resolve) => {
    signalAcquired = resolve;
  });
  const held = new Promise<void>((resolve) => {
    signalRelease = resolve;
  });
  // The lock covers mutable route selection through canonical in-flight registration.
  // Otherwise a later retry can bind newer defaults while the first request is resolving.
  void getMessageOperationRouteBindingQueue(params.context).enqueue(
    params.binding.key,
    async () => {
      signalAcquired?.();
      await held;
    },
  );
  await acquired;

  let released = false;
  return () => {
    if (released) {
      return;
    }
    released = true;
    signalRelease?.();
  };
}

function resolveMessageOperationAuthorityScope(params: {
  prefix: MessageOperationPrefix;
  conversationReadOrigin?: ConversationReadInvocationOrigin;
  operation?: string;
}): string {
  return params.prefix === "message.action"
    ? `:${params.conversationReadOrigin ?? "delegated"}:${params.operation ?? "unknown"}`
    : "";
}

function resolveGatewayInflightRequest(params: {
  context: GatewayRequestContext;
  prefix: MessageOperationPrefix;
  idempotencyKey: string;
  respond: RespondFn;
  conversationReadOrigin?: ConversationReadInvocationOrigin;
  operation?: string;
  requestScope?: string;
}):
  | {
      kind: "ready";
      idem: string;
      dedupeKey: string;
      inflightMap: Map<string, Promise<InflightResult>>;
    }
  | {
      kind: "handled";
      done: Promise<void>;
    } {
  const idem = params.idempotencyKey;
  const authorityScope = resolveMessageOperationAuthorityScope(params);
  const requestScope = params.requestScope ? `:${params.requestScope}` : "";
  const dedupeKey = `${params.prefix}${authorityScope}${requestScope}:${idem}`;
  return resolveIdempotentGatewayRequest({
    context: params.context,
    dedupeKey,
    idempotencyKey: idem,
    respond: params.respond,
  });
}

function parseMessageOperationRoute(
  requestScope: string | undefined,
): MessageOperationRoute | undefined {
  if (!requestScope) {
    return undefined;
  }
  try {
    const parsed: unknown = JSON.parse(requestScope);
    if (
      !Array.isArray(parsed) ||
      parsed.length !== 2 ||
      typeof parsed[0] !== "string" ||
      typeof parsed[1] !== "string"
    ) {
      return undefined;
    }
    const channel = normalizeMessageChannel(parsed[0]);
    const accountId = normalizeOptionalAccountId(parsed[1]);
    if (!channel || channel !== parsed[0] || !accountId || accountId !== parsed[1]) {
      return undefined;
    }
    return { channel, accountId, requestScope };
  } catch {
    return undefined;
  }
}

function resolveMessageOperationRouteBinding(params: {
  context: GatewayRequestContext;
  prefix: MessageOperationPrefix;
  idempotencyKey: string;
  conversationReadOrigin?: ConversationReadInvocationOrigin;
  operation?: string;
  requestChannel: unknown;
  accountIds: readonly unknown[];
}): MessageOperationRouteBinding | undefined {
  const rawChannel = readStringValue(params.requestChannel);
  const channel = rawChannel ? normalizeMessageChannel(rawChannel) : undefined;
  if (rawChannel && !channel) {
    return undefined;
  }
  const providedAccountIds = params.accountIds.filter(
    (value) => value !== undefined && value !== null && (typeof value !== "string" || value.trim()),
  );
  const normalizedAccountIds = providedAccountIds.map((value) =>
    typeof value === "string" ? normalizeOptionalAccountId(value) : undefined,
  );
  if (normalizedAccountIds.some((accountId) => !accountId)) {
    return undefined;
  }
  // SAFETY: the preceding guard returns if any normalized account is absent.
  const distinctAccountIds = [...new Set(normalizedAccountIds as string[])];
  if (distinctAccountIds.length > 1) {
    return undefined;
  }
  const accountId = distinctAccountIds[0];
  const authorityScope = resolveMessageOperationAuthorityScope(params);
  const explicitRouteScope = JSON.stringify([channel ?? null, accountId ?? null]);
  const key = `${params.prefix}${authorityScope}:route-binding:${explicitRouteScope}:${params.idempotencyKey}`;
  return {
    key,
    reservedRoute: parseMessageOperationRoute(
      getMessageOperationRouteBindings(params.context).get(key)?.requestScope,
    ),
  };
}

function bindMessageOperationRoute(params: {
  context: GatewayRequestContext;
  binding: MessageOperationRouteBinding | undefined;
  requestScope: string;
}): boolean {
  if (!params.binding) {
    return true;
  }
  const bindings = getMessageOperationRouteBindings(params.context);
  const existing = bindings.get(params.binding.key);
  if (existing) {
    if (existing.requestScope !== params.requestScope) {
      return false;
    }
    bindings.set(params.binding.key, { ...existing, ts: Date.now() });
    return true;
  }
  // Bind the canonical route before dispatch so retries can replay without
  // consulting mutable defaults or plugin/account configuration.
  bindings.set(params.binding.key, {
    ts: Date.now(),
    requestScope: params.requestScope,
    retainUntilSettled: false,
  });
  pruneMessageOperationRouteBindings(bindings, Date.now());
  return true;
}

function refreshMessageOperationRouteBinding(params: {
  context: GatewayRequestContext;
  binding: MessageOperationRouteBinding | undefined;
  requestScope: string;
}): void {
  if (!params.binding) {
    return;
  }
  const bindings = getMessageOperationRouteBindings(params.context);
  const existing = bindings.get(params.binding.key);
  if (existing?.requestScope === params.requestScope) {
    bindings.set(params.binding.key, {
      ...existing,
      ts: Date.now(),
      retainUntilSettled: false,
    });
    pruneMessageOperationRouteBindings(bindings, Date.now());
  }
}

function retainMessageOperationRouteBinding(params: {
  context: GatewayRequestContext;
  binding: MessageOperationRouteBinding | undefined;
  requestScope: string;
}): void {
  if (!params.binding) {
    return;
  }
  const bindings = getMessageOperationRouteBindings(params.context);
  const existing = bindings.get(params.binding.key);
  if (existing?.requestScope === params.requestScope) {
    // Active provider work owns this alias even past TTL or capacity pressure;
    // settlement below restarts ordinary expiry.
    bindings.set(params.binding.key, {
      ...existing,
      retainUntilSettled: true,
    });
  }
}

function replayReservedMessageOperationRoute(params: {
  context: GatewayRequestContext;
  binding: MessageOperationRouteBinding | undefined;
  prefix: MessageOperationPrefix;
  idempotencyKey: string;
  respond: RespondFn;
  conversationReadOrigin?: ConversationReadInvocationOrigin;
  operation?: string;
}): Promise<void> | undefined {
  if (!params.binding?.reservedRoute) {
    return undefined;
  }
  const inflight = resolveGatewayInflightRequest({
    context: params.context,
    prefix: params.prefix,
    idempotencyKey: params.idempotencyKey,
    respond: params.respond,
    conversationReadOrigin: params.conversationReadOrigin,
    operation: params.operation,
    requestScope: params.binding.reservedRoute.requestScope,
  });
  if (inflight.kind === "ready") {
    return undefined;
  }
  return inflight.done;
}

function resolveMessageOperationAccountRoute(params: {
  cfg: OpenClawConfig;
  channel: string;
  plugin: ChannelPlugin;
  accountIds: readonly unknown[];
  conflictMessage: string;
}): { accountId: string | undefined; effectiveAccountId: string; requestScope: string } {
  const accountIds = params.accountIds
    .map((accountId) =>
      validateExplicitMessageAccountSelection({
        cfg: params.cfg,
        channel: params.channel,
        accountId,
        plugin: params.plugin,
      }),
    )
    .filter((accountId): accountId is string => accountId !== undefined);
  const distinctAccountIds = [...new Set(accountIds)];
  if (distinctAccountIds.length > 1) {
    throw new Error(params.conflictMessage);
  }
  const accountId = distinctAccountIds[0];
  // Missing input remains host-derived authority; this value only canonicalizes
  // idempotency and is not forwarded as a caller-supplied explicit selection.
  const effectiveAccountId =
    accountId ??
    normalizeAccountId(resolveChannelDefaultAccountId({ plugin: params.plugin, cfg: params.cfg }));
  return {
    accountId,
    effectiveAccountId,
    requestScope: JSON.stringify([params.channel, effectiveAccountId]),
  };
}

export async function withMessageOperationRoute<
  T extends {
    cfg: OpenClawConfig;
    channel: string;
    plugin: ChannelPlugin;
  },
>(params: {
  context: GatewayRequestContext;
  prefix: MessageOperationPrefix;
  idempotencyKey: string;
  respond: RespondFn;
  conversationReadOrigin?: ConversationReadInvocationOrigin;
  operation?: string;
  requestChannel: unknown;
  bindingAccountIds: readonly unknown[];
  routeAccountIds: (binding: MessageOperationRouteBinding | undefined) => readonly unknown[];
  conflictMessage: string;
  authorize?: () => boolean;
  /** Ephemeral scheduled reads must consult current provider policy on every invocation. */
  replayResults?: boolean;
  resolveChannel: (requestChannel: unknown) => Promise<T | undefined>;
  work: (
    route: T & {
      accountId: string | undefined;
      idem: string;
      dedupeKey: string | undefined;
      authorize: () => boolean;
    },
  ) => Promise<InflightResult>;
}): Promise<void> {
  if (params.replayResults === false) {
    const resolved = await params.resolveChannel(params.requestChannel);
    if (!resolved) {
      return;
    }
    try {
      const accountRoute = resolveMessageOperationAccountRoute({
        ...resolved,
        accountIds: params.routeAccountIds(undefined),
        conflictMessage: params.conflictMessage,
      });
      const authorize = params.authorize ?? (() => true);
      const assertCurrent = () => {
        if (!authorize()) {
          throw new Error("agent runtime authority is no longer active");
        }
      };
      assertCurrent();
      const result = await params.work({
        ...resolved,
        accountId: accountRoute.effectiveAccountId,
        idem: params.idempotencyKey,
        dedupeKey: undefined,
        authorize,
      });
      assertCurrent();
      params.respond(result.ok, result.payload, result.error, result.meta);
    } catch (error) {
      respondGatewayInvalidRequest({ respond: params.respond, channel: resolved.channel, error });
    }
    return;
  }
  const bindingParams = {
    context: params.context,
    prefix: params.prefix,
    idempotencyKey: params.idempotencyKey,
    conversationReadOrigin: params.conversationReadOrigin,
    operation: params.operation,
    requestChannel: params.requestChannel,
    accountIds: params.bindingAccountIds,
  };
  let binding = resolveMessageOperationRouteBinding(bindingParams);
  const releaseLock = await acquireMessageOperationRouteBindingLock({
    context: params.context,
    binding,
  });
  try {
    // Re-resolve under the lock so route aliases bind against current state; replay
    // releases first because awaiting while locked would deadlock concurrent retries.
    binding = resolveMessageOperationRouteBinding(bindingParams);
    const reservedReplay = replayReservedMessageOperationRoute({
      context: params.context,
      binding,
      prefix: params.prefix,
      idempotencyKey: params.idempotencyKey,
      respond: params.respond,
      conversationReadOrigin: params.conversationReadOrigin,
      operation: params.operation,
    });
    if (reservedReplay) {
      releaseLock();
      await reservedReplay;
      return;
    }
    const resolved = await params.resolveChannel(
      binding?.reservedRoute?.channel ?? params.requestChannel,
    );
    if (!resolved) {
      return;
    }
    let accountRoute: ReturnType<typeof resolveMessageOperationAccountRoute>;
    try {
      accountRoute = resolveMessageOperationAccountRoute({
        ...resolved,
        accountIds: params.routeAccountIds(binding),
        conflictMessage: params.conflictMessage,
      });
    } catch (error) {
      respondGatewayInvalidRequest({ respond: params.respond, channel: resolved.channel, error });
      return;
    }
    if (
      !bindMessageOperationRoute({
        context: params.context,
        binding,
        requestScope: accountRoute.requestScope,
      })
    ) {
      respondGatewayInvalidRequest({
        respond: params.respond,
        channel: resolved.channel,
        error: "idempotency key is already bound to a different message route",
      });
      return;
    }
    const inflight = resolveGatewayInflightRequest({
      context: params.context,
      prefix: params.prefix,
      idempotencyKey: params.idempotencyKey,
      respond: params.respond,
      conversationReadOrigin: params.conversationReadOrigin,
      operation: params.operation,
      requestScope: accountRoute.requestScope,
    });
    if (inflight.kind === "handled") {
      releaseLock();
      await inflight.done;
      return;
    }
    // Routing and attachment preparation may yield while the admitted run
    // closes. Revalidate before any provider-visible message side effect.
    if (params.authorize && !params.authorize()) {
      params.respond(
        false,
        undefined,
        errorShape(ErrorCodes.INVALID_REQUEST, "agent runtime authority is no longer active"),
      );
      return;
    }
    retainMessageOperationRouteBinding({
      context: params.context,
      binding,
      requestScope: accountRoute.requestScope,
    });
    const work = params
      .work({
        ...resolved,
        accountId: accountRoute.accountId,
        idem: inflight.idem,
        dedupeKey: inflight.dedupeKey,
        authorize: params.authorize ?? (() => true),
      })
      .finally(() => {
        refreshMessageOperationRouteBinding({
          context: params.context,
          binding,
          requestScope: accountRoute.requestScope,
        });
      });
    const inflightWork = runGatewayInflightWork({ ...inflight, work, respond: params.respond });
    releaseLock();
    await inflightWork;
  } finally {
    releaseLock();
  }
}

function respondGatewayInvalidRequest(params: {
  respond: RespondFn;
  channel: string;
  error: unknown;
}): void {
  params.respond(false, undefined, errorShape(ErrorCodes.INVALID_REQUEST, String(params.error)), {
    channel: params.channel,
    error: formatForLog(params.error),
  });
}
