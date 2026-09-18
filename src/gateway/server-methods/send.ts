// Send gateway methods route operator/tool messages and poll actions through
// channel plugins, outbound session state, durable delivery, and transcript mirrors.
import {
  normalizeOptionalLowercaseString,
  normalizeOptionalString,
  readStringValue,
} from "@openclaw/normalization-core/string-coerce";
import {
  ErrorCodes,
  GatewayErrorDetailCodes,
  errorShape,
  validateMessageActionParams,
  validatePollParams,
  validateSendParams,
} from "../../../packages/gateway-protocol/src/index.js";
import { sendDurableMessageBatchCore } from "../../channels/message/runtime.js";
import { dispatchChannelMessageAction } from "../../channels/plugins/message-action-dispatch.js";
import { resolveChannelThreadAddressing } from "../../channels/thread-addressing.js";
import {
  createChannelPartialDeliveryError,
  isChannelPartialDeliveryError,
} from "../../channels/turn/partial-delivery-error.js";
import { createOutboundSendDeps } from "../../cli/deps.js";
import {
  getRuntimeConfigSnapshot,
  getRuntimeConfigSourceSnapshot,
  selectApplicableRuntimeConfig,
} from "../../config/runtime-snapshot.js";
import type { OpenClawConfig } from "../../config/types.openclaw.js";
import { resolveOutboundChannelPlugin } from "../../infra/outbound/channel-resolution.js";
import { resolveMessageChannelSelection } from "../../infra/outbound/channel-selection.js";
import { OutboundDeliveryError } from "../../infra/outbound/deliver-types.js";
import { resolveImplicitMessageActionTarget } from "../../infra/outbound/message-action-normalization.js";
import {
  hydrateAttachmentParamsForAction,
  resolveAttachmentMediaPolicy,
} from "../../infra/outbound/message-action-params.js";
import { actionHasTarget } from "../../infra/outbound/message-action-spec.js";
import {
  ensureOutboundSessionEntry,
  resolveOutboundSessionRoute,
} from "../../infra/outbound/outbound-session.js";
import {
  createOutboundPayloadPlan,
  projectOutboundPayloadPlanForMirror,
} from "../../infra/outbound/payloads.js";
import { buildOutboundSessionContext } from "../../infra/outbound/session-context.js";
import {
  beginTerminalSourceReplyDelivery,
  cancelTerminalSourceReplyDelivery,
  mirrorDeliveredSourceReplyToTranscript,
  reconcileTerminalSourceReplyDelivery,
} from "../../infra/outbound/source-reply-mirror.js";
import { maybeResolveIdLikeTarget } from "../../infra/outbound/target-resolver.js";
import { resolveOutboundTarget } from "../../infra/outbound/targets.js";
import { getAgentScopedMediaLocalRoots } from "../../media/local-roots.js";
import { resolveAgentScopedOutboundMediaAccess } from "../../media/read-capability.js";
import { KeyedAsyncQueue } from "../../plugin-sdk/keyed-async-queue.js";
import { extractToolPayload } from "../../plugin-sdk/tool-payload.js";
import { normalizePollInput } from "../../polls.js";
import {
  isAgentHarnessSessionKey,
  resolveMissingAgentHarnessSessionError,
} from "../../sessions/agent-harness-session-key.js";
import {
  normalizeSessionKeyPreservingOpaquePeerIds,
  parseThreadSessionSuffix,
} from "../../sessions/session-key-utils.js";
import { withChannelReadAuthority } from "../../shared/channel-read-authority.js";
import { INTERNAL_MESSAGE_CHANNEL, normalizeMessageChannel } from "../../utils/message-channel.js";
import { resolveGatewayConversationReadOrigin } from "../conversation-read-origin.js";
import { selectMessageActionRequesterIdentity } from "../message-action-turn-capability.js";
import {
  authorizeGatewaySessionCreation,
  resolveSandboxedSessionCreation,
} from "../operator-role-policy.js";
import { ADMIN_SCOPE } from "../operator-scopes.js";
import { resolveRequestedSessionAgentId } from "../session-request-agent.js";
import { loadSessionEntry } from "../session-utils.js";
import { formatForLog } from "../ws-log.js";
import type { GatewayInflightResult as InflightResult } from "./inflight.js";
import {
  createMessageActionRuntimeAuthority,
  resolveAgentRuntimeMessageActionAuthorization,
  resolveAgentRuntimeMessageActionConfig,
  resolveTrustedMessageActionToolContext,
} from "./message-action-context.js";
import { withMessageOperationRoute } from "./message-operation-route.js";
import type { GatewayRequestContext, GatewayRequestHandlers } from "./types.js";
import { assertValidParams } from "./validation.js";

async function resolveRequestedChannel(params: {
  requestChannel: unknown;
  unsupportedMessage: (input: string) => string;
  context: GatewayRequestContext;
  config?: OpenClawConfig;
  rejectWebchatAsInternalOnly?: boolean;
}): Promise<
  | {
      cfg: OpenClawConfig;
      sourceCfg: OpenClawConfig;
      channel: string;
    }
  | {
      error: ReturnType<typeof errorShape>;
    }
> {
  const channelInput = readStringValue(params.requestChannel);
  const normalizedChannel = channelInput ? normalizeMessageChannel(channelInput) : undefined;
  if (params.rejectWebchatAsInternalOnly && normalizedChannel === INTERNAL_MESSAGE_CHANNEL) {
    return {
      error: errorShape(
        ErrorCodes.INVALID_REQUEST,
        "unsupported channel: webchat (internal-only). Use `chat.send` for WebChat UI messages or choose a deliverable channel.",
      ),
    };
  }
  if (channelInput && !normalizedChannel) {
    return {
      error: errorShape(ErrorCodes.INVALID_REQUEST, params.unsupportedMessage(channelInput)),
    };
  }
  const sourceCfg = params.config ?? params.context.getRuntimeConfig();
  const cfg = sourceCfg;
  let channel = normalizedChannel;
  if (!channel) {
    try {
      channel = (await resolveMessageChannelSelection({ cfg })).channel;
    } catch (err) {
      return { error: errorShape(ErrorCodes.INVALID_REQUEST, String(err)) };
    }
  }
  return { cfg, sourceCfg, channel };
}

async function resolveInternalDeliveryChannel(
  requestChannel: unknown,
  context: GatewayRequestContext,
  config?: OpenClawConfig,
): Promise<
  | {
      kind: "ready";
      cfg: OpenClawConfig;
      sourceCfg: OpenClawConfig;
      channel: string;
    }
  | {
      kind: "failed";
      result: InflightResult;
    }
> {
  const resolvedChannel = await resolveRequestedChannel({
    requestChannel,
    unsupportedMessage: (input) => `unsupported channel: ${input}`,
    context,
    config,
    rejectWebchatAsInternalOnly: true,
  });
  if ("error" in resolvedChannel) {
    return {
      kind: "failed",
      result: { ok: false, error: resolvedChannel.error },
    };
  }
  return { kind: "ready", ...resolvedChannel };
}

function resolveGatewayOutboundTarget(params: {
  channel: string;
  to: string;
  cfg: OpenClawConfig;
  accountId?: string;
}):
  | {
      ok: true;
      to: string;
    }
  | {
      ok: false;
      error: ReturnType<typeof errorShape>;
    } {
  const resolved = resolveOutboundTarget({
    channel: params.channel,
    to: params.to,
    cfg: params.cfg,
    accountId: params.accountId,
    mode: "explicit",
  });
  if (!resolved.ok) {
    return {
      ok: false,
      error: errorShape(ErrorCodes.INVALID_REQUEST, String(resolved.error)),
    };
  }
  return { ok: true, to: resolved.to };
}

function resolveMessageActionRuntimeConfig(params: {
  cfg: OpenClawConfig;
  sourceCfg: OpenClawConfig;
}): OpenClawConfig {
  const runtimeConfig = getRuntimeConfigSnapshot();
  const runtimeSourceConfig = getRuntimeConfigSourceSnapshot();
  if (!runtimeConfig || !runtimeSourceConfig) {
    return params.cfg;
  }
  const selected = selectApplicableRuntimeConfig({
    inputConfig: params.sourceCfg,
    runtimeConfig,
    runtimeSourceConfig,
  });
  // Message actions must use the hot runtime snapshot when it matches the caller's source config.
  if (selected === runtimeConfig && selected !== params.cfg) {
    return selected;
  }
  return params.cfg;
}

function buildGatewayDeliveryPayload(params: {
  runId: string;
  channel: string;
  result: Record<string, unknown>;
}): Record<string, unknown> {
  const payload: Record<string, unknown> = {
    runId: params.runId,
    messageId: params.result.messageId,
    channel: params.channel,
  };
  const optionalKeys = ["chatId", "channelId", "toJid", "conversationId", "pollId"] as const;
  for (const key of optionalKeys) {
    if (key in params.result) {
      payload[key] = params.result[key];
    }
  }
  return payload;
}

function createGatewayInflightResult(params: {
  context: GatewayRequestContext;
  dedupeKey: string | undefined;
  channel: string;
  result: Pick<InflightResult, "ok" | "payload" | "error">;
  meta?: Record<string, unknown>;
}): InflightResult {
  if (params.dedupeKey !== undefined) {
    params.context.dedupe.set(params.dedupeKey, { ts: Date.now(), ...params.result });
  }
  return {
    ...params.result,
    meta: { channel: params.channel, ...params.meta },
  };
}

function createGatewayInflightSuccess(params: {
  context: GatewayRequestContext;
  dedupeKey: string | undefined;
  payload: unknown;
  channel: string;
}): InflightResult {
  return createGatewayInflightResult({ ...params, result: { ok: true, payload: params.payload } });
}

function createGatewayInflightUnavailableFailure(params: {
  context: GatewayRequestContext;
  dedupeKey: string | undefined;
  channel: string;
  err: unknown;
}): InflightResult {
  // A channel partial-delivery error carries the receipt of the part that was
  // already delivered (e.g. a caption sent before the media upload failed).
  // Preserve it on the structured error and mark the result non-retryable so
  // the agent does not resend an already-visible message; `String(err)` alone
  // would drop the receipt and invite a duplicate delivery on retry.
  const partialDelivery = isChannelPartialDeliveryError(params.err)
    ? params.err.deliveryResult
    : undefined;
  const queuedDelivery =
    !partialDelivery &&
    params.err instanceof OutboundDeliveryError &&
    params.err.queueCustody === "held";
  const error = errorShape(
    ErrorCodes.UNAVAILABLE,
    String(params.err),
    partialDelivery
      ? { details: { partialDelivery }, retryable: false }
      : queuedDelivery
        ? { details: { code: GatewayErrorDetailCodes.OUTBOUND_DELIVERY_QUEUED } }
        : undefined,
  );
  return createGatewayInflightResult({
    ...params,
    result: { ok: false, error },
    meta: { error: formatForLog(params.err) },
  });
}

function createGatewayInflightAuthorityFailure(params: {
  context: GatewayRequestContext;
  dedupeKey: string | undefined;
  channel: string;
}): InflightResult {
  return createGatewayInflightResult({
    ...params,
    result: {
      ok: false,
      error: errorShape(ErrorCodes.INVALID_REQUEST, "agent runtime authority is no longer active"),
    },
  });
}

async function mirrorDeliveredSourceReplyToTranscriptBestEffort(params: {
  context: GatewayRequestContext;
  mirror: Parameters<typeof mirrorDeliveredSourceReplyToTranscript>[0];
}) {
  try {
    const mirrored = await mirrorDeliveredSourceReplyToTranscript(params.mirror);
    if (!mirrored && params.mirror.sourceReplyFinal === true) {
      params.context.logGateway?.warn?.(
        "Terminal source reply receipt was not mirrored; restart recovery is fail-closed.",
        {
          channel: params.mirror.channel,
          sessionKey: params.mirror.sessionKey,
        },
      );
    }
  } catch (err) {
    params.context.logGateway?.warn?.("Source reply transcript mirror failed after delivery.", {
      error: formatForLog(err),
      channel: params.mirror.channel,
      sessionKey: params.mirror.sessionKey,
    });
  }
}

const sourceReplyTranscriptMirrorQueue = new KeyedAsyncQueue();

function resolveSourceReplyTranscriptMirrorQueueKey(
  mirror: Parameters<typeof mirrorDeliveredSourceReplyToTranscript>[0],
): string {
  // Missing session keys are serialized together so global mirrors preserve delivery order.
  return mirror.sessionKey?.trim() || "__global__";
}

function scheduleDeliveredSourceReplyTranscriptMirror(params: {
  context: GatewayRequestContext;
  mirror: Parameters<typeof mirrorDeliveredSourceReplyToTranscript>[0];
}): Promise<void> {
  const queueKey = resolveSourceReplyTranscriptMirrorQueueKey(params.mirror);
  // Queue per session so current-conversation source replies are visible before
  // a following turn can read the transcript.
  return sourceReplyTranscriptMirrorQueue.enqueue(queueKey, () =>
    mirrorDeliveredSourceReplyToTranscriptBestEffort(params),
  );
}

export const sendHandlers: GatewayRequestHandlers = {
  "message.action": async ({
    params: request,
    respond,
    context,
    client,
    sessionMutationCommitGuard,
  }) => {
    if (!assertValidParams(request, validateMessageActionParams, "message.action", respond)) {
      return;
    }
    const trustedContext = resolveTrustedMessageActionToolContext({ client, request });
    if (!trustedContext.ok) {
      respond(false, undefined, trustedContext.error);
      return;
    }
    const conversationReadOrigin = resolveGatewayConversationReadOrigin({
      client,
      requestedOrigin: request.conversationReadOrigin,
    });
    const messageAuthority = createMessageActionRuntimeAuthority({
      client,
      context,
      respond,
      sessionMutationCommitGuard,
      request,
      authorization: trustedContext.messageActionAuthorization,
    });
    const assertDirectAdapterHandoff = messageAuthority.agentRuntimeAuthority.commitGuard;
    const onPlatformSendDispatch = assertDirectAdapterHandoff
      ? async () => assertDirectAdapterHandoff()
      : undefined;
    const downstreamToolContext = trustedContext.toolContext
      ? { ...trustedContext.toolContext, skipCrossContextDecoration: true as const }
      : undefined;
    const downstreamMessageActionAuthorization = trustedContext.messageActionAuthorization
      ? { ...trustedContext.messageActionAuthorization, toolContext: downstreamToolContext }
      : undefined;
    await withMessageOperationRoute({
      context,
      prefix: "message.action",
      operation: request.action,
      idempotencyKey: request.idempotencyKey,
      respond,
      conversationReadOrigin,
      requestChannel: request.channel,
      bindingAccountIds: [messageAuthority.routeAccountId, request.params.accountId],
      routeAccountIds: (binding) => [
        messageAuthority.routeAccountId,
        request.params.accountId,
        binding?.reservedRoute?.accountId,
      ],
      conflictMessage: "message.action accountId does not match params.accountId",
      authorize: messageAuthority.agentRuntimeAuthority.hasActive,
      replayResults: messageAuthority.assertReadCurrent === undefined,
      resolveChannel: async (requestChannel) => {
        const resolved = await resolveRequestedChannel({
          requestChannel,
          unsupportedMessage: (input) => `unsupported channel: ${input}`,
          context,
          config: trustedContext.messageActionConfig,
          rejectWebchatAsInternalOnly: true,
        });
        if ("error" in resolved) {
          respond(false, undefined, resolved.error);
          return undefined;
        }
        const { cfg: selectedCfg, sourceCfg, channel } = resolved;
        const cfg =
          trustedContext.messageActionConfig ??
          resolveMessageActionRuntimeConfig({ cfg: selectedCfg, sourceCfg });
        const plugin = resolveOutboundChannelPlugin({ channel, cfg });
        const canonicalAction =
          ((request.action === "send" &&
            Boolean(plugin?.message?.send?.text || plugin?.outbound?.sendText)) ||
            (request.action === "poll" && Boolean(plugin?.outbound?.sendPoll))) &&
          (!plugin?.actions?.handleAction ||
            plugin.actions.supportsAction?.({ action: request.action }) === false);
        if (!plugin || (!plugin.actions?.handleAction && !canonicalAction)) {
          respond(
            false,
            undefined,
            errorShape(
              ErrorCodes.INVALID_REQUEST,
              `Channel ${channel} does not support action ${request.action}.`,
            ),
          );
          return undefined;
        }
        return { cfg, channel, plugin, canonicalAction };
      },
      work: async ({ cfg, channel, plugin, canonicalAction, accountId, dedupeKey, authorize }) => {
        try {
          const completed = await withChannelReadAuthority(
            request.action === "download-file" || messageAuthority.assertReadCurrent
              ? assertDirectAdapterHandoff
              : undefined,
            async () => {
              const sessionKey = normalizeOptionalString(request.sessionKey) ?? undefined;
              const requestedAgentId =
                normalizeOptionalString(request.agentId) ?? trustedContext.runtimeAgentId;
              const sessionOwner = sessionKey
                ? resolveRequestedSessionAgentId(cfg, sessionKey, requestedAgentId)
                : undefined;
              if (sessionOwner && !sessionOwner.ok) {
                return { ok: false, error: sessionOwner.error, meta: { channel } };
              }
              const agentId = sessionOwner?.agentId ?? requestedAgentId;
              const sourceReplySessionKey = trustedContext.sourceReplySessionKey;
              const sourceReplyOwner = sourceReplySessionKey
                ? resolveRequestedSessionAgentId(cfg, sourceReplySessionKey, agentId)
                : undefined;
              if (sourceReplyOwner && !sourceReplyOwner.ok) {
                return { ok: false, error: sourceReplyOwner.error, meta: { channel } };
              }
              // Default-agent resolution may fail, so role-free sends must not enter this policy path.
              if (request.action === "send" && cfg.gateway?.roles) {
                const actionAgent =
                  agentId ??
                  sourceReplyOwner?.agentId ??
                  resolveRequestedSessionAgentId(cfg, "main");
                if (typeof actionAgent !== "string" && !actionAgent.ok) {
                  return { ok: false, error: actionAgent.error, meta: { channel } };
                }
                const actionAgentId =
                  typeof actionAgent === "string" ? actionAgent : actionAgent.agentId;
                const agentAccessError = authorizeGatewaySessionCreation({
                  cfg,
                  client,
                  agentId: actionAgentId,
                });
                if (agentAccessError) {
                  return { ok: false, error: agentAccessError, meta: { channel } };
                }
              }
              if (accountId) {
                request.params.accountId = accountId;
              }
              if (
                canonicalAction &&
                request.action === "send" &&
                !normalizeOptionalString(request.params.target) &&
                !actionHasTarget("send", request.params, {
                  channel,
                  aliasSpec: plugin.actions?.messageActionTargetAliases?.send ?? null,
                }) &&
                !resolveImplicitMessageActionTarget(trustedContext.toolContext)
              ) {
                // Native sends could use account defaults without a target. Resolve that
                // owner fact before core routing and source-reply receipts require it.
                const target = resolveOutboundTarget({ channel, plugin, cfg, accountId });
                if (!target.ok) {
                  throw target.error;
                }
                request.params.to = target.to;
              }
              const resolvedMediaAccess = resolveAgentScopedOutboundMediaAccess({
                cfg,
                agentId,
                sessionKey,
                messageProvider: sessionKey ? undefined : channel,
                accountId: sessionKey
                  ? (trustedContext.requesterAccountId ?? accountId)
                  : accountId,
                requesterSenderId: trustedContext.requesterSenderId,
                requesterSenderName: trustedContext.requesterSenderName,
                requesterSenderUsername: trustedContext.requesterSenderUsername,
                requesterSenderE164: trustedContext.requesterSenderE164,
              });
              // Gateway actions receive policy-scoped roots/workspace only; the
              // originating agent turn never delegates its host reader over RPC.
              const mediaAccess = {
                localRoots: resolvedMediaAccess.localRoots,
                ...(resolvedMediaAccess.workspaceDir
                  ? { workspaceDir: resolvedMediaAccess.workspaceDir }
                  : {}),
              };
              if (request.action === "send") {
                await hydrateAttachmentParamsForAction({
                  cfg,
                  channel,
                  accountId,
                  args: request.params,
                  action: "send",
                  mediaPolicy: resolveAttachmentMediaPolicy({
                    mediaAccess: resolvedMediaAccess,
                  }),
                });
              }
              const sourceReplyMirror = {
                action: request.action,
                channel,
                actionParams: request.params,
                cfg,
                accountId,
                currentAccountId: trustedContext.requesterAccountId,
                sessionKey: sourceReplySessionKey ?? sessionKey,
                sessionId: trustedContext.sessionId,
                agentId,
                toolContext: trustedContext.toolContext,
                replyToIsExplicit: request.reply?.source === "explicit",
                idempotencyKey: request.idempotencyKey,
                toolCallId: trustedContext.sourceReplyToolCallId,
                ...(trustedContext.sourceReplyFinal !== undefined
                  ? { sourceReplyFinal: trustedContext.sourceReplyFinal }
                  : {}),
              };
              const terminalDeliveryStart =
                trustedContext.sourceReplyFinal === true
                  ? await beginTerminalSourceReplyDelivery(sourceReplyMirror)
                  : undefined;
              if (terminalDeliveryStart && "outcome" in terminalDeliveryStart) {
                return createGatewayInflightSuccess({
                  context,
                  dedupeKey,
                  payload: terminalDeliveryStart.result,
                  channel,
                });
              }
              const terminalDeliveryReceipt = terminalDeliveryStart;
              // Attachment and receipt preparation can outlive the admitted run.
              // Close the receipt and stop before the provider-owned action boundary.
              if (!authorize()) {
                await cancelTerminalSourceReplyDelivery(terminalDeliveryReceipt);
                return createGatewayInflightAuthorityFailure({ context, dedupeKey, channel });
              }
              const gatewayClientScopes = client?.connect?.scopes ?? [];
              const inboundEventKind: "room_event" | "user_request" =
                request.inboundTurnKind === "room_event" ? "room_event" : "user_request";
              const actionContext = {
                channel,
                action: request.action as never,
                cfg,
                params: request.params,
                reply: request.reply,
                accountId,
                // Only the model's message tool mints an agent-runtime turn context, and
                // it resends proven-not-sent failures itself, so its gateway-owned plugin
                // delivery must not also stay replayable (#124279). Operator, CLI, and
                // external RPC clients carry none and keep recovery's replay (#100979).
                deliveryRetryOwner: trustedContext.runtimeAgentId ? ("caller" as const) : undefined,
                ...selectMessageActionRequesterIdentity(trustedContext),
                senderIsOwner: gatewayClientScopes.includes(ADMIN_SCOPE)
                  ? request.senderIsOwner === true
                  : false,
                conversationReadOrigin,
                sessionKey,
                sessionId: normalizeOptionalString(request.sessionId) ?? undefined,
                inboundEventKind,
                agentId,
                mediaAccess,
                mediaLocalRoots: mediaAccess.localRoots,
                toolContext: downstreamToolContext,
                dryRun: false,
                messageActionAuthorization: downstreamMessageActionAuthorization,
                gatewayClientScopes,
                assertDirectAdapterHandoff,
                onPlatformSendDispatch,
                // Model-authored sends own proven-not-sent retries; every scheduled
                // generic delivery must also stay inside its admitted job lifetime.
                skipQueue:
                  client?.internal?.agentRuntimeIdentity !== undefined &&
                  (request.action === "send" ||
                    Boolean(trustedContext.messageActionAuthorization?.scheduled)),
              };
              const settleTerminalDelivery = async (
                deliveredPayload: unknown,
                mirrorTranscript = true,
              ) => {
                try {
                  await reconcileTerminalSourceReplyDelivery({
                    deliveredPayload,
                    mirror: sourceReplyMirror,
                    receipt: terminalDeliveryReceipt,
                  });
                } catch (err) {
                  // The pre-send intent remains durable. Return the provider result so
                  // the model does not retry an external effect with an unknown outcome.
                  context.logGateway?.warn?.(
                    "Terminal source reply receipt reconciliation failed.",
                    {
                      error: formatForLog(err),
                      channel,
                      sessionKey,
                    },
                  );
                }
                if (mirrorTranscript) {
                  await scheduleDeliveredSourceReplyTranscriptMirror({
                    context,
                    mirror: {
                      ...sourceReplyMirror,
                      deliveredPayload,
                    },
                  });
                }
              };
              let payload: unknown;
              try {
                if (canonicalAction || messageAuthority.assertScheduledWriteCurrent) {
                  const { runMessageAction } =
                    await import("../../infra/outbound/message-action-runner.js");
                  const result = await runMessageAction({
                    ...actionContext,
                    gatewayOwnedDelivery: true,
                    ...(request.action === "send"
                      ? {
                          // This RPC owns source-reply receipts and their transcript mirror.
                          suppressTranscriptMirror: true,
                          actionOrigin: trustedContext.runtimeAgentId
                            ? ("message-tool" as const)
                            : undefined,
                        }
                      : {}),
                    params: {
                      ...request.params,
                      channel,
                      ...(accountId ? { accountId } : {}),
                      idempotencyKey: request.idempotencyKey,
                    },
                  });
                  payload = result.payload;
                } else {
                  const handled = await dispatchChannelMessageAction(actionContext);
                  if (handled) {
                    payload = extractToolPayload(handled);
                  } else {
                    await cancelTerminalSourceReplyDelivery(terminalDeliveryReceipt);
                    const error = errorShape(
                      ErrorCodes.INVALID_REQUEST,
                      `Message action ${request.action} not supported for channel ${channel}.`,
                    );
                    return createGatewayInflightResult({
                      context,
                      dedupeKey,
                      channel,
                      result: { ok: false, error },
                    });
                  }
                }
              } catch (err) {
                if (isChannelPartialDeliveryError(err)) {
                  // Accepted delivery evidence settles the terminal receipt, but it
                  // cannot prove which requested parts should enter the transcript.
                  await settleTerminalDelivery(err.deliveryResult, false);
                }
                throw err;
              }
              await settleTerminalDelivery(payload);
              // A downloaded artifact is not cacheable until the enclosing read
              // has accepted its provider/caller lifetime and resource identity.
              return request.action === "download-file"
                ? { ok: true, payload, meta: { channel } }
                : createGatewayInflightSuccess({ context, dedupeKey, payload, channel });
            },
            undefined,
            (result) => {
              if (request.action === "download-file" && result.ok) {
                createGatewayInflightSuccess({
                  context,
                  dedupeKey,
                  payload: result.payload,
                  channel,
                });
              }
            },
          );
          return completed;
        } catch (err) {
          if (isChannelPartialDeliveryError(err)) {
            return createGatewayInflightUnavailableFailure({ context, dedupeKey, channel, err });
          }
          if (!authorize()) {
            return createGatewayInflightAuthorityFailure({ context, dedupeKey, channel });
          }
          return createGatewayInflightUnavailableFailure({ context, dedupeKey, channel, err });
        }
      },
    });
  },
  send: async ({ params: request, respond, context, client, sessionMutationCommitGuard }) => {
    if (!assertValidParams(request, validateSendParams, "send", respond)) {
      return;
    }
    const to = normalizeOptionalString(request.to) ?? "";
    const message = request.message?.trim() ? request.message : "";
    const mediaUrl = normalizeOptionalString(request.mediaUrl);
    const mediaUrls = Array.isArray(request.mediaUrls)
      ? request.mediaUrls
          .map((entry) => normalizeOptionalString(entry))
          .filter((entry): entry is string => Boolean(entry))
      : undefined;
    const buffer = readStringValue(request.buffer);
    if (!message && !mediaUrl && (mediaUrls?.length ?? 0) === 0 && !buffer) {
      respond(
        false,
        undefined,
        errorShape(ErrorCodes.INVALID_REQUEST, "invalid send params: text or media is required"),
      );
      return;
    }
    const requestedAccountId = normalizeOptionalString(request.accountId);
    const replyToId = normalizeOptionalString(request.replyToId);
    const threadId = normalizeOptionalString(request.threadId);
    const messageActionAuthorization = resolveAgentRuntimeMessageActionAuthorization(client);
    const messageActionConfig = resolveAgentRuntimeMessageActionConfig(client);
    const messageAuthority = createMessageActionRuntimeAuthority({
      client,
      context,
      respond,
      sessionMutationCommitGuard,
      request: { action: "send", accountId: request.accountId, params: {} },
      authorization: messageActionAuthorization,
    });
    const agentRuntimeAuthority = messageAuthority.agentRuntimeAuthority;
    const hasAgentRuntimeAuthority = client?.internal?.agentRuntimeIdentity !== undefined;
    const commitAgentRuntimeAuthority = agentRuntimeAuthority.commitGuard;
    const onPlatformSendDispatch = commitAgentRuntimeAuthority
      ? async () => commitAgentRuntimeAuthority()
      : undefined;
    await withMessageOperationRoute({
      context,
      prefix: "send",
      idempotencyKey: request.idempotencyKey,
      respond,
      requestChannel: request.channel,
      bindingAccountIds: [request.accountId],
      routeAccountIds: (binding) => [requestedAccountId, binding?.reservedRoute?.accountId],
      conflictMessage: "send account selections do not match",
      authorize: agentRuntimeAuthority.hasActive,
      resolveChannel: async (requestChannel) => {
        const resolved = await resolveInternalDeliveryChannel(
          requestChannel,
          context,
          messageActionConfig,
        );
        if (resolved.kind !== "ready") {
          const result = resolved.result;
          respond(result.ok, result.payload, result.error, result.meta);
          return undefined;
        }
        const { cfg, channel } = resolved;
        const plugin = resolveOutboundChannelPlugin({ channel, cfg });
        if (!plugin) {
          respond(
            false,
            undefined,
            errorShape(ErrorCodes.INVALID_REQUEST, `unsupported channel: ${channel}`),
          );
          return undefined;
        }
        return { cfg, channel, plugin };
      },
      work: async ({ cfg, channel, accountId, idem, dedupeKey, authorize }) => {
        try {
          const resolvedTarget = resolveGatewayOutboundTarget({
            channel,
            to,
            cfg,
            accountId,
          });
          if (!resolvedTarget.ok) {
            return {
              ok: false,
              error: resolvedTarget.error,
              meta: { channel },
            };
          }
          const idLikeTarget = await withChannelReadAuthority(
            messageActionAuthorization?.scheduled ? commitAgentRuntimeAuthority : undefined,
            () =>
              maybeResolveIdLikeTarget({
                cfg,
                channel,
                input: resolvedTarget.to,
                accountId,
              }),
          );
          const deliveryTarget = idLikeTarget?.to ?? resolvedTarget.to;
          // Preserve opaque, case-sensitive peer IDs (e.g. Matrix room ids) on an
          // explicit session key instead of raw-lowercasing it (openclaw#75670).
          // Non-enrolled channels still canonicalize to lowercase via the registry.
          const providedSessionKey =
            normalizeSessionKeyPreservingOpaquePeerIds(request.sessionKey) || undefined;
          const explicitAgentId = normalizeOptionalString(request.agentId);
          const sessionOwner = providedSessionKey
            ? resolveRequestedSessionAgentId(cfg, providedSessionKey, explicitAgentId)
            : undefined;
          if (sessionOwner && !sessionOwner.ok) {
            return { ok: false, error: sessionOwner.error, meta: { channel } };
          }
          const sessionAgentId = sessionOwner?.agentId;
          const implicitAgent =
            !explicitAgentId && !sessionAgentId
              ? resolveRequestedSessionAgentId(cfg, "main")
              : undefined;
          if (implicitAgent && !implicitAgent.ok) {
            return { ok: false, error: implicitAgent.error, meta: { channel } };
          }
          const effectiveAgentId =
            explicitAgentId ?? sessionAgentId ?? (implicitAgent?.ok ? implicitAgent.agentId : null);
          if (!effectiveAgentId) {
            return {
              ok: false,
              error: errorShape(ErrorCodes.INVALID_REQUEST, "agent selection is required"),
              meta: { channel },
            };
          }
          const sendArgs: Record<string, unknown> = {
            mediaUrl,
            mediaUrls,
            buffer,
            filename: normalizeOptionalString(request.filename) ?? undefined,
            contentType: normalizeOptionalString(request.contentType) ?? undefined,
          };
          await hydrateAttachmentParamsForAction({
            cfg,
            channel,
            accountId,
            args: sendArgs,
            action: "send",
            mediaPolicy: resolveAttachmentMediaPolicy({
              mediaLocalRoots: getAgentScopedMediaLocalRoots(cfg, effectiveAgentId),
            }),
          });
          const hydratedMediaUrl = normalizeOptionalString(sendArgs.mediaUrl);
          const hydratedMediaUrls = Array.isArray(sendArgs.mediaUrls)
            ? sendArgs.mediaUrls
                .map((entry) => normalizeOptionalString(entry))
                .filter((entry): entry is string => Boolean(entry))
            : undefined;
          const outboundDeps = context.deps ? createOutboundSendDeps(context.deps) : undefined;
          const outboundPayloads = [
            {
              text: message,
              mediaUrl: hydratedMediaUrl,
              mediaUrls: hydratedMediaUrls,
              ...(request.asVoice === true ? { audioAsVoice: true } : {}),
            },
          ];
          const outboundPayloadPlan = createOutboundPayloadPlan(outboundPayloads);
          const mirrorProjection = projectOutboundPayloadPlanForMirror(outboundPayloadPlan);
          const mirrorText = mirrorProjection.text;
          const mirrorMediaUrls = mirrorProjection.mediaUrls;
          const derivedRoute = await resolveOutboundSessionRoute({
            cfg,
            channel,
            agentId: effectiveAgentId,
            accountId,
            target: deliveryTarget,
            currentSessionKey: providedSessionKey,
            resolvedTarget: idLikeTarget,
            replyToId,
            threadId,
          });
          const providedSessionBaseKey =
            parseThreadSessionSuffix(providedSessionKey).baseSessionKey ?? providedSessionKey;
          const shouldUseDerivedThreadSessionKey =
            resolveChannelThreadAddressing(channel) === "message" &&
            Boolean(providedSessionKey) &&
            Boolean(normalizeOptionalString(derivedRoute?.threadId)) &&
            normalizeOptionalLowercaseString(derivedRoute?.baseSessionKey) ===
              normalizeOptionalLowercaseString(providedSessionBaseKey) &&
            normalizeOptionalLowercaseString(derivedRoute?.sessionKey) !== providedSessionKey;
          // Message-scoped threads can refine an existing base session only after target lookup.
          const outboundRoute = derivedRoute
            ? providedSessionKey
              ? shouldUseDerivedThreadSessionKey
                ? {
                    ...derivedRoute,
                    baseSessionKey: derivedRoute.baseSessionKey ?? providedSessionKey,
                  }
                : {
                    ...derivedRoute,
                    sessionKey: providedSessionKey,
                    baseSessionKey: providedSessionKey,
                  }
              : derivedRoute
            : null;
          const outboundSessionKey = outboundRoute?.sessionKey ?? providedSessionKey;
          if (outboundSessionKey) {
            const agentAccessError = authorizeGatewaySessionCreation({
              cfg,
              client,
              agentId: effectiveAgentId,
            });
            if (agentAccessError) {
              return { ok: false, error: agentAccessError, meta: { channel } };
            }
          }
          if (outboundSessionKey && isAgentHarnessSessionKey(outboundSessionKey)) {
            const { canonicalKey, entry } = loadSessionEntry(outboundSessionKey);
            const missingHarnessSessionError = resolveMissingAgentHarnessSessionError(
              canonicalKey,
              entry,
            );
            if (missingHarnessSessionError) {
              return {
                ok: false,
                error: errorShape(ErrorCodes.INVALID_REQUEST, missingHarnessSessionError),
                meta: { channel },
              };
            }
          }
          // Durable route/session persistence commits only after platform
          // evidence: a failed send must not rebind the folded main session's
          // delivery route. Once-only across multi-payload results, and before
          // the in-delivery transcript mirror so first contacts have a row.
          let outboundRoutePersisted = false;
          const commitOutboundSessionRoute = async () => {
            if (outboundRoutePersisted || !outboundRoute) {
              return;
            }
            outboundRoutePersisted = true;
            await ensureOutboundSessionEntry({
              cfg,
              channel,
              accountId,
              route: outboundRoute,
              creation: resolveSandboxedSessionCreation(client, cfg),
              sourceSessionKey: client?.internal?.agentRuntimeIdentity?.sessionKey,
            });
          };
          const outboundSession = buildOutboundSessionContext({
            cfg,
            agentId: effectiveAgentId,
            sessionKey: outboundSessionKey,
            conversationType: outboundRoute?.chatType,
          });
          // Target, attachment, route, and session preparation may all yield.
          // The durable provider handoff is the final authority commit point.
          if (!authorize()) {
            return createGatewayInflightAuthorityFailure({ context, dedupeKey, channel });
          }
          const send = await sendDurableMessageBatchCore({
            cfg,
            channel,
            to: deliveryTarget,
            accountId,
            payloads: outboundPayloads,
            replyToId: replyToId ?? null,
            session: outboundSession,
            gifPlayback: request.gifPlayback,
            forceDocument: request.forceDocument,
            threadId: outboundRoute?.threadId ?? threadId ?? null,
            deps: outboundDeps,
            gatewayClientScopes: client?.connect?.scopes ?? [],
            silent: request.silent,
            formatting: request.parseMode ? { parseMode: request.parseMode } : undefined,
            onDeliveryResult: commitOutboundSessionRoute,
            // Runtime-bound sends cannot outlive their operational run. Keep
            // recovery from replaying them after the live authority closes.
            onPlatformSendDispatch,
            assertDirectAdapterHandoff: commitAgentRuntimeAuthority,
            skipQueue: hasAgentRuntimeAuthority,
            mirror: outboundSessionKey
              ? {
                  sessionKey: outboundSessionKey,
                  agentId: effectiveAgentId,
                  text: mirrorText || message,
                  mediaUrls: mirrorMediaUrls.length > 0 ? mirrorMediaUrls : undefined,
                  idempotencyKey: idem,
                }
              : undefined,
          });
          // Safety net for adapters whose results carry no platform identity:
          // any partially or fully sent batch still binds the route.
          if (send.status === "sent" || send.status === "partial_failed") {
            await commitOutboundSessionRoute();
          }
          if (send.status === "failed") {
            throw send.error;
          }
          if (send.status === "partial_failed") {
            throw createChannelPartialDeliveryError(send.error, {
              messageIds: send.results.map((result) => result.messageId),
              receipt: send.receipt,
              visibleReplySent: true,
            });
          }
          const results = send.status === "sent" ? send.results : [];

          const result = results.at(-1);
          if (!result) {
            throw new Error("No delivery result");
          }
          const payload = buildGatewayDeliveryPayload({ runId: idem, channel, result });
          return createGatewayInflightSuccess({
            context,
            dedupeKey,
            payload,
            channel,
          });
        } catch (err) {
          if (isChannelPartialDeliveryError(err)) {
            return createGatewayInflightUnavailableFailure({ context, dedupeKey, channel, err });
          }
          if (hasAgentRuntimeAuthority && !agentRuntimeAuthority.hasActive()) {
            return createGatewayInflightAuthorityFailure({ context, dedupeKey, channel });
          }
          return createGatewayInflightUnavailableFailure({ context, dedupeKey, channel, err });
        }
      },
    });
  },
  poll: async ({ params: request, respond, context, client, sessionMutationCommitGuard }) => {
    if (!assertValidParams(request, validatePollParams, "poll", respond)) {
      return;
    }
    const messageAuthority = createMessageActionRuntimeAuthority({
      client,
      context,
      respond,
      sessionMutationCommitGuard,
      request: { action: "poll", accountId: request.accountId, params: {} },
      authorization: resolveAgentRuntimeMessageActionAuthorization(client),
    });
    const messageActionConfig = resolveAgentRuntimeMessageActionConfig(client);
    const agentRuntimeAuthority = messageAuthority.agentRuntimeAuthority;
    const hasAgentRuntimeAuthority = client?.internal?.agentRuntimeIdentity !== undefined;
    const commitAgentRuntimeAuthority = agentRuntimeAuthority.commitGuard;
    const onPlatformSendDispatch = commitAgentRuntimeAuthority
      ? async () => commitAgentRuntimeAuthority()
      : undefined;
    await withMessageOperationRoute({
      context,
      prefix: "poll",
      idempotencyKey: request.idempotencyKey,
      respond,
      requestChannel: request.channel,
      bindingAccountIds: [request.accountId],
      routeAccountIds: (binding) => [request.accountId, binding?.reservedRoute?.accountId],
      conflictMessage: "poll account selections do not match",
      authorize: agentRuntimeAuthority.hasActive,
      resolveChannel: async (requestChannel) => {
        const resolved = await resolveRequestedChannel({
          requestChannel,
          unsupportedMessage: (input) => `unsupported poll channel: ${input}`,
          context,
          config: messageActionConfig,
        });
        if ("error" in resolved) {
          respond(false, undefined, resolved.error);
          return undefined;
        }
        const { cfg, channel } = resolved;
        const plugin = resolveOutboundChannelPlugin({ channel, cfg });
        const outbound = plugin?.outbound;
        if (
          typeof request.durationSeconds === "number" &&
          outbound?.supportsPollDurationSeconds !== true
        ) {
          // Duration support is channel-specific; reject before normalizing to avoid silent truncation.
          respond(
            false,
            undefined,
            errorShape(
              ErrorCodes.INVALID_REQUEST,
              `durationSeconds is not supported for ${channel} polls`,
            ),
          );
          return undefined;
        }
        if (typeof request.isAnonymous === "boolean" && outbound?.supportsAnonymousPolls !== true) {
          respond(
            false,
            undefined,
            errorShape(
              ErrorCodes.INVALID_REQUEST,
              `isAnonymous is not supported for ${channel} polls`,
            ),
          );
          return undefined;
        }
        if (!plugin || !outbound?.sendPoll) {
          respond(
            false,
            undefined,
            errorShape(ErrorCodes.INVALID_REQUEST, `unsupported poll channel: ${channel}`),
          );
          return undefined;
        }
        return { cfg, channel, plugin, outbound, sendPoll: outbound.sendPoll };
      },
      work: async ({ cfg, channel, accountId, idem, dedupeKey, authorize, outbound, sendPoll }) => {
        const poll = {
          question: request.question,
          options: request.options,
          maxSelections: request.maxSelections,
          durationSeconds: request.durationSeconds,
          durationHours: request.durationHours,
        };
        const threadId = normalizeOptionalString(request.threadId);
        try {
          const resolvedTarget = resolveGatewayOutboundTarget({
            channel,
            to: request.to.trim(),
            cfg,
            accountId,
          });
          if (!resolvedTarget.ok) {
            return { ok: false, error: resolvedTarget.error };
          }
          const normalized = outbound.pollMaxOptions
            ? normalizePollInput(poll, { maxOptions: outbound.pollMaxOptions })
            : normalizePollInput(poll);
          if (!authorize()) {
            return createGatewayInflightAuthorityFailure({ context, dedupeKey, channel });
          }
          const result = await sendPoll({
            cfg,
            to: resolvedTarget.to,
            poll: normalized,
            accountId,
            threadId,
            silent: request.silent,
            isAnonymous: request.isAnonymous,
            gatewayClientScopes: client?.connect?.scopes ?? [],
            onPlatformSendDispatch,
            assertDirectAdapterHandoff: commitAgentRuntimeAuthority,
          });
          const payload = buildGatewayDeliveryPayload({ runId: idem, channel, result });
          return createGatewayInflightSuccess({ context, dedupeKey, payload, channel });
        } catch (err) {
          if (isChannelPartialDeliveryError(err)) {
            return createGatewayInflightUnavailableFailure({ context, dedupeKey, channel, err });
          }
          if (hasAgentRuntimeAuthority && !agentRuntimeAuthority.hasActive()) {
            return createGatewayInflightAuthorityFailure({ context, dedupeKey, channel });
          }
          return createGatewayInflightUnavailableFailure({ context, dedupeKey, channel, err });
        }
      },
    });
  },
};
/* oxlint-disable max-lines -- TODO: split this grandfathered oversized file. */
