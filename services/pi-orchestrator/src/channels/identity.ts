import type { ChannelIdentity } from "./contracts.js";
import type { TrustedRequestPrincipal } from "../application.js";

export class ChannelIdentityError extends Error {}

// Channel bindings are authoritative only in Forge. Never consult a local identity file.
export function identityFromTrustedBinding(
  principal: TrustedRequestPrincipal, channel: string, externalUserId: string,
): ChannelIdentity {
  if (channel !== "web" && channel !== "feishu" && channel !== "dingtalk") {
    throw new ChannelIdentityError("Invalid channel");
  }
  const webSession = channel === "web" && principal.principal_type === "human" &&
    principal.credential_id !== null && externalUserId === principal.principal_id;
  const channelBinding = channel !== "web" && principal.channel_binding_id === `${channel}:${externalUserId}`;
  if (!externalUserId || (!webSession && !channelBinding)) {
    throw new ChannelIdentityError("Channel identity is not authorized");
  }
  return { org_id: principal.organization_id, team_id: principal.workspace_id, user_id: principal.principal_id };
}
