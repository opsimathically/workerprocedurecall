import {
  createHmac,
  createVerify,
  timingSafeEqual
} from 'node:crypto';
import type { IncomingHttpHeaders } from 'node:http2';

export type cluster_transport_jwt_algorithm_t = 'HS256' | 'RS256';

export type cluster_transport_jwt_key_t = {
  kid: string;
  algorithm: cluster_transport_jwt_algorithm_t;
  secret?: string;
  public_key_pem?: string;
  not_before_unix_ms?: number;
  expires_unix_ms?: number;
};

export type cluster_transport_token_validation_config_t = {
  enabled: boolean;
  token_header_name?: string;
  token_prefix?: string;
  expected_issuer?: string;
  expected_audience?: string | string[];
  clock_skew_ms?: number;
  key_by_kid: Record<string, cluster_transport_jwt_key_t>;
};

export type cluster_transport_trusted_identity_t = {
  subject: string;
  tenant_id: string;
  scopes: string[];
  signed_claims: string;
  environment?: string;
  token_id?: string;
  issuer?: string;
  audience?: string | string[];
  token_expires_unix_ms?: number;
};

export type cluster_transport_token_validation_result_t =
  | {
      ok: true;
      identity: cluster_transport_trusted_identity_t;
      details: {
        token_header_name: string;
      };
    }
  | {
      ok: false;
      message: string;
      details: Record<string, unknown>;
    };

export type cluster_transport_replay_protection_config_t = {
  enabled: boolean;
  window_ms: number;
  require_token_id_for_privileged_requests: boolean;
};

type jwt_header_t = {
  alg?: string;
  typ?: string;
  kid?: string;
};

type jwt_payload_t = {
  sub?: string;
  tenant_id?: string;
  scope?: string;
  scopes?: string[];
  capability_claims?: string[];
  environment?: string;
  env?: string;
  iss?: string;
  aud?: string | string[];
  exp?: number;
  nbf?: number;
  iat?: number;
  jti?: string;
};

function IsRecordObject(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function Base64UrlDecode(params: { value: string }): Buffer {
  const { value } = params;
  const normalized_value = value.replace(/-/g, '+').replace(/_/g, '/');
  const padded_value = normalized_value + '='.repeat((4 - (normalized_value.length % 4)) % 4);
  return Buffer.from(padded_value, 'base64');
}

function ParseJsonObject(params: { value: Buffer }): Record<string, unknown> {
  const { value } = params;
  const text_value = value.toString('utf8');
  const parsed = JSON.parse(text_value) as unknown;

  if (!IsRecordObject(parsed)) {
    throw new Error('JWT segment is not a JSON object.');
  }

  return parsed;
}

function ConstantTimeBufferEqual(params: {
  left_value: Buffer;
  right_value: Buffer;
}): boolean {
  const { left_value, right_value } = params;

  if (left_value.length !== right_value.length) {
    return false;
  }

  return timingSafeEqual(left_value, right_value);
}

function NormalizeScopeList(params: {
  payload: jwt_payload_t;
}): string[] {
  const { payload } = params;

  if (Array.isArray(payload.scopes)) {
    return payload.scopes.filter((scope): scope is string => {
      return typeof scope === 'string' && scope.length > 0;
    });
  }

  if (Array.isArray(payload.capability_claims)) {
    return payload.capability_claims.filter((scope): scope is string => {
      return typeof scope === 'string' && scope.length > 0;
    });
  }

  if (typeof payload.scope === 'string' && payload.scope.length > 0) {
    return payload.scope
      .split(' ')
      .map((scope) => scope.trim())
      .filter((scope) => scope.length > 0);
  }

  return [];
}

function NormalizeAudienceList(params: {
  audience: string | string[] | undefined;
}): string[] {
  const { audience } = params;

  if (typeof audience === 'string') {
    return [audience];
  }

  if (Array.isArray(audience)) {
    return audience.filter((value): value is string => {
      return typeof value === 'string' && value.length > 0;
    });
  }

  return [];
}

function ReadHeaderValue(params: {
  headers: IncomingHttpHeaders;
  header_name: string;
}): string | undefined {
  const { headers, header_name } = params;
  const value = headers[header_name.toLowerCase()];

  if (typeof value === 'string') {
    return value;
  }

  if (Array.isArray(value) && typeof value[0] === 'string') {
    return value[0];
  }

  return undefined;
}

export class ClusterTransportTokenValidator {
  private token_validation_config: cluster_transport_token_validation_config_t = {
    enabled: false,
    token_header_name: 'authorization',
    token_prefix: 'Bearer ',
    clock_skew_ms: 5_000,
    key_by_kid: {}
  };

  constructor(params: {
    token_validation_config?: cluster_transport_token_validation_config_t;
  } = {}) {
    if (params.token_validation_config) {
      this.setTokenValidationConfig({
        token_validation_config: params.token_validation_config
      });
    }
  }

  setTokenValidationConfig(params: {
    token_validation_config: cluster_transport_token_validation_config_t;
  }): void {
    const { token_validation_config } = params;

    this.token_validation_config = {
      enabled: token_validation_config.enabled,
      token_header_name: token_validation_config.token_header_name ?? 'authorization',
      token_prefix: token_validation_config.token_prefix ?? 'Bearer ',
      expected_issuer: token_validation_config.expected_issuer,
      expected_audience: token_validation_config.expected_audience,
      clock_skew_ms: token_validation_config.clock_skew_ms ?? 5_000,
      key_by_kid: { ...token_validation_config.key_by_kid }
    };
  }

  setTokenVerificationKeySet(params: {
    key_by_kid: Record<string, cluster_transport_jwt_key_t>;
  }): void {
    this.token_validation_config = {
      ...this.token_validation_config,
      key_by_kid: { ...params.key_by_kid }
    };
  }

  upsertTokenVerificationKey(params: {
    key: cluster_transport_jwt_key_t;
  }): void {
    const { key } = params;

    this.token_validation_config = {
      ...this.token_validation_config,
      key_by_kid: {
        ...this.token_validation_config.key_by_kid,
        [key.kid]: { ...key }
      }
    };
  }

  getTokenValidationConfig(): cluster_transport_token_validation_config_t {
    return {
      ...this.token_validation_config,
      key_by_kid: { ...this.token_validation_config.key_by_kid }
    };
  }

  validateRequestHeaders(params: {
    headers: IncomingHttpHeaders;
    now_unix_ms?: number;
  }): cluster_transport_token_validation_result_t {
    const { headers, now_unix_ms = Date.now() } = params;

    if (!this.token_validation_config.enabled) {
      return {
        ok: false,
        message: 'Token validation is disabled.',
        details: {
          reason: 'token_validation_disabled'
        }
      };
    }

    const token_header_name = (this.token_validation_config.token_header_name ?? 'authorization').toLowerCase();

    const header_value = ReadHeaderValue({
      headers,
      header_name: token_header_name
    });

    if (typeof header_value !== 'string' || header_value.length === 0) {
      return {
        ok: false,
        message: 'Authentication token header is missing.',
        details: {
          reason: 'missing_token_header',
          token_header_name
        }
      };
    }

    const token_prefix = this.token_validation_config.token_prefix ?? 'Bearer ';

    if (token_prefix.length > 0 && !header_value.startsWith(token_prefix)) {
      return {
        ok: false,
        message: 'Authentication token header prefix is invalid.',
        details: {
          reason: 'invalid_token_prefix',
          token_header_name
        }
      };
    }

    const token = token_prefix.length > 0 ? header_value.slice(token_prefix.length) : header_value;

    return this.validateToken({
      token,
      now_unix_ms,
      token_header_name
    });
  }

  private validateToken(params: {
    token: string;
    now_unix_ms: number;
    token_header_name: string;
  }): cluster_transport_token_validation_result_t {
    const { token, now_unix_ms, token_header_name } = params;

    const token_segment_list = token.split('.');
    if (token_segment_list.length !== 3) {
      return {
        ok: false,
        message: 'Authentication token format is invalid.',
        details: {
          reason: 'invalid_token_format'
        }
      };
    }

    const [encoded_header, encoded_payload, encoded_signature] = token_segment_list;

    let header: jwt_header_t;
    let payload: jwt_payload_t;

    try {
      header = ParseJsonObject({
        value: Base64UrlDecode({ value: encoded_header })
      }) as jwt_header_t;

      payload = ParseJsonObject({
        value: Base64UrlDecode({ value: encoded_payload })
      }) as jwt_payload_t;
    } catch {
      return {
        ok: false,
        message: 'Authentication token payload is invalid.',
        details: {
          reason: 'invalid_token_json'
        }
      };
    }

    const algorithm = header.alg;
    const key_id = header.kid;

    if ((algorithm !== 'HS256' && algorithm !== 'RS256') || typeof key_id !== 'string') {
      return {
        ok: false,
        message: 'Authentication token header is invalid.',
        details: {
          reason: 'invalid_token_header'
        }
      };
    }

    const key = this.token_validation_config.key_by_kid[key_id];
    if (!key || key.algorithm !== algorithm) {
      return {
        ok: false,
        message: 'Authentication key is unavailable.',
        details: {
          reason: 'missing_or_mismatched_key'
        }
      };
    }

    if (
      typeof key.not_before_unix_ms === 'number' &&
      now_unix_ms + (this.token_validation_config.clock_skew_ms ?? 0) < key.not_before_unix_ms
    ) {
      return {
        ok: false,
        message: 'Authentication key is not active yet.',
        details: {
          reason: 'key_not_active'
        }
      };
    }

    if (
      typeof key.expires_unix_ms === 'number' &&
      now_unix_ms - (this.token_validation_config.clock_skew_ms ?? 0) >= key.expires_unix_ms
    ) {
      return {
        ok: false,
        message: 'Authentication key is expired.',
        details: {
          reason: 'key_expired'
        }
      };
    }

    const signing_input = `${encoded_header}.${encoded_payload}`;
    const signature_buffer = Base64UrlDecode({ value: encoded_signature });

    const signature_verified = this.verifyTokenSignature({
      algorithm,
      key,
      signing_input,
      signature_buffer
    });

    if (!signature_verified) {
      return {
        ok: false,
        message: 'Authentication token signature is invalid.',
        details: {
          reason: 'invalid_signature'
        }
      };
    }

    const clock_skew_ms = this.token_validation_config.clock_skew_ms ?? 0;

    if (typeof payload.nbf === 'number') {
      const not_before_unix_ms = payload.nbf * 1000;
      if (now_unix_ms + clock_skew_ms < not_before_unix_ms) {
        return {
          ok: false,
          message: 'Authentication token is not active yet.',
          details: {
            reason: 'token_not_before'
          }
        };
      }
    }

    if (typeof payload.exp === 'number') {
      const expires_unix_ms = payload.exp * 1000;
      if (now_unix_ms - clock_skew_ms >= expires_unix_ms) {
        return {
          ok: false,
          message: 'Authentication token is expired.',
          details: {
            reason: 'token_expired'
          }
        };
      }
    } else {
      return {
        ok: false,
        message: 'Authentication token must include exp.',
        details: {
          reason: 'token_exp_missing'
        }
      };
    }

    if (
      typeof this.token_validation_config.expected_issuer === 'string' &&
      payload.iss !== this.token_validation_config.expected_issuer
    ) {
      return {
        ok: false,
        message: 'Authentication token issuer is invalid.',
        details: {
          reason: 'issuer_mismatch'
        }
      };
    }

    const expected_audience_list = NormalizeAudienceList({
      audience: this.token_validation_config.expected_audience
    });

    if (expected_audience_list.length > 0) {
      const actual_audience_list = NormalizeAudienceList({
        audience: payload.aud
      });

      const has_matching_audience = expected_audience_list.some((expected_audience): boolean => {
        return actual_audience_list.includes(expected_audience);
      });

      if (!has_matching_audience) {
        return {
          ok: false,
          message: 'Authentication token audience is invalid.',
          details: {
            reason: 'audience_mismatch'
          }
        };
      }
    }

    if (typeof payload.sub !== 'string' || payload.sub.length === 0) {
      return {
        ok: false,
        message: 'Authentication token subject is invalid.',
        details: {
          reason: 'subject_missing'
        }
      };
    }

    if (typeof payload.tenant_id !== 'string' || payload.tenant_id.length === 0) {
      return {
        ok: false,
        message: 'Authentication token tenant_id is invalid.',
        details: {
          reason: 'tenant_missing'
        }
      };
    }

    const scope_list = NormalizeScopeList({
      payload
    });

    return {
      ok: true,
      identity: {
        subject: payload.sub,
        tenant_id: payload.tenant_id,
        scopes: scope_list,
        signed_claims: token,
        environment:
          typeof payload.environment === 'string'
            ? payload.environment
            : typeof payload.env === 'string'
              ? payload.env
              : undefined,
        token_id: typeof payload.jti === 'string' ? payload.jti : undefined,
        issuer: typeof payload.iss === 'string' ? payload.iss : undefined,
        audience: payload.aud,
        token_expires_unix_ms: typeof payload.exp === 'number' ? payload.exp * 1000 : undefined
      },
      details: {
        token_header_name
      }
    };
  }

  private verifyTokenSignature(params: {
    algorithm: cluster_transport_jwt_algorithm_t;
    key: cluster_transport_jwt_key_t;
    signing_input: string;
    signature_buffer: Buffer;
  }): boolean {
    const { algorithm, key, signing_input, signature_buffer } = params;

    if (algorithm === 'HS256') {
      if (typeof key.secret !== 'string' || key.secret.length === 0) {
        return false;
      }

      const expected_signature = createHmac('sha256', key.secret)
        .update(signing_input)
        .digest();

      return ConstantTimeBufferEqual({
        left_value: expected_signature,
        right_value: signature_buffer
      });
    }

    if (typeof key.public_key_pem !== 'string' || key.public_key_pem.length === 0) {
      return false;
    }

    const verifier = createVerify('RSA-SHA256');
    verifier.update(signing_input);
    verifier.end();

    return verifier.verify(key.public_key_pem, signature_buffer);
  }
}

export class ClusterTransportReplayProtection {
  private replay_protection_config: cluster_transport_replay_protection_config_t = {
    enabled: false,
    window_ms: 120_000,
    require_token_id_for_privileged_requests: true
  };

  private readonly replay_record_expires_unix_ms_by_key = new Map<string, number>();

  setReplayProtectionConfig(params: {
    replay_protection_config: cluster_transport_replay_protection_config_t;
  }): void {
    this.replay_protection_config = {
      enabled: params.replay_protection_config.enabled,
      window_ms: params.replay_protection_config.window_ms,
      require_token_id_for_privileged_requests:
        params.replay_protection_config.require_token_id_for_privileged_requests
    };
  }

  getReplayProtectionConfig(): cluster_transport_replay_protection_config_t {
    return {
      ...this.replay_protection_config
    };
  }

  checkAndRecordReplay(params: {
    replay_key: string;
    now_unix_ms?: number;
  }): {
    ok: boolean;
    details: Record<string, unknown>;
  } {
    const { replay_key, now_unix_ms = Date.now() } = params;

    this.removeExpiredReplayRecords({
      now_unix_ms
    });

    if (!this.replay_protection_config.enabled) {
      return {
        ok: true,
        details: {
          replay_protection_enabled: false
        }
      };
    }

    const existing_expires_unix_ms = this.replay_record_expires_unix_ms_by_key.get(
      replay_key
    );

    if (typeof existing_expires_unix_ms === 'number' && existing_expires_unix_ms > now_unix_ms) {
      return {
        ok: false,
        details: {
          reason: 'replay_detected'
        }
      };
    }

    this.replay_record_expires_unix_ms_by_key.set(
      replay_key,
      now_unix_ms + this.replay_protection_config.window_ms
    );

    return {
      ok: true,
      details: {
        replay_protection_enabled: true,
        replay_window_ms: this.replay_protection_config.window_ms
      }
    };
  }

  private removeExpiredReplayRecords(params: { now_unix_ms: number }): void {
    const { now_unix_ms } = params;

    for (const [replay_key, expires_unix_ms] of this.replay_record_expires_unix_ms_by_key) {
      if (expires_unix_ms <= now_unix_ms) {
        this.replay_record_expires_unix_ms_by_key.delete(replay_key);
      }
    }
  }
}
