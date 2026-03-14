import type { SecureServerOptions } from 'node:http2';
import { X509Certificate } from 'node:crypto';

export type cluster_tls_server_config_t = {
  key_pem: string;
  cert_pem: string;
  ca_pem_list: string[];
  request_client_cert?: boolean;
  reject_unauthorized_client_cert?: boolean;
  min_tls_version?: 'TLSv1.2' | 'TLSv1.3';
};

export type cluster_tls_client_config_t = {
  ca_pem_list: string[];
  client_cert_pem: string;
  client_key_pem: string;
  reject_unauthorized?: boolean;
  servername?: string;
  min_tls_version?: 'TLSv1.2' | 'TLSv1.3';
};

function EnsureNonEmptyString(params: {
  value: unknown;
  field_name: string;
}): string {
  const { value, field_name } = params;

  if (typeof value !== 'string' || value.length === 0) {
    throw new Error(`${field_name} must be a non-empty string.`);
  }

  return value;
}

function EnsurePemList(params: {
  value: unknown;
  field_name: string;
}): string[] {
  const { value, field_name } = params;

  if (!Array.isArray(value) || value.length === 0) {
    throw new Error(`${field_name} must be a non-empty string array.`);
  }

  const normalized_pem_list: string[] = [];
  for (let index = 0; index < value.length; index += 1) {
    const pem_value = value[index];
    if (typeof pem_value !== 'string' || pem_value.length === 0) {
      throw new Error(`${field_name}[${index}] must be a non-empty string.`);
    }
    normalized_pem_list.push(pem_value);
  }

  return normalized_pem_list;
}

function ValidateCertificateTimeWindow(params: {
  cert_pem: string;
  field_name: string;
  now_unix_ms?: number;
}): void {
  const { cert_pem, field_name } = params;
  const now_unix_ms = params.now_unix_ms ?? Date.now();

  let parsed_certificate: X509Certificate;
  try {
    parsed_certificate = new X509Certificate(cert_pem);
  } catch (error) {
    throw new Error(
      `${field_name} is not a valid PEM certificate: ${
        error instanceof Error ? error.message : String(error)
      }`
    );
  }

  const valid_from_unix_ms = Date.parse(parsed_certificate.validFrom);
  const valid_to_unix_ms = Date.parse(parsed_certificate.validTo);

  if (!Number.isFinite(valid_from_unix_ms) || !Number.isFinite(valid_to_unix_ms)) {
    throw new Error(`${field_name} has an invalid certificate validity window.`);
  }

  if (now_unix_ms < valid_from_unix_ms) {
    throw new Error(
      `${field_name} certificate is not active yet (valid_from=${parsed_certificate.validFrom}).`
    );
  }

  if (now_unix_ms >= valid_to_unix_ms) {
    throw new Error(
      `${field_name} certificate is expired (valid_to=${parsed_certificate.validTo}).`
    );
  }
}

function ValidateMinTlsVersion(params: {
  min_tls_version: unknown;
  field_name: string;
}): 'TLSv1.2' | 'TLSv1.3' {
  const { min_tls_version, field_name } = params;

  if (typeof min_tls_version === 'undefined') {
    return 'TLSv1.2';
  }

  if (min_tls_version !== 'TLSv1.2' && min_tls_version !== 'TLSv1.3') {
    throw new Error(`${field_name} must be TLSv1.2 or TLSv1.3.`);
  }

  return min_tls_version;
}

export function ValidateTlsServerConfig(params: {
  tls_server_config: cluster_tls_server_config_t | undefined;
  now_unix_ms?: number;
}): cluster_tls_server_config_t {
  const tls_server_config = params.tls_server_config;

  if (!tls_server_config) {
    throw new Error('security.tls server config is required for secure-only transport.');
  }

  const key_pem = EnsureNonEmptyString({
    value: tls_server_config.key_pem,
    field_name: 'security.tls.key_pem'
  });

  const cert_pem = EnsureNonEmptyString({
    value: tls_server_config.cert_pem,
    field_name: 'security.tls.cert_pem'
  });

  const ca_pem_list = EnsurePemList({
    value: tls_server_config.ca_pem_list,
    field_name: 'security.tls.ca_pem_list'
  });

  ValidateCertificateTimeWindow({
    cert_pem,
    field_name: 'security.tls.cert_pem',
    now_unix_ms: params.now_unix_ms
  });

  for (let index = 0; index < ca_pem_list.length; index += 1) {
    ValidateCertificateTimeWindow({
      cert_pem: ca_pem_list[index],
      field_name: `security.tls.ca_pem_list[${index}]`,
      now_unix_ms: params.now_unix_ms
    });
  }

  return {
    key_pem,
    cert_pem,
    ca_pem_list,
    request_client_cert: tls_server_config.request_client_cert ?? true,
    reject_unauthorized_client_cert:
      tls_server_config.reject_unauthorized_client_cert ?? true,
    min_tls_version: ValidateMinTlsVersion({
      min_tls_version: tls_server_config.min_tls_version,
      field_name: 'security.tls.min_tls_version'
    })
  };
}

export function ValidateTlsClientConfig(params: {
  tls_client_config: cluster_tls_client_config_t | undefined;
  now_unix_ms?: number;
  field_prefix?: string;
}): cluster_tls_client_config_t {
  const tls_client_config = params.tls_client_config;
  const field_prefix = params.field_prefix ?? 'transport_security';

  if (!tls_client_config) {
    throw new Error(`${field_prefix} is required for secure-only transport.`);
  }

  const ca_pem_list = EnsurePemList({
    value: tls_client_config.ca_pem_list,
    field_name: `${field_prefix}.ca_pem_list`
  });

  const client_cert_pem = EnsureNonEmptyString({
    value: tls_client_config.client_cert_pem,
    field_name: `${field_prefix}.client_cert_pem`
  });

  const client_key_pem = EnsureNonEmptyString({
    value: tls_client_config.client_key_pem,
    field_name: `${field_prefix}.client_key_pem`
  });

  ValidateCertificateTimeWindow({
    cert_pem: client_cert_pem,
    field_name: `${field_prefix}.client_cert_pem`,
    now_unix_ms: params.now_unix_ms
  });

  for (let index = 0; index < ca_pem_list.length; index += 1) {
    ValidateCertificateTimeWindow({
      cert_pem: ca_pem_list[index],
      field_name: `${field_prefix}.ca_pem_list[${index}]`,
      now_unix_ms: params.now_unix_ms
    });
  }

  if (
    typeof tls_client_config.servername !== 'undefined' &&
    (typeof tls_client_config.servername !== 'string' ||
      tls_client_config.servername.length === 0)
  ) {
    throw new Error(`${field_prefix}.servername must be a non-empty string when provided.`);
  }

  return {
    ca_pem_list,
    client_cert_pem,
    client_key_pem,
    reject_unauthorized: tls_client_config.reject_unauthorized ?? true,
    servername: tls_client_config.servername,
    min_tls_version: ValidateMinTlsVersion({
      min_tls_version: tls_client_config.min_tls_version,
      field_name: `${field_prefix}.min_tls_version`
    })
  };
}

export function BuildTlsServerOptions(params: {
  tls_server_config: cluster_tls_server_config_t;
}): SecureServerOptions {
  const { tls_server_config } = params;

  return {
    key: tls_server_config.key_pem,
    cert: tls_server_config.cert_pem,
    ca: tls_server_config.ca_pem_list,
    allowHTTP1: false,
    requestCert: tls_server_config.request_client_cert ?? true,
    rejectUnauthorized: tls_server_config.reject_unauthorized_client_cert ?? true,
    minVersion: tls_server_config.min_tls_version ?? 'TLSv1.2'
  };
}

export function BuildTlsClientConnectOptions(params: {
  tls_client_config: cluster_tls_client_config_t;
}): Record<string, unknown> {
  const { tls_client_config } = params;

  const connect_options: Record<string, unknown> = {
    ca: tls_client_config.ca_pem_list,
    cert: tls_client_config.client_cert_pem,
    key: tls_client_config.client_key_pem,
    rejectUnauthorized: tls_client_config.reject_unauthorized ?? true,
    minVersion: tls_client_config.min_tls_version ?? 'TLSv1.2'
  };

  if (typeof tls_client_config.servername === 'string') {
    connect_options.servername = tls_client_config.servername;
  }

  return connect_options;
}

export function BuildHttpsAuthority(params: {
  host: string;
  port: number;
}): string {
  return `https://${params.host}:${params.port}`;
}
