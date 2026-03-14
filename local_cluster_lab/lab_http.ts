import type { IncomingMessage } from 'node:http';
import https from 'node:https';

import { BuildLabTlsClientConfig } from './lab_security';

function ReadResponseBody(params: { response: IncomingMessage }): Promise<string> {
  return new Promise<string>((resolve, reject): void => {
    const chunk_list: Buffer[] = [];

    params.response.on('data', (chunk): void => {
      if (typeof chunk === 'string') {
        chunk_list.push(Buffer.from(chunk));
        return;
      }

      chunk_list.push(chunk as Buffer);
    });

    params.response.on('end', (): void => {
      resolve(Buffer.concat(chunk_list).toString('utf8'));
    });

    params.response.on('error', (error): void => {
      reject(error);
    });
  });
}

export async function HttpJsonRequest(params: {
  method: 'GET' | 'POST';
  host: string;
  port: number;
  path: string;
  timeout_ms: number;
  body?: Record<string, unknown>;
}): Promise<unknown> {
  const payload_text =
    params.body === undefined ? undefined : JSON.stringify(params.body);
  const tls_client_config = BuildLabTlsClientConfig();

  return await new Promise<unknown>((resolve, reject): void => {
    const request = https.request(
      {
        method: params.method,
        host: params.host,
        port: params.port,
        path: params.path,
        timeout: params.timeout_ms,
        ca: tls_client_config.ca_pem_list,
        cert: tls_client_config.client_cert_pem,
        key: tls_client_config.client_key_pem,
        rejectUnauthorized: tls_client_config.reject_unauthorized ?? true,
        servername: tls_client_config.servername,
        minVersion: tls_client_config.min_tls_version,
        headers: payload_text
          ? {
              'content-type': 'application/json',
              'content-length': Buffer.byteLength(payload_text)
            }
          : undefined
      },
      async (response): Promise<void> => {
        try {
          const response_body = await ReadResponseBody({
            response
          });
          const parsed_body = response_body.length > 0 ? JSON.parse(response_body) : {};

          if ((response.statusCode ?? 500) >= 400) {
            reject(
              new Error(
                `HTTP ${response.statusCode}: ${JSON.stringify(parsed_body)}`
              )
            );
            return;
          }

          resolve(parsed_body);
        } catch (error) {
          reject(error);
        }
      }
    );

    request.on('error', (error): void => {
      reject(error);
    });

    request.on('timeout', (): void => {
      request.destroy(new Error('HTTP request timed out.'));
    });

    if (payload_text) {
      request.write(payload_text);
    }

    request.end();
  });
}

export async function WaitForCondition(params: {
  timeout_ms: number;
  poll_interval_ms: number;
  condition_func: () => Promise<boolean>;
}): Promise<void> {
  const started_unix_ms = Date.now();

  while (Date.now() - started_unix_ms <= params.timeout_ms) {
    if (await params.condition_func()) {
      return;
    }

    await new Promise<void>((resolve): void => {
      setTimeout(resolve, params.poll_interval_ms);
    });
  }

  throw new Error(`Condition not met within ${params.timeout_ms}ms.`);
}
