import type { cluster_http2_transport_security_config_t } from '../../src/classes/clustertransport/ClusterHttp2Transport.class';
import type { cluster_tls_client_config_t, cluster_tls_server_config_t } from '../../src/classes/clustertransport/ClusterTlsSecurity.class';
import {
  phase10_ca_cert_pem,
  phase10_client_cert_pem,
  phase10_client_key_pem,
  phase10_server_cert_pem,
  phase10_server_key_pem
} from './phase10_tls_material';

export function BuildSecureServerTlsConfig(): cluster_tls_server_config_t {
  return {
    key_pem: phase10_server_key_pem,
    cert_pem: phase10_server_cert_pem,
    ca_pem_list: [phase10_ca_cert_pem],
    request_client_cert: true,
    reject_unauthorized_client_cert: true,
    min_tls_version: 'TLSv1.2'
  };
}

export function BuildSecureClientTlsConfig(): cluster_tls_client_config_t {
  return {
    ca_pem_list: [phase10_ca_cert_pem],
    client_cert_pem: phase10_client_cert_pem,
    client_key_pem: phase10_client_key_pem,
    reject_unauthorized: true,
    servername: 'localhost',
    min_tls_version: 'TLSv1.2'
  };
}

export function BuildSecureNodeTransportSecurity(params?: {
  token_validation?: cluster_http2_transport_security_config_t['token_validation'];
  replay_protection?: cluster_http2_transport_security_config_t['replay_protection'];
  session?: cluster_http2_transport_security_config_t['session'];
}): cluster_http2_transport_security_config_t {
  return {
    tls: BuildSecureServerTlsConfig(),
    token_validation: params?.token_validation,
    replay_protection: params?.replay_protection,
    session: params?.session
  };
}
