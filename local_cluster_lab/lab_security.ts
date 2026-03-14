import type { cluster_http2_transport_security_config_t } from '../src/classes/clustertransport/ClusterHttp2Transport.class';
import type {
  cluster_tls_client_config_t,
  cluster_tls_server_config_t
} from '../src/classes/clustertransport/ClusterTlsSecurity.class';
import {
  phase10_ca_cert_pem,
  phase10_client_cert_pem,
  phase10_client_key_pem,
  phase10_server_cert_pem,
  phase10_server_key_pem
} from '../test/fixtures/phase10_tls_material';

export function BuildLabTlsServerConfig(): cluster_tls_server_config_t {
  return {
    key_pem: phase10_server_key_pem,
    cert_pem: phase10_server_cert_pem,
    ca_pem_list: [phase10_ca_cert_pem],
    request_client_cert: true,
    reject_unauthorized_client_cert: true,
    min_tls_version: 'TLSv1.2'
  };
}

export function BuildLabTlsClientConfig(): cluster_tls_client_config_t {
  return {
    ca_pem_list: [phase10_ca_cert_pem],
    client_cert_pem: phase10_client_cert_pem,
    client_key_pem: phase10_client_key_pem,
    reject_unauthorized: true,
    servername: 'localhost',
    min_tls_version: 'TLSv1.2'
  };
}

export function BuildLabNodeTransportSecurity(): cluster_http2_transport_security_config_t {
  return {
    tls: BuildLabTlsServerConfig()
  };
}
