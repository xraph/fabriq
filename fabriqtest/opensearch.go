package fabriqtest

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// OpenSearchImage pins the native OpenSearch conformance server.
const OpenSearchImage = "opensearchproject/opensearch@sha256:3b55f6b6249cfdb92b52ee59adc8a266203ba9ef9f5d82544fad6f1af2f3609e"

// SearchServer is an authenticated, TLS-verified local search test endpoint.
type SearchServer struct {
	URL      string
	Username string
	Password string
	CACert   []byte
}

// StartOpenSearch starts a single native node with security enabled and a
// fresh certificate for localhost. The test owns and terminates the container.
func StartOpenSearch(t testing.TB) SearchServer {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	cert := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "localhost"}, DNSNames: []string{"localhost"}, IPAddresses: []net.IP{net.ParseIP("127.0.0.1"), net.ParseIP("::1")}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment | x509.KeyUsageCertSign, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
	der, err := x509.CreateCertificate(rand.Reader, cert, cert, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	ca := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	pkcs8, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: pkcs8})
	roots := x509.NewCertPool()
	roots.AppendCertsFromPEM(ca)
	server := SearchServer{Username: "admin", Password: "FabriqTest!2026-OpenSearch", CACert: ca}
	ctx := context.Background()
	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{ContainerRequest: testcontainers.ContainerRequest{
		Name: fmt.Sprintf("fabriq-opensearch-%d", time.Now().UnixNano()), Image: OpenSearchImage, ExposedPorts: []string{"9200/tcp"},
		Env: map[string]string{
			"discovery.type": "single-node", "OPENSEARCH_JAVA_OPTS": "-Xms512m -Xmx512m", "OPENSEARCH_INITIAL_ADMIN_PASSWORD": server.Password, "DISABLE_PERFORMANCE_ANALYZER_AGENT_CLI": "true",
			"plugins.security.ssl.http.pemcert_filepath": "fabriq-test-cert.pem", "plugins.security.ssl.http.pemkey_filepath": "fabriq-test-key.pem", "plugins.security.ssl.http.pemtrustedcas_filepath": "fabriq-test-cert.pem",
		},
		Files: []testcontainers.ContainerFile{
			{Reader: bytes.NewReader(ca), ContainerFilePath: "/usr/share/opensearch/config/fabriq-test-cert.pem", FileMode: 0o644},
			{Reader: bytes.NewReader(keyPEM), ContainerFilePath: "/usr/share/opensearch/config/fabriq-test-key.pem", FileMode: 0o644},
		},
		WaitingFor: wait.ForHTTP("/_cluster/health?wait_for_status=yellow&timeout=60s").WithPort("9200/tcp").WithTLS(true, &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}).WithBasicAuth(server.Username, server.Password).WithStartupTimeout(3 * time.Minute),
	}, Started: true})
	if err != nil {
		t.Fatalf("fabriqtest: start opensearch: %v", err)
	}
	t.Cleanup(func() {
		if termErr := testcontainers.TerminateContainer(c); termErr != nil {
			t.Logf("terminate opensearch: %v", termErr)
		}
	})
	ep, err := c.Endpoint(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	server.URL = "https://" + ep
	return server
}
