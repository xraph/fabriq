package client

import "testing"

func TestParseDSN(t *testing.T) {
	tests := []struct {
		name    string
		dsn     string
		want    DSN
		wantErr bool
	}{
		{
			name: "https default port with tls=true override",
			dsn:  "fabriq://fq_k@h.co/acme?tls=true",
			want: DSN{
				Transport: "http",
				TLS:       true,
				Host:      "h.co",
				Port:      "443",
				BasePath:  "/admin",
				Tenant:    "acme",
				Key:       "fq_k",
				Version:   1,
			},
		},
		{
			name: "localhost with explicit port and tls=false override",
			dsn:  "fabriq://fq_k@localhost:8080/acme?tls=false",
			want: DSN{
				Transport: "http",
				TLS:       false,
				Host:      "localhost",
				Port:      "8080",
				BasePath:  "/admin",
				Tenant:    "acme",
				Key:       "fq_k",
				Version:   1,
			},
		},
		{
			name: "localhost no tenant path",
			dsn:  "fabriq://fq_k@localhost:8080",
			want: DSN{
				Transport: "http",
				TLS:       false,
				Host:      "localhost",
				Port:      "8080",
				BasePath:  "/admin",
				Tenant:    "",
				Key:       "fq_k",
				Version:   1,
			},
		},
		{
			name:    "missing key errors",
			dsn:     "fabriq://h.co",
			wantErr: true,
		},
		{
			name:    "bad scheme errors",
			dsn:     "pg://x",
			wantErr: true,
		},
		{
			name: "grpc transport",
			dsn:  "fabriq+grpc://fq_k@h",
			want: DSN{
				Transport: "grpc",
				TLS:       true,
				Host:      "h",
				Port:      "443",
				BasePath:  "/admin",
				Tenant:    "",
				Key:       "fq_k",
				Version:   1,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseDSN(tt.dsn)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("ParseDSN(%q) = %+v, want error", tt.dsn, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseDSN(%q) unexpected error: %v", tt.dsn, err)
			}
			if got != tt.want {
				t.Fatalf("ParseDSN(%q) = %+v, want %+v", tt.dsn, got, tt.want)
			}
		})
	}
}

func TestDSN_BaseURL(t *testing.T) {
	tests := []struct {
		name string
		dsn  DSN
		want string
	}{
		{
			name: "tls on",
			dsn: DSN{
				TLS:      true,
				Host:     "h.co",
				Port:     "443",
				BasePath: "/admin",
			},
			want: "https://h.co:443/admin",
		},
		{
			name: "tls off",
			dsn: DSN{
				TLS:      false,
				Host:     "localhost",
				Port:     "8080",
				BasePath: "/admin",
			},
			want: "http://localhost:8080/admin",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.dsn.BaseURL(); got != tt.want {
				t.Fatalf("BaseURL() = %q, want %q", got, tt.want)
			}
		})
	}
}
