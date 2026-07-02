package config

import "testing"

func TestParseCallerTokens(t *testing.T) {
	cases := []struct {
		name string
		raw  string
		want map[string]string
	}{
		{
			name: "name=token pairs",
			raw:  "smart-summary=sk_aaa,reporter=sk_bbb",
			want: map[string]string{"sk_aaa": "smart-summary", "sk_bbb": "reporter"},
		},
		{
			name: "bare token falls back to default",
			raw:  "test-token-smart-summary",
			want: map[string]string{"test-token-smart-summary": "default"},
		},
		{
			name: "mixed bare and named",
			raw:  "bare-tok, named=sk_xxx ",
			want: map[string]string{"bare-tok": "default", "sk_xxx": "named"},
		},
		{
			name: "empty",
			raw:  "",
			want: map[string]string{},
		},
		{
			name: "empty name before = falls back to default",
			raw:  "=sk_only_token",
			want: map[string]string{"sk_only_token": "default"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := parseCallerTokens(tc.raw)
			if len(got) != len(tc.want) {
				t.Fatalf("len = %d, want %d (%v)", len(got), len(tc.want), got)
			}
			for k, v := range tc.want {
				if got[k] != v {
					t.Errorf("token %q -> %q, want %q", k, got[k], v)
				}
			}
		})
	}
}

func TestValidate(t *testing.T) {
	base := func() *Config {
		return &Config{
			OS:    OSConfig{Endpoints: []string{"http://os:9200"}},
			MySQL: MySQLConfig{DSN: "dsn"},
			S3:    S3Config{Bucket: "b", KeyPrefix: "test"},
			Auth:  AuthConfig{Enabled: true, CallerTokens: map[string]string{"t": "c"}},
			Exec:  ExecConfig{Workers: 4},
		}
	}
	if err := base().validate(); err != nil {
		t.Fatalf("valid config rejected: %v", err)
	}

	c := base()
	c.OS.Endpoints = nil
	if err := c.validate(); err == nil {
		t.Error("expected error for missing OS endpoints")
	}

	c = base()
	c.S3.KeyPrefix = ""
	if err := c.validate(); err == nil {
		t.Error("expected error for missing S3 key prefix")
	}

	c = base()
	c.Auth.CallerTokens = map[string]string{}
	if err := c.validate(); err == nil {
		t.Error("expected error for auth enabled with no tokens")
	}

	c = base()
	c.Auth.Enabled = false
	c.Auth.CallerTokens = map[string]string{}
	if err := c.validate(); err != nil {
		t.Errorf("auth disabled with no tokens should be valid: %v", err)
	}
}
