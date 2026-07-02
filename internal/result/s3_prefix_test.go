package result

import "testing"

func TestWithPrefix(t *testing.T) {
	cases := []struct {
		name      string
		keyPrefix string
		in        string
		want      string
	}{
		{"empty prefix", "", "octo-message-export-api/a/b", "octo-message-export-api/a/b"},
		{"plain prefix", "test", "octo-message-export-api/a/b", "test/octo-message-export-api/a/b"},
		{"trailing slash", "test/", "octo-message-export-api/a/b", "test/octo-message-export-api/a/b"},
		{"leading+trailing slash", "/prod/", "octo-message-export-api/a/b", "prod/octo-message-export-api/a/b"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			u := &S3Uploader{cfg: S3Config{KeyPrefix: tc.keyPrefix}}
			if got := u.withPrefix(tc.in); got != tc.want {
				t.Errorf("withPrefix(%q) with KeyPrefix=%q = %q, want %q", tc.in, tc.keyPrefix, got, tc.want)
			}
		})
	}
}

func TestTaskPrefix(t *testing.T) {
	u := &S3Uploader{cfg: S3Config{KeyPrefix: "test"}}
	got := u.TaskPrefix("2026-06-16", "octo-search", "abc123")
	want := "test/octo-message-export-api/2026-06-16/octo-search/abc123/"
	if got != want {
		t.Errorf("TaskPrefix = %q, want %q", got, want)
	}
}
