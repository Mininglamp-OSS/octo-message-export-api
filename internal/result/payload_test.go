package result

import "testing"

func TestExtractPayload(t *testing.T) {
	cases := []struct {
		name       string
		payload    map[string]any
		payloadRaw map[string]any
		wantText   string
		wantOK     bool
	}{
		{
			name:     "type1 text.content (real local OS shape)",
			payload:  map[string]any{"type": 1, "text": map[string]any{"content": "asdfaga"}},
			wantText: "asdfaga",
			wantOK:   true,
		},
		{
			name:       "type1 fallback to payloadRaw.content",
			payload:    map[string]any{"type": 1},
			payloadRaw: map[string]any{"content": "from-raw"},
			wantText:   "from-raw",
			wantOK:     true,
		},
		{
			name:     "type1 empty content still ok (caller skips on empty)",
			payload:  map[string]any{"type": 1, "text": map[string]any{"content": ""}},
			wantText: "",
			wantOK:   true,
		},
		{
			name:     "type14 richText.searchText (real indexer shape)",
			payload:  map[string]any{"type": 14, "richText": map[string]any{"searchText": "look: [图片] done"}},
			wantText: "look: [图片] done",
			wantOK:   true,
		},
		{
			name:     "type14 missing richText (indexer omits on empty) -> ok empty",
			payload:  map[string]any{"type": 14},
			wantText: "",
			wantOK:   true,
		},
		{
			name:     "type14 empty searchText -> ok empty",
			payload:  map[string]any{"type": 14, "richText": map[string]any{"searchText": ""}},
			wantText: "",
			wantOK:   true,
		},
		{
			name:    "other type skipped",
			payload: map[string]any{"type": 99, "text": map[string]any{"content": "system msg"}},
			wantOK:  false,
		},
		{
			name:    "nil payload skipped",
			payload: nil,
			wantOK:  false,
		},
		{
			name:    "missing type treated as other -> skipped",
			payload: map[string]any{"text": map[string]any{"content": "x"}},
			wantOK:  false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			text, ok := extractPayload(tc.payload, tc.payloadRaw)
			if ok != tc.wantOK {
				t.Fatalf("ok = %v, want %v", ok, tc.wantOK)
			}
			if ok && text != tc.wantText {
				t.Errorf("text = %q, want %q", text, tc.wantText)
			}
		})
	}
}
