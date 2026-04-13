package main

import "testing"

func TestExtractBookCode(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"000413259473d01615745", "000413259473d01615745"},
		{"https://www.calameo.com/books/000413259473d01615745", "000413259473d01615745"},
		{"https://www.calameo.com/read/000413259473d01615745?authid=foo", "000413259473d01615745"},
		{"https://v.calameo.com/?bkcode=000413259473d01615745", "000413259473d01615745"},
	}

	for _, tt := range tests {
		got, err := extractBookCode(tt.input)
		if err != nil {
			t.Fatalf("extractBookCode(%q) returned error: %v", tt.input, err)
		}
		if got != tt.want {
			t.Fatalf("extractBookCode(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func TestBuildSignedPageURL(t *testing.T) {
	got := buildSignedPageURL("https://ps.calameoassets.com/", "abc123", 7, "?_token_=exp=1~acl=%2Fabc123%2F%2A~hmac=sig", "jpg")
	want := "https://ps.calameoassets.com/abc123/p7.jpg?_token_=exp=1~acl=%2Fabc123%2F%2A~hmac=sig"
	if got != want {
		t.Fatalf("buildSignedPageURL() = %q, want %q", got, want)
	}
}

func TestSanitizeFilename(t *testing.T) {
	got := sanitizeFilename(`  A:/Bad*Name?.pdf  `)
	want := "A__Bad_Name_.pdf"
	if got != want {
		t.Fatalf("sanitizeFilename() = %q, want %q", got, want)
	}
}
