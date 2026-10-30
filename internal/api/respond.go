package api

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"strings"
)

// writeCacheable writes a JSON body with an ETag, answering a matching
// conditional request with 304 instead.
//
// The ETag is a hash of the exact bytes, which makes it trivially correct —
// the body cannot drift from the validator — at the cost of building the
// response before discovering it was unchanged. That trade is right here
// because the work it does not save is *local*: a discovery page is a Pebble
// prefix scan plus, when the cache is clean, zero RPC calls. So the saving an
// ETag actually delivers is bandwidth, which is also exactly what the
// td_http_bytes_saved_by_304_total metric claims — not a reduction in provider
// requests, which the cache is responsible for.
//
// It returns the number of body bytes written, so the caller can record what a
// 304 avoided.
func (d Deps) writeCacheable(w http.ResponseWriter, r *http.Request, body any) (written int, notModified bool) {
	encoded, err := json.Marshal(body)
	if err != nil {
		writeProblem(w, r, http.StatusInternalServerError, "encoding-failed",
			"Response could not be encoded", err.Error())
		return 0, false
	}

	etag := etagOf(encoded)
	w.Header().Set("ETag", etag)
	// A page is only valid for the block height it names, so it must not be
	// reused without revalidation; no-cache permits the conditional request
	// while forbidding a blind cache hit.
	w.Header().Set("Cache-Control", "no-cache")

	if ifNoneMatch(r.Header.Get("If-None-Match"), etag) {
		w.WriteHeader(http.StatusNotModified)
		if d.Metrics != nil {
			d.Metrics.HTTPSavedBy304.Add(float64(len(encoded)))
		}
		return len(encoded), true
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	n, _ := w.Write(encoded)
	return n, false
}

// etagOf is a strong validator over the response bytes.
func etagOf(body []byte) string {
	sum := sha256.Sum256(body)
	return `"` + hex.EncodeToString(sum[:16]) + `"`
}

// ifNoneMatch implements the RFC 9110 comparison: a comma-separated list, the
// wildcard, and weak validators compared by their opaque part.
func ifNoneMatch(header, etag string) bool {
	if header == "" {
		return false
	}
	if strings.TrimSpace(header) == "*" {
		return true
	}
	for _, candidate := range strings.Split(header, ",") {
		if weakEqual(strings.TrimSpace(candidate), etag) {
			return true
		}
	}
	return false
}

func weakEqual(a, b string) bool {
	return strings.TrimPrefix(a, "W/") == strings.TrimPrefix(b, "W/")
}
