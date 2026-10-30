package api

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
)

// writeCacheable writes a JSON body with a strong ETag over its exact bytes,
// answering a matching conditional request with 304 instead.
//
// Hashing the bytes makes it trivially correct, and is right for responses that
// only change when their content does. A discovery page is not one of those: see
// writeDiscovery. The work a 304 skips is local either way, so what an ETag saves
// is bandwidth, which is exactly what td_http_bytes_saved_by_304_total claims —
// not provider requests, which the balance cache is responsible for.
func (d Deps) writeCacheable(w http.ResponseWriter, r *http.Request, body any) {
	encoded, err := json.Marshal(body)
	if err != nil {
		writeProblem(w, r, http.StatusInternalServerError, "encoding-failed",
			"Response could not be encoded", err.Error())
		return
	}
	// A page is only valid for the block height it names, so it must not be
	// reused without revalidation; no-cache permits the conditional request
	// while forbidding a blind cache hit.
	w.Header().Set("Cache-Control", "no-cache")
	d.writeEncoded(w, r, encoded, `"`+hashOf(encoded)+`"`)
}

// writeDiscovery writes a discovery page with a weak ETag over validator, which
// is the page minus whatever moves on its own.
//
// Hashing the whole body would be useless here. The height moves every 12
// seconds, so for a client polling once a minute the validator would always have
// changed and a 304 would never happen. So the validator holds what a client
// acts on: tokens, ranges, metadata, balance values and their status. A 304 stays
// honest because the current head still travels in X-Block-Number and
// X-Block-Hash, which a 304 may carry, and because a token or balance that
// actually changes changes the validator.
//
// private is load-bearing: this is per-account balance data.
func (d Deps) writeDiscovery(w http.ResponseWriter, r *http.Request, body, validator any, block blockInfo) {
	encoded, err := json.Marshal(body)
	if err != nil {
		writeProblem(w, r, http.StatusInternalServerError, "encoding-failed",
			"Response could not be encoded", err.Error())
		return
	}
	tag, err := json.Marshal(validator)
	if err != nil {
		writeProblem(w, r, http.StatusInternalServerError, "encoding-failed",
			"Response could not be encoded", err.Error())
		return
	}
	w.Header().Set("Cache-Control", "private, no-cache")
	w.Header().Set("X-Block-Number", strconv.FormatUint(block.Height, 10))
	if block.Hash != "" {
		w.Header().Set("X-Block-Hash", block.Hash)
	}
	d.writeEncoded(w, r, encoded, `W/"`+hashOf(tag)+`"`)
}

func (d Deps) writeEncoded(w http.ResponseWriter, r *http.Request, encoded []byte, etag string) {
	w.Header().Set("ETag", etag)
	if ifNoneMatch(r.Header.Get("If-None-Match"), etag) {
		w.WriteHeader(http.StatusNotModified)
		if d.Metrics != nil {
			d.Metrics.HTTPSavedBy304.Add(float64(len(encoded)))
		}
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(encoded)
}

func hashOf(body []byte) string {
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:16])
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
