// Command measure is the phase-1 throwaway measurement tool.
//
// It replaces the estimates in the plan with numbers taken from a real RPC
// endpoint: Transfer-log density per block, the ERC-20 / ERC-721 split derived
// from log shape, how many account-token pairs each block yields, response
// sizes, and encoding/json decode throughput. Everything it prints is intended
// to be pasted into docs/measurements.md.
//
// Usage: RPC_URL=... go run ./tools/measure [-windows 10] [-window-size 200]
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"hash/maphash"
	"io"
	"log"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

// keccak256("Transfer(address,address,uint256)") — shared by ERC-20 and ERC-721.
const transferTopic0 = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"

const zeroAddr = "0000000000000000000000000000000000000000"

type rpcReq struct {
	JSONRPC string `json:"jsonrpc"`
	ID      int    `json:"id"`
	Method  string `json:"method"`
	Params  []any  `json:"params"`
}

type logEntry struct {
	Address     string   `json:"address"`
	Topics      []string `json:"topics"`
	Data        string   `json:"data"`
	BlockNumber string   `json:"blockNumber"`
}

type rpcResp struct {
	Result []logEntry `json:"result"`
	Error  *struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

type standard int

const (
	stdERC20 standard = iota
	stdERC721
	stdOther
)

// classify decides the token standard from log shape alone — no contract calls.
// ERC-20: 3 topics (sig, from, to) + 32 bytes of data (the unindexed value).
// ERC-721: 4 topics (sig, from, to, tokenId) + empty data.
func classify(l logEntry) standard {
	switch {
	case len(l.Topics) == 3 && len(l.Data) == 2+64:
		return stdERC20
	case len(l.Topics) == 4 && (l.Data == "0x" || l.Data == ""):
		return stdERC721
	default:
		return stdOther
	}
}

// topicToAddr takes the low 20 bytes of a 32-byte topic, lowercased, no 0x.
func topicToAddr(t string) (string, bool) {
	t = strings.TrimPrefix(strings.ToLower(t), "0x")
	if len(t) != 64 {
		return "", false
	}
	return t[24:], true
}

type stats struct {
	blocks       int
	logs         int
	byStd        map[standard]int
	shapeHist    map[string]int
	pairEvents   int // gross (account, token) emissions, zero-address excluded
	zeroSkipped  int
	uniquePairs  map[string]struct{}
	uniqueTokens map[string]struct{}
	uniqueAccts  map[string]struct{}
	decodedBytes int64
	decodeNanos  int64
	requestNanos int64
	calls        int
}

func newStats() *stats {
	return &stats{
		byStd:        map[standard]int{},
		shapeHist:    map[string]int{},
		uniquePairs:  map[string]struct{}{},
		uniqueTokens: map[string]struct{}{},
		uniqueAccts:  map[string]struct{}{},
	}
}

func (s *stats) ingest(logs []logEntry) {
	for _, l := range logs {
		s.logs++
		std := classify(l)
		s.byStd[std]++
		s.shapeHist[fmt.Sprintf("topics=%d data=%dB", len(l.Topics), (len(l.Data)-2)/2)]++
		if std == stdOther {
			continue
		}
		token := strings.TrimPrefix(strings.ToLower(l.Address), "0x")
		s.uniqueTokens[token] = struct{}{}
		for _, t := range l.Topics[1:3] {
			acct, ok := topicToAddr(t)
			if !ok {
				continue
			}
			if acct == zeroAddr {
				s.zeroSkipped++
				continue
			}
			s.pairEvents++
			s.uniquePairs[acct+token] = struct{}{}
			s.uniqueAccts[acct] = struct{}{}
		}
	}
}

type client struct {
	url string
	hc  *http.Client
}

// getLogs fetches Transfer logs for [from,to] and reports decoded size plus the
// time split between the request and encoding/json.
func (c *client) getLogs(from, to uint64) ([]logEntry, int64, time.Duration, time.Duration, error) {
	body, err := json.Marshal(rpcReq{
		JSONRPC: "2.0", ID: 1, Method: "eth_getLogs",
		Params: []any{map[string]any{
			"fromBlock": fmt.Sprintf("0x%x", from),
			"toBlock":   fmt.Sprintf("0x%x", to),
			"topics":    []string{transferTopic0},
		}},
	})
	if err != nil {
		return nil, 0, 0, 0, err
	}

	reqStart := time.Now()
	req, err := http.NewRequest("POST", c.url, bytes.NewReader(body))
	if err != nil {
		return nil, 0, 0, 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	// NOTE: deliberately not setting Accept-Encoding — Go's Transport adds
	// gzip and decompresses transparently. Setting it by hand would make us
	// responsible for decompression. Measured: ~14x smaller, ~12x faster.
	resp, err := c.hc.Do(req)
	if err != nil {
		return nil, 0, 0, 0, err
	}
	defer resp.Body.Close()

	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, 0, 0, 0, err
	}
	reqDur := time.Since(reqStart)

	decStart := time.Now()
	var out rpcResp
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, 0, 0, 0, fmt.Errorf("decode: %w (body starts %.120q)", err, raw)
	}
	decDur := time.Since(decStart)

	if out.Error != nil {
		return nil, 0, 0, 0, fmt.Errorf("rpc error %d: %s", out.Error.Code, out.Error.Message)
	}
	return out.Result, int64(len(raw)), reqDur, decDur, nil
}

func (c *client) blockNumber() (uint64, error) {
	body, _ := json.Marshal(rpcReq{JSONRPC: "2.0", ID: 1, Method: "eth_blockNumber", Params: []any{}})
	resp, err := c.hc.Post(c.url, "application/json", bytes.NewReader(body))
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	var out struct {
		Result string `json:"result"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return 0, err
	}
	return strconv.ParseUint(strings.TrimPrefix(out.Result, "0x"), 16, 64)
}

// curve scans a contiguous run and reports how the unique-pair count grows with
// blocks scanned. This is what sizes the store: pair emissions are a loose upper
// bound because popular pairs recur, so the growth curve is the honest input.
func curve(c *client, from uint64, blocks, chunk int) {
	var seed = maphash.MakeSeed()
	seen := make(map[uint64]struct{}, 8<<20)
	emissions := 0
	scanned := 0
	fmt.Printf("%8s  %12s  %12s  %7s  %s\n", "blocks", "emissions", "uniquePairs", "dedup", "unique/block")
	for scanned < blocks {
		n := min(chunk, blocks-scanned)
		lo := from + uint64(scanned)
		logs, _, _, _, err := c.getLogs(lo, lo+uint64(n)-1)
		if err != nil {
			log.Printf("curve chunk at %d: %v", lo, err)
			return
		}
		for _, l := range logs {
			if classify(l) == stdOther {
				continue
			}
			token := strings.TrimPrefix(strings.ToLower(l.Address), "0x")
			for _, t := range l.Topics[1:3] {
				acct, ok := topicToAddr(t)
				if !ok || acct == zeroAddr {
					continue
				}
				emissions++
				seen[maphash.String(seed, acct+token)] = struct{}{}
			}
		}
		scanned += n
		fmt.Printf("%8d  %12d  %12d  %6.2fx  %12.1f\n",
			scanned, emissions, len(seen),
			float64(emissions)/float64(len(seen)), float64(len(seen))/float64(scanned))
	}
}

func main() {
	var (
		windows    = flag.Int("windows", 10, "number of sample windows spread across the range")
		windowSize = flag.Int("window-size", 200, "blocks per window")
		startBlock = flag.Uint64("start-block", 25_933_634, "assignment start block")
		curveN     = flag.Int("curve", 0, "if >0, scan this many contiguous blocks and report unique-pair growth")
		curveChunk = flag.Int("curve-chunk", 250, "blocks per request in curve mode")
	)
	flag.Parse()

	url := os.Getenv("RPC_URL")
	if url == "" {
		log.Fatal("RPC_URL is required (see .env.example)")
	}
	c := &client{url: url, hc: &http.Client{Timeout: 5 * time.Minute}}

	tip, err := c.blockNumber()
	if err != nil {
		log.Fatalf("blockNumber: %v", err)
	}
	span := tip - *startBlock
	fmt.Printf("tip=%d startBlock=%d span=%d blocks (%.1f days at 12s)\n\n",
		tip, *startBlock, span, float64(span)*12/86400)

	if *curveN > 0 {
		curve(c, *startBlock, *curveN, *curveChunk)
		return
	}

	s := newStats()
	stride := span / uint64(*windows)
	for i := range *windows {
		from := *startBlock + stride*uint64(i)
		to := from + uint64(*windowSize) - 1
		if to > tip {
			to = tip
		}
		logs, nbytes, reqDur, decDur, err := c.getLogs(from, to)
		if err != nil {
			log.Printf("window %d (%d-%d): %v", i, from, to, err)
			continue
		}
		s.calls++
		s.blocks += int(to - from + 1)
		s.decodedBytes += nbytes
		s.requestNanos += reqDur.Nanoseconds()
		s.decodeNanos += decDur.Nanoseconds()
		s.ingest(logs)
		fmt.Printf("window %2d  blocks %d-%d  logs=%6d  %6.1f MB  req=%5.2fs  decode=%5.2fs\n",
			i, from, to, len(logs), float64(nbytes)/1e6, reqDur.Seconds(), decDur.Seconds())
	}
	if s.blocks == 0 {
		log.Fatal("no windows succeeded")
	}
	report(s, span)
}

func report(s *stats, span uint64) {
	perBlock := func(n int) float64 { return float64(n) / float64(s.blocks) }
	fmt.Printf("\n===== sample: %d blocks across %d windows =====\n", s.blocks, s.calls)

	fmt.Printf("\n-- log density\n")
	fmt.Printf("  Transfer logs           %d  (%.1f per block)\n", s.logs, perBlock(s.logs))
	fmt.Printf("  ERC-20   (3 topics+32B) %d  (%.1f%%)\n", s.byStd[stdERC20], 100*float64(s.byStd[stdERC20])/float64(s.logs))
	fmt.Printf("  ERC-721  (4 topics+0B)  %d  (%.1f%%)\n", s.byStd[stdERC721], 100*float64(s.byStd[stdERC721])/float64(s.logs))
	fmt.Printf("  unclassified            %d  (%.2f%%)\n", s.byStd[stdOther], 100*float64(s.byStd[stdOther])/float64(s.logs))

	fmt.Printf("\n-- log shapes seen (validates shape-based classification)\n")
	type kv struct {
		k string
		v int
	}
	var shapes []kv
	for k, v := range s.shapeHist {
		shapes = append(shapes, kv{k, v})
	}
	sort.Slice(shapes, func(i, j int) bool { return shapes[i].v > shapes[j].v })
	for i, sh := range shapes {
		if i >= 6 {
			fmt.Printf("  ... %d more shapes\n", len(shapes)-6)
			break
		}
		fmt.Printf("  %-22s %8d  (%.2f%%)\n", sh.k, sh.v, 100*float64(sh.v)/float64(s.logs))
	}

	fmt.Printf("\n-- account-token pairs\n")
	fmt.Printf("  pair emissions          %d  (%.1f per block)\n", s.pairEvents, perBlock(s.pairEvents))
	fmt.Printf("  zero-address skipped    %d  (mint/burn legs)\n", s.zeroSkipped)
	fmt.Printf("  unique pairs in sample  %d  (dedup %.2fx within sample)\n",
		len(s.uniquePairs), float64(s.pairEvents)/float64(len(s.uniquePairs)))
	fmt.Printf("  unique tokens           %d\n", len(s.uniqueTokens))
	fmt.Printf("  unique accounts         %d\n", len(s.uniqueAccts))

	fmt.Printf("\n-- transfer + decode cost\n")
	reqSec := float64(s.requestNanos) / 1e9
	decSec := float64(s.decodeNanos) / 1e9
	mb := float64(s.decodedBytes) / 1e6
	fmt.Printf("  decoded JSON            %.1f MB  (%.0f KB per block)\n", mb, float64(s.decodedBytes)/float64(s.blocks)/1e3)
	fmt.Printf("  request wall time       %.2fs  (%.1f MB/s decoded-equivalent)\n", reqSec, mb/reqSec)
	fmt.Printf("  encoding/json decode    %.2fs  (%.0f MB/s, %.0f%% of request time)\n",
		decSec, mb/decSec, 100*decSec/reqSec)

	fmt.Printf("\n===== extrapolated to the full %d-block window =====\n", span)
	scale := float64(span) / float64(s.blocks)
	totalLogs := float64(s.logs) * scale
	totalBytes := float64(s.decodedBytes) * scale
	fmt.Printf("  Transfer logs           %.0fM\n", totalLogs/1e6)
	fmt.Printf("  JSON transferred        %.0f GB decoded / ~%.1f GB on the wire (14.5x gzip, measured)\n",
		totalBytes/1e9, totalBytes/1e9/14.5)
	fmt.Printf("  pair emissions          %.0fM  (upper bound on unique pairs)\n", float64(s.pairEvents)*scale/1e6)
	fmt.Printf("  backfill wall time      ~%.0f min at the observed single-stream rate\n",
		(totalBytes/(mb*1e6/reqSec))/60)
	fmt.Printf("  decode CPU              ~%.0f min total (%.2f cores if backfill takes %.0f min)\n",
		(totalBytes/(mb*1e6/decSec))/60,
		decSec/reqSec, (totalBytes/(mb*1e6/reqSec))/60)
	fmt.Printf("\n  NOTE: unique-pair count for the full range is strictly below the pair-emission\n")
	fmt.Printf("        upper bound; popular pairs recur heavily across blocks, so treat the\n")
	fmt.Printf("        store-size figure as a ceiling, not an estimate.\n")
}
