// Package evm is the generic EVM chain adapter. It is parameterised entirely by
// configuration, so every EVM network — Ethereum, Polygon, Arbitrum, Base — is
// served by this one implementation.
package evm

import (
	"tokendiscovery/internal/core/domain"
)

// TransferTopic0 is keccak256("Transfer(address,address,uint256)").
//
// ERC-20 and ERC-721 share this signature, which is the whole basis of the
// service: one eth_getLogs filtered on this single topic captures every
// token interaction on the chain for every account at once. The two standards
// are then separated by log shape, at no additional RPC cost.
//
// TestTransferTopic0MatchesKeccak derives this from the signature string rather
// than trusting the literal.
var TransferTopic0 = domain.MustParseHash(
	"0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef")

// Log is a JSON-RPC log object.
//
// Every field is a string rather than a decoded type on purpose. A typed
// json.Unmarshaler would abort the entire response on one malformed value,
// which would turn a single bad log into a lost 200-block range. Decoding into
// strings always succeeds for well-formed JSON, so each log can then be
// validated and skipped individually — the decoder isolates per-log faults and
// counts them instead of failing the batch.
type Log struct {
	Address     string   `json:"address"`
	Topics      []string `json:"topics"`
	Data        string   `json:"data"`
	BlockNumber string   `json:"blockNumber"`
	BlockHash   string   `json:"blockHash"`
	TxHash      string   `json:"transactionHash"`
	LogIndex    string   `json:"logIndex"`
	Removed     bool     `json:"removed"`
}
