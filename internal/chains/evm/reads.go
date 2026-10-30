package evm

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"sync"
	"unicode"
	"unicode/utf8"

	"tokendiscovery/internal/core/domain"
	"tokendiscovery/internal/rpc"
)

// Per-token outcomes. These reach API responses, so they are fixed strings: a
// transport error can carry the endpoint URL, which carries the API key.
const (
	errBalanceUnread    = "balance could not be read"
	errBalanceReverted  = "balanceOf reverted"
	errBalanceMalformed = "balanceOf did not return a uint256"
)

// multicall runs one batch and reports the block it actually executed at.
//
// The leading getBlockNumber makes the answer self-verifying: the block is
// stated by the EVM that ran the batch, not assumed from the request, so a node
// that served the call from some other height cannot pass it off as the pin.
func (a *Adapter) multicall(ctx context.Context, blockRef string, calls []mcCall) (uint64, []mcResult, error) {
	all := make([]mcCall, 0, len(calls)+1)
	all = append(all, mcCall{target: a.cfg.MulticallAddress, data: selGetBlockNumber})
	all = append(all, calls...)

	if a.m != nil {
		a.m.MulticallCalls.Inc()
		a.m.MulticallSlots.Add(float64(len(calls)))
	}

	var result string
	err := a.rpc.Call(ctx, &result, "eth_call", map[string]any{
		"to":   a.cfg.MulticallAddress.Hex(),
		"data": "0x" + hex.EncodeToString(encodeAggregate3(all)),
	}, blockRef)
	if err != nil {
		return 0, nil, err
	}

	raw, err := hex.DecodeString(strings.TrimPrefix(result, "0x"))
	if err != nil {
		return 0, nil, fmt.Errorf("multicall result: %w", err)
	}
	// An eth_call to an address with no code succeeds with empty output, so
	// a chain without this deployment ends up here rather than as an RPC error.
	if len(raw) == 0 {
		return 0, nil, fmt.Errorf("multicall returned nothing: no contract at %s at %s", a.cfg.MulticallAddress.Hex(), blockRef)
	}
	results, err := decodeAggregate3(raw, len(all))
	if err != nil {
		return 0, nil, fmt.Errorf("multicall result: %w", err)
	}
	if !results[0].success {
		return 0, nil, errors.New("multicall: getBlockNumber failed")
	}
	executed, err := abiUint(results[0].ret, 0)
	if err != nil {
		return 0, nil, fmt.Errorf("multicall block number: %w", err)
	}
	return executed, results[1:], nil
}

// Balances implements ports.BalanceReader.
//
// Chunks run concurrently: the caller has a sub-second budget, and running them
// in sequence would let an early slow chunk starve the rest. The width is
// bounded by the API's page limit over the chunk size.
func (a *Adapter) Balances(ctx context.Context, account domain.Address, tokens []domain.TokenRef, atBlock uint64) ([]domain.Balance, error) {
	out := make([]domain.Balance, len(tokens))
	for i, t := range tokens {
		out[i] = domain.Balance{Token: t.Address, Err: errBalanceUnread}
	}

	chunk := a.cfg.MulticallChunk
	errs := make([]error, (len(out)+chunk-1)/chunk)
	var wg sync.WaitGroup
	for i := range errs {
		part := out[i*chunk : min((i+1)*chunk, len(out))]
		wg.Go(func() {
			errs[i] = a.readBalances(ctx, account, part, hexUint64(atBlock))
		})
	}
	wg.Wait()
	return out, errors.Join(errs...)
}

// readBalances fills out in place, handling the two whole-batch failures that
// have an answer other than giving up.
func (a *Adapter) readBalances(ctx context.Context, account domain.Address, out []domain.Balance, blockRef string) error {
	calls := make([]mcCall, len(out))
	for i, b := range out {
		calls[i] = mcCall{target: b.Token, data: encodeBalanceOf(account)}
	}

	executed, results, err := a.multicall(ctx, blockRef, calls)
	switch {
	case err == nil:
	case errors.Is(err, rpc.ErrBeyondHead) && blockRef != tagLatest:
		// The pin is outside what this node holds: pruned state, or a head it
		// has not reached. A value at another height is still worth having,
		// because the executed block travels with it and the caller decides.
		return a.readBalances(ctx, account, out, tagLatest)
	case errors.Is(err, rpc.ErrExecutionFailed):
		// allowFailure cannot contain a contract that burns the batch's gas:
		// it takes 63/64 of what is left, and the batch itself runs out. Split
		// until the culprit is alone, so that one spam token costs a few extra
		// calls once instead of every balance on the page.
		if len(out) == 1 {
			out[0].Reverted, out[0].Err = true, errBalanceReverted
			return nil
		}
		mid := len(out) / 2
		return errors.Join(
			a.readBalances(ctx, account, out[:mid], blockRef),
			a.readBalances(ctx, account, out[mid:], blockRef),
		)
	default:
		return err
	}

	for i, r := range results {
		switch {
		case !r.success:
			out[i].Reverted, out[i].Err = true, errBalanceReverted
		case len(r.ret) != abiWord:
			// In particular empty output, which is what calling an address with
			// no code returns. Reading that as zero would report a confident
			// zero for a contract that was never asked.
			out[i].Reverted, out[i].Err = true, errBalanceMalformed
		default:
			out[i].Raw, out[i].Block, out[i].Err = new(big.Int).SetBytes(r.ret), executed, ""
		}
	}
	return nil
}

// Metadata implements ports.MetadataReader.
//
// It reads at latest because the answer is not tied to a height: it is cached
// for the lifetime of the store, and no response claims a block for it.
func (a *Adapter) Metadata(ctx context.Context, tokens []domain.TokenRef) ([]domain.TokenMeta, error) {
	const slotsPerToken = 3
	chunk := max(a.cfg.MulticallChunk/slotsPerToken, 1)

	parts := make([][]domain.TokenMeta, (len(tokens)+chunk-1)/chunk)
	errs := make([]error, len(parts))
	var wg sync.WaitGroup
	for i := range parts {
		wg.Go(func() {
			parts[i], errs[i] = a.readMetadata(ctx, tokens[i*chunk:min((i+1)*chunk, len(tokens))])
		})
	}
	wg.Wait()

	out := make([]domain.TokenMeta, 0, len(tokens))
	for _, p := range parts {
		out = append(out, p...)
	}
	return out, errors.Join(errs...)
}

func (a *Adapter) readMetadata(ctx context.Context, tokens []domain.TokenRef) ([]domain.TokenMeta, error) {
	calls := make([]mcCall, 0, len(tokens)*3)
	for _, t := range tokens {
		calls = append(calls,
			mcCall{target: t.Address, data: selSymbol},
			mcCall{target: t.Address, data: selName},
		)
		if t.Standard != domain.StandardERC721 {
			calls = append(calls, mcCall{target: t.Address, data: selDecimals})
		}
	}

	_, results, err := a.multicall(ctx, tagLatest, calls)
	switch {
	case err == nil:
	case errors.Is(err, rpc.ErrExecutionFailed):
		if len(tokens) == 1 {
			return []domain.TokenMeta{{Token: tokens[0].Address, Standard: tokens[0].Standard}}, nil
		}
		mid := len(tokens) / 2
		left, lerr := a.readMetadata(ctx, tokens[:mid])
		right, rerr := a.readMetadata(ctx, tokens[mid:])
		return append(left, right...), errors.Join(lerr, rerr)
	default:
		return nil, err
	}

	out := make([]domain.TokenMeta, len(tokens))
	for i, t := range tokens {
		m := domain.TokenMeta{Token: t.Address, Standard: t.Standard}
		if results[0].success {
			m.Symbol = abiString(results[0].ret)
		}
		if results[1].success {
			m.Name = abiString(results[1].ret)
		}
		results = results[2:]

		if t.Standard == domain.StandardERC721 {
			m.Resolved = m.Symbol != "" || m.Name != ""
		} else {
			// Without decimals an ERC-20 amount cannot be displayed, and a
			// default of 0 would be a wrong answer rather than a missing one.
			if r := results[0]; r.success && len(r.ret) == abiWord {
				if d, err := abiUint(r.ret, 0); err == nil && d <= 255 {
					m.Decimals, m.Resolved = uint8(d), true
				}
			}
			results = results[1:]
		}
		out[i] = m
	}
	return out, nil
}

// cleanText returns s if it is valid text, stripped of control characters, and
// empty otherwise. These strings are whatever a token's deployer chose.
func cleanText(raw []byte) string {
	if !utf8.Valid(raw) {
		return ""
	}
	return strings.TrimSpace(strings.Map(func(r rune) rune {
		if unicode.IsControl(r) {
			return -1
		}
		return r
	}, string(raw)))
}
