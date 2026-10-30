package chains

import (
	"context"
	"errors"
	"testing"

	"tokendiscovery/internal/core/domain"
)

type stub struct{ info domain.ChainInfo }

func (s stub) Chain() domain.ChainInfo { return s.info }
func (s stub) Watermarks(context.Context) (domain.Watermarks, error) {
	return domain.Watermarks{}, errors.New("unused")
}
func (s stub) Header(context.Context, uint64) (domain.BlockRef, error) {
	return domain.BlockRef{}, errors.New("unused")
}
func (s stub) ScanAccountTokens(context.Context, uint64, uint64) (domain.AccountTokenBatch, error) {
	return domain.AccountTokenBatch{}, errors.New("unused")
}
func (s stub) Balances(context.Context, domain.Address, []domain.TokenRef, uint64) ([]domain.Balance, error) {
	return nil, errors.New("unused")
}
func (s stub) Metadata(context.Context, []domain.TokenRef) ([]domain.TokenMeta, error) {
	return nil, errors.New("unused")
}

func chain(slug string, id uint64) stub {
	return stub{info: domain.ChainInfo{
		Slug: slug, ChainID: id, StartBlock: 1,
		Standards:  []domain.Standard{domain.StandardERC20},
		Watermarks: domain.WatermarkConsensusTags,
	}}
}

func TestRegistryResolves(t *testing.T) {
	r, err := NewRegistry(chain("polygon", 137), chain("ethereum", 1))
	if err != nil {
		t.Fatalf("NewRegistry: %v", err)
	}

	// A URL path is not a place to get subtly wrong, so lookup ignores case.
	for _, slug := range []string{"ethereum", "Ethereum", "ETHEREUM"} {
		a, ok := r.Adapter(slug)
		if !ok || a.Chain().ChainID != 1 {
			t.Errorf("Adapter(%q) = %v, %v", slug, a, ok)
		}
	}
	if _, ok := r.Adapter("dogecoin"); ok {
		t.Error("an unconfigured chain resolved")
	}
	if a, ok := r.ByID(137); !ok || a.Chain().Slug != "polygon" {
		t.Errorf("ByID(137) = %v, %v", a, ok)
	}

	// Listed in slug order, so /v1/chains is stable between calls and the
	// response is cacheable.
	chains := r.Chains()
	if len(chains) != 2 || chains[0].Slug != "ethereum" || chains[1].Slug != "polygon" {
		t.Fatalf("Chains() = %+v, want slug order", chains)
	}
	// The returned slice must be a copy: a caller mutating it must not be able
	// to rewrite the service's configuration.
	chains[0].Slug = "mutated"
	if r.Chains()[0].Slug != "ethereum" {
		t.Error("Chains() handed out the registry's own slice")
	}
}

func TestRegistryRejectsAmbiguity(t *testing.T) {
	// A duplicate would make Adapter return an arbitrary one of the two, which
	// is a configuration error worth failing startup over.
	if _, err := NewRegistry(chain("ethereum", 1), chain("ethereum", 2)); err == nil {
		t.Error("duplicate slug was accepted")
	}
	if _, err := NewRegistry(chain("ethereum", 1), chain("eth-mainnet", 1)); err == nil {
		t.Error("duplicate chain id was accepted")
	}
	if _, err := NewRegistry(chain("", 1)); err == nil {
		t.Error("a chain with no slug was accepted")
	}
	// An empty registry is valid; it simply serves no chains.
	r, err := NewRegistry()
	if err != nil {
		t.Fatalf("NewRegistry() with no chains: %v", err)
	}
	if len(r.Chains()) != 0 {
		t.Error("an empty registry listed chains")
	}
}
