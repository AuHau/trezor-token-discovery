// Package chains holds the registry that resolves a chain slug to its adapter.
//
// It is the other half of the modularity the assignment asks for: the API is
// written against "whatever chains are configured" rather than a hardcoded
// network, so a second EVM network is a config block and a non-EVM one is a new
// ChainAdapter behind the same port.
package chains

import (
	"fmt"
	"sort"
	"strings"

	"tokendiscovery/internal/core/domain"
	"tokendiscovery/internal/core/ports"
)

// Registry is an immutable set of configured chains, keyed by slug.
//
// Immutable on purpose: chains come from configuration read at startup, so
// there is no add-at-runtime path to make concurrency-safe, and the read path
// needs no lock.
type Registry struct {
	bySlug map[string]ports.ChainAdapter
	byID   map[uint64]ports.ChainAdapter
	infos  []domain.ChainInfo
}

// NewRegistry builds a registry, rejecting duplicate slugs and chain IDs —
// either would make `Adapter` return an arbitrary one of the two.
func NewRegistry(adapters ...ports.ChainAdapter) (*Registry, error) {
	r := &Registry{
		bySlug: make(map[string]ports.ChainAdapter, len(adapters)),
		byID:   make(map[uint64]ports.ChainAdapter, len(adapters)),
	}
	for _, a := range adapters {
		info := a.Chain()
		slug := strings.ToLower(info.Slug)
		if slug == "" {
			return nil, fmt.Errorf("chains: chain %d has no slug", info.ChainID)
		}
		if _, dup := r.bySlug[slug]; dup {
			return nil, fmt.Errorf("chains: duplicate slug %q", slug)
		}
		if _, dup := r.byID[info.ChainID]; dup {
			return nil, fmt.Errorf("chains: duplicate chain id %d", info.ChainID)
		}
		r.bySlug[slug] = a
		r.byID[info.ChainID] = a
		r.infos = append(r.infos, info)
	}
	sort.Slice(r.infos, func(i, j int) bool { return r.infos[i].Slug < r.infos[j].Slug })
	return r, nil
}

// Adapter resolves a slug. Lookup is case-insensitive so a URL path is not a
// place users can get subtly wrong.
func (r *Registry) Adapter(slug string) (ports.ChainAdapter, bool) {
	a, ok := r.bySlug[strings.ToLower(slug)]
	return a, ok
}

// ByID resolves a chain ID, for callers that carry one rather than a slug.
func (r *Registry) ByID(chainID uint64) (ports.ChainAdapter, bool) {
	a, ok := r.byID[chainID]
	return a, ok
}

// Chains lists the configured chains in slug order.
func (r *Registry) Chains() []domain.ChainInfo {
	out := make([]domain.ChainInfo, len(r.infos))
	copy(out, r.infos)
	return out
}

var _ ports.Registry = (*Registry)(nil)
