package manager

import (
	"strings"

	"github.com/sirrobot01/decypharr/internal/config"
	debrid "github.com/sirrobot01/decypharr/pkg/debrid/common"
)

// DebridClientInfo describes one debrid client as the runtime actually holds
// it, not as config declares it.
//
// RegistryKey is deliberately separate from Name. Clients are stored under
// config's Name field, and provider selection compares an arr's
// selected_debrid against that same string with ==. A case or spelling
// difference therefore silently fails to match, and nothing in config
// inspection reveals it — both sides look correct in isolation.
type DebridClientInfo struct {
	RegistryKey      string `json:"registry_key"`
	Name             string `json:"name"`
	Provider         string `json:"provider"`
	DownloadUncached bool   `json:"download_uncached"`
}

// DebridChainDiagnosis reports the provider chain a torrent add would actually
// walk for a given arr, and why it came out that way.
//
// A single-provider chain is the failure being diagnosed: with fallback enabled
// the chain should hold every registered client, so an add that exhausts one
// provider moves to the next. If the chain is short, the reason is here rather
// than in config.
type DebridChainDiagnosis struct {
	Arr               string             `json:"arr"`
	ArrFound          bool               `json:"arr_found"`
	SelectedDebrid    string             `json:"selected_debrid"`
	FallbackOnFailure bool               `json:"fallback_on_failure"`
	SelectedFound     bool               `json:"selected_found"`
	ConfiguredCount   int                `json:"configured_count"`
	RegisteredCount   int                `json:"registered_count"`
	Registered        []DebridClientInfo `json:"registered"`
	ConfiguredOnly    []string           `json:"configured_but_not_registered"`
	Chain             []string           `json:"chain"`
	// ChainIsSingleProvider is the headline. When fallback is enabled and more
	// than one client is registered, a true here means provider selection
	// silently narrowed the chain.
	ChainIsSingleProvider bool `json:"chain_is_single_provider"`
}

// RegisteredDebridClients lists the clients the manager actually holds.
//
// A client whose construction failed is skipped at startup with a logged error
// and is simply absent thereafter, so config listing a provider is not evidence
// that provider can be used.
func (m *Manager) RegisteredDebridClients() []DebridClientInfo {
	clients := m.FilterDebrid(func(debrid.Client) bool { return true })
	infos := make([]DebridClientInfo, 0, len(clients))
	for _, client := range clients {
		dc := client.Config()
		infos = append(infos, DebridClientInfo{
			RegistryKey:      dc.Name,
			Name:             dc.Name,
			Provider:         dc.Provider,
			DownloadUncached: dc.DownloadsUncached(),
		})
	}
	return infos
}

// DiagnoseDebridChain resolves the provider chain for an arr exactly as an add
// would, using the same selection function, so the answer cannot drift from the
// real behaviour.
func (m *Manager) DiagnoseDebridChain(arrName string) *DebridChainDiagnosis {
	diagnosis := &DebridChainDiagnosis{
		Arr:            arrName,
		Registered:     m.RegisteredDebridClients(),
		ConfiguredOnly: []string{},
		Chain:          []string{},
	}
	diagnosis.RegisteredCount = len(diagnosis.Registered)

	registered := make(map[string]struct{}, len(diagnosis.Registered))
	for _, info := range diagnosis.Registered {
		registered[info.RegistryKey] = struct{}{}
	}
	for _, dc := range config.Get().Debrids {
		diagnosis.ConfiguredCount++
		if _, ok := registered[dc.Name]; !ok {
			diagnosis.ConfiguredOnly = append(diagnosis.ConfiguredOnly, dc.Name)
		}
	}

	if a := m.Arr().Get(arrName); a != nil {
		diagnosis.ArrFound = true
		diagnosis.SelectedDebrid = a.SelectedDebrid
		diagnosis.FallbackOnFailure = a.FallbackOnFailure
	}

	// The same call the add path makes, so this cannot report a chain the real
	// code would not build.
	clients, selectedFound := m.debridClientsForRequest(diagnosis.SelectedDebrid, diagnosis.FallbackOnFailure)
	diagnosis.SelectedFound = selectedFound
	for _, client := range clients {
		dc := client.Config()
		name := dc.Name
		if name == "" {
			name = dc.Provider
		}
		diagnosis.Chain = append(diagnosis.Chain, name)
	}
	diagnosis.ChainIsSingleProvider = len(diagnosis.Chain) <= 1

	return diagnosis
}

// MatchesSelectedDebrid reports whether a selection string would match a
// registered client under the exact comparison provider selection uses.
// Exposed so a caller can tell "not configured" apart from "configured but
// spelled differently", which look identical from outside.
func (info DebridClientInfo) MatchesSelectedDebrid(selected string) bool {
	return info.RegistryKey == selected
}

// EqualFoldSelectedDebrid reports a case-insensitive match. When this is true
// while MatchesSelectedDebrid is false, selection is failing purely on case.
func (info DebridClientInfo) EqualFoldSelectedDebrid(selected string) bool {
	return strings.EqualFold(info.RegistryKey, selected)
}
