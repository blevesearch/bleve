//  Copyright (c) 2023 Couchbase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// 		http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package registry

import (
	"fmt"
	"sync"

	"github.com/blevesearch/bleve/v2/analysis"
)

var (
	ErrHookNotRegistered = fmt.Errorf("hook not registered")
	ErrHookAlreadyExists = fmt.Errorf("hook already exists")
)

// Analysis hook signature
//
// Analysis hook will be invoked at indexing and search analysis time.
// Analysis hook can be specified as the analyzer in the index mapping.
//
//	returns:
//		`tokenStream`: result of analysis
//		`skip` (bool): A way for embedder to tell bleve that hook blew up and
//			bleve should skip indexing/searching the value.
//
// * This hook based mechanism is a way for the embedder to register its own
// analysis logic with bleve, without having to modify bleve's code.
//
// * Using this, Embedder can create complex analysis pipelines,
// which may involve custom/niche analysis components (char filters, tokenizers,
// token filters), network calls to other services, or may involve running
// user submitted code.
//
// Note:
//   - Bleve won't handle errors/timeouts in the callback.
//   - In case, embedder's analysis part of the callback errored (or timedout),
//     callback can error and bleve will skip indexing/searching the value.
type TokensAnalyzerHook func([]byte) (analysis.TokenStream, error)

// Concurrent safe registry of tokens analyzer hooks.
type TokensAnalyzerHooks struct {
	m     *sync.RWMutex                 // protect following fields
	hooks map[string]TokensAnalyzerHook // Hook Identifier -> Hook
}

func NewTokensAnalyzerHooks() *TokensAnalyzerHooks {
	return &TokensAnalyzerHooks{
		m:     &sync.RWMutex{},
		hooks: make(map[string]TokensAnalyzerHook),
	}
}

func RegisterTokensAnalyzerHook(name string, hook TokensAnalyzerHook) error {
	// check if hook already exists
	tahs.m.RLock()
	_, exists := tahs.hooks[name]
	if exists {
		tahs.m.RUnlock()
		return ErrHookAlreadyExists
	}
	tahs.m.RUnlock()

	// update the registry
	tahs.m.Lock()
	tahs.hooks[name] = hook
	tahs.m.Unlock()

	return nil
}

// todo: add comment (embedder must make sure that it is not deregistering an active hook)
func DeregisterTokensAnalyzerHook(name string) {
	// Early exit if hook doesn't exist
	tahs.m.RLock()
	_, exists := tahs.hooks[name]
	if !exists {
		tahs.m.RUnlock()
		return
	}
	tahs.m.RUnlock()

	// update the registry
	tahs.m.Lock()
	delete(tahs.hooks, name)
	tahs.m.Unlock()
}

func GetTokensAnalyzerHook(name string) (TokensAnalyzerHook, error) {
	tahs.m.RLock()
	hook, exists := tahs.hooks[name]
	tahs.m.RUnlock()
	if exists {
		return hook, nil
	}

	return nil, ErrHookNotRegistered
}

// -----------------------------------------------------------------------------
// Hook to Analyzer adapter

func HookTokensAnalyzerBuild(name string, config map[string]interface{},
	cache *Cache) (interface{}, error) {
	_, err := GetTokensAnalyzerHook(name)
	if err != nil {
		return nil, err
	}

	return NewDefaultTokensAnalyzer(name), nil
}

type DefaultTokensAnalyzer struct {
	name string
}

func NewDefaultTokensAnalyzer(name string) *DefaultTokensAnalyzer {
	return &DefaultTokensAnalyzer{
		name: name,
	}
}

func (a *DefaultTokensAnalyzer) Type() string {
	return analysis.HookTokensAnalyzerType
}

func (a *DefaultTokensAnalyzer) Analyze(input []byte) interface{} {
	hook, err := GetTokensAnalyzerHook(a.name)
	if err != nil {
		return analysis.HookTokensAnalyzer{
			Tokens: nil,
			Err:    err,
		}
	}

	tokens, err := hook(input)
	return analysis.HookTokensAnalyzer{
		Tokens: tokens,
		Err:    err,
	}
}
