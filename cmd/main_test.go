package cmd

import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/dynatrace-oss/dtctl/pkg/auth"
)

// guardAuthOAuthFlow is the package-wide test default for the OAuth boundary.
// It fails loudly so no cmd test can open a real browser or callback listener
// by forgetting the per-test fake.
type guardAuthOAuthFlow struct{}

func (guardAuthOAuthFlow) Start(context.Context) (*auth.TokenSet, error) {
	return nil, errors.New("test reached the OAuth boundary without installTestAuthOAuthFlow")
}

func (guardAuthOAuthFlow) GetUserInfo(string) (*auth.UserInfo, error) {
	return nil, errors.New("test reached the OAuth boundary without installTestAuthOAuthFlow")
}

// TestMain replaces the production OAuth-flow constructor before any test
// runs. Tests that must reach the boundary install a counting fake via
// installTestAuthOAuthFlow; its cleanup restores this guard, never the real
// browser flow.
func TestMain(m *testing.M) {
	authNewOAuthFlowFunc = func(*auth.OAuthConfig) (authOAuthFlow, error) {
		return guardAuthOAuthFlow{}, nil
	}
	os.Exit(m.Run())
}
