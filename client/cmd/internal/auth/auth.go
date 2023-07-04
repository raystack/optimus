package auth

import (
	"context"

	"github.com/goto/salt/log"
	"github.com/goto/salt/oidc"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"

	"github.com/goto/optimus/config"
)

type Auth struct {
	logger log.Logger
	cfg    *oauth2.Config
}

func NewAuth(logger log.Logger, authConfig config.Auth) *Auth {
	return &Auth{
		logger: logger,
		cfg:    toAuthConfig(authConfig),
	}
}

func (a Auth) GetToken(ctx context.Context) (*oauth2.Token, error) {
	token, err := RetrieveFromKeyring(a.cfg.ClientID)
	if err == nil {
		return token, nil
	}

	token, err = a.getTokenFromServer(ctx, a.cfg)
	if err == nil {
		err = StoreInKeyring(a.cfg.ClientID, token)
		if err != nil {
			a.logger.Debug("not able to save token in keyring")
		}
		return token, nil
	}
	return nil, err
}

func (Auth) getTokenFromServer(ctx context.Context, cfg *oauth2.Config) (*oauth2.Token, error) {
	source := oidc.NewTokenSource(ctx, cfg, cfg.ClientID)
	return source.Token()
}

func toAuthConfig(authConfig config.Auth) *oauth2.Config {
	callbackURL := "http://localhost:9090/auth/callback"
	cfg := &oauth2.Config{
		ClientID:     authConfig.ClientID,
		ClientSecret: authConfig.ClientSecret,
		Endpoint:     google.Endpoint,
		RedirectURL:  callbackURL,
		Scopes:       []string{"openid email"},
	}

	return cfg
}
