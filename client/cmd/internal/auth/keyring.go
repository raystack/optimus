package auth

import (
	"encoding/json"
	"errors"

	"github.com/zalando/go-keyring"
	"golang.org/x/oauth2"
)

const (
	keyringService = "optimus"
)

func RetrieveFromKeyring(clientID string) (*oauth2.Token, error) {
	tokenStr, err := keyring.Get(keyringService, clientID)
	if err != nil {
		return nil, err
	}

	var token oauth2.Token
	if err := json.Unmarshal([]byte(tokenStr), &token); err != nil {
		return nil, err
	}

	if !token.Valid() {
		return nil, errors.New("token is not valid")
	}

	return &token, err
}

func StoreInKeyring(clientID string, t *oauth2.Token) error {
	tokenBytes, err := json.Marshal(t)
	if err != nil {
		return err
	}

	tokenStr := string(tokenBytes)
	return keyring.Set(keyringService, clientID, tokenStr)
}
