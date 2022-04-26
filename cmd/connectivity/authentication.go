package connectivity

import (
	"context"
	"fmt"
)

type bearerAuthentication struct {
	Token string
}

func (a *bearerAuthentication) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
	return map[string]string{
		"Authorization": fmt.Sprintf("Bearer %s", a.Token),
	}, nil
}

func (a *bearerAuthentication) RequireTransportSecurity() bool {
	return false
}

type basicAuthentication struct {
	Token string
}

func (a *basicAuthentication) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
	return map[string]string{
		"Authorization": fmt.Sprintf("Basic %s", a.Token),
	}, nil
}

func (a *basicAuthentication) RequireTransportSecurity() bool {
	return false
}
