package connection

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

func (*bearerAuthentication) RequireTransportSecurity() bool {
	return false
}
