package connection

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"time"

	"github.com/raystack/salt/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/raystack/optimus/client/cmd/internal/auth"
	"github.com/raystack/optimus/config"
)

const authTimeout = time.Minute * 1

type Secure struct {
	l          log.Logger
	authConfig config.Auth
}

func NewSecure(l log.Logger, cfg *config.ClientConfig) *Secure {
	return &Secure{
		l:          l,
		authConfig: cfg.Auth,
	}
}

func (s *Secure) Create(host string) (*grpc.ClientConn, error) {
	ctx, dialCancel := context.WithTimeout(context.Background(), optimusDialTimeout)
	defer dialCancel()

	opts, err := s.getOptionsWithAuth()
	if err != nil {
		return nil, err
	}

	conn, err := grpc.DialContext(ctx, host, opts...)
	if errors.Is(err, context.DeadlineExceeded) {
		err = errServerNotReachable(host)
	}

	return conn, err
}

func (s *Secure) getOptionsWithAuth() ([]grpc.DialOption, error) {
	if s.authConfig.ClientID == "" || s.authConfig.ClientSecret == "" {
		return nil, errors.New("invalid auth configuration, clientID or clientSecret is empty")
	}

	// setup https connection
	tlsCredentials, err := loadTLSCredentials()
	if err != nil {
		return nil, err
	}

	opts := append(defaultDialOptions(), grpc.WithTransportCredentials(tlsCredentials))

	// add the token for authentication
	a := auth.NewAuth(s.l, s.authConfig)

	ctx, dialCancel := context.WithTimeout(context.Background(), authTimeout)
	defer dialCancel()

	token, err := a.GetToken(ctx)
	if err != nil {
		return nil, err
	}
	if token == nil {
		return nil, errors.New("unable to get valid token")
	}

	opts = append(opts, grpc.WithPerRPCCredentials(&bearerAuthentication{
		Token: token.AccessToken,
	}))

	return opts, nil
}

func loadTLSCredentials() (credentials.TransportCredentials, error) {
	certPool, err := x509.SystemCertPool()
	if err != nil {
		return nil, fmt.Errorf("unable to read system certs")
	}

	tlsConfig := &tls.Config{
		RootCAs:    certPool,
		MinVersion: tls.VersionTLS12,
	}
	return credentials.NewTLS(tlsConfig), nil
}
