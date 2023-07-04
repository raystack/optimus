package connection

import (
	"context"
	"errors"

	"github.com/goto/salt/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Insecure struct {
	l log.Logger
}

func NewInsecure(l log.Logger) *Insecure {
	return &Insecure{
		l: l,
	}
}

func (*Insecure) Create(host string) (*grpc.ClientConn, error) {
	ctx, dialCancel := context.WithTimeout(context.Background(), optimusDialTimeout)
	defer dialCancel()

	opts := append(defaultDialOptions(), grpc.WithTransportCredentials(insecure.NewCredentials()))

	conn, err := grpc.DialContext(ctx, host, opts...)
	if errors.Is(err, context.DeadlineExceeded) {
		err = errServerNotReachable(host)
	}

	return conn, err
}
