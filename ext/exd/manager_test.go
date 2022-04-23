package exd_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	tMock "github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/odpf/optimus/ext/exd"
	"github.com/odpf/optimus/mock"
)

type ManagerTestSuite struct {
	suite.Suite
}

func (m *ManagerTestSuite) TestInstall() {
	defaultParser := exd.ParseRegistry
	defer func() { exd.ParseRegistry = defaultParser }()
	defaultNewClient := exd.NewClientRegistry
	defer func() { exd.NewClientRegistry = defaultNewClient }()

	m.Run("should return error if remote path is empty", func() {
		ctx := context.Background()
		httpDoer := &mock.HTTPDoer{}
		manifester := &mock.Manifester{}
		installer := &mock.Installer{}

		manager, err := exd.NewManager(ctx, httpDoer, manifester, installer)
		if err != nil {
			panic(err)
		}

		var remotePath string

		actualErr := manager.Install(remotePath)

		m.Error(actualErr)
	})

	m.Run("should return error if validation is error", func() {
		manager := &exd.Manager{}
		remotePath := "gojek/optimus-extension-valor"

		actualErr := manager.Install(remotePath)

		m.Error(actualErr)
	})

	m.Run("should return error if error loading manifest", func() {
		ctx := context.Background()
		httpDoer := &mock.HTTPDoer{}
		installer := &mock.Installer{}

		manifester := &mock.Manifester{}
		manifester.On("Load", tMock.Anything).Return(nil, errors.New("random error"))

		manager, err := exd.NewManager(ctx, httpDoer, manifester, installer)
		if err != nil {
			panic(err)
		}

		remotePath := "gojek/optimus-extension-valor"

		actualErr := manager.Install(remotePath)

		m.Error(actualErr)
	})

	m.Run("should return error if error parsing remote path", func() {
		exd.ParseRegistry = []exd.Parser{
			func(remotePath string) (*exd.Metadata, error) {
				return nil, errors.New("parsing failed")
			},
		}

		manifester := &mock.Manifester{}
		manifester.On("Load", tMock.Anything).Return(&exd.Manifest{}, nil)

		ctx := context.Background()
		httpDoer := &mock.HTTPDoer{}
		installer := &mock.Installer{}
		manager, err := exd.NewManager(ctx, httpDoer, manifester, installer)
		if err != nil {
			panic(err)
		}

		remotePath := "gojek/optimus-extension-valor"

		actualErr := manager.Install(remotePath)

		m.Error(actualErr)
	})

	m.Run("should return error if no parser could recognize remote path", func() {
		exd.ParseRegistry = []exd.Parser{
			func(remotePath string) (*exd.Metadata, error) {
				return nil, exd.ErrUnrecognizedRemotePath
			},
		}

		manifester := &mock.Manifester{}
		manifester.On("Load", tMock.Anything).Return(&exd.Manifest{}, nil)

		ctx := context.Background()
		httpDoer := &mock.HTTPDoer{}
		installer := &mock.Installer{}
		manager, err := exd.NewManager(ctx, httpDoer, manifester, installer)
		if err != nil {
			panic(err)
		}

		remotePath := "gojek/optimus-extension-valor"

		actualErr := manager.Install(remotePath)

		m.Error(actualErr)
	})

	m.Run("should return error if error getting new client", func() {
		providerName := "testing"
		exd.ParseRegistry = []exd.Parser{
			func(remotePath string) (*exd.Metadata, error) {
				return &exd.Metadata{
					ProviderName: providerName,
				}, nil
			},
		}

		newClientFactory := &exd.NewClientFactory{}
		exd.NewClientRegistry = newClientFactory

		manifester := &mock.Manifester{}
		manifester.On("Load", tMock.Anything).Return(&exd.Manifest{}, nil)

		ctx := context.Background()
		httpDoer := &mock.HTTPDoer{}
		installer := &mock.Installer{}
		manager, err := exd.NewManager(ctx, httpDoer, manifester, installer)
		if err != nil {
			panic(err)
		}

		remotePath := "gojek/optimus-extension-valor"

		actualErr := manager.Install(remotePath)

		m.Error(actualErr)
	})

	m.Run("should return error if error when downloading", func() {
		providerName := "testing"
		exd.ParseRegistry = []exd.Parser{
			func(remotePath string) (*exd.Metadata, error) {
				return &exd.Metadata{
					ProviderName: providerName,
				}, nil
			},
		}

		client := &mock.Client{}
		client.On("Download", tMock.Anything).Return(nil, errors.New("random error"))
		newClientFactory := &exd.NewClientFactory{}
		newClientFactory.Add(providerName, func(ctx context.Context, httpDoer exd.HTTPDoer) (exd.Client, error) {
			return client, nil
		})
		exd.NewClientRegistry = newClientFactory

		manifester := &mock.Manifester{}
		manifester.On("Load", tMock.Anything).Return(&exd.Manifest{}, nil)

		ctx := context.Background()
		httpDoer := &mock.HTTPDoer{}
		installer := &mock.Installer{}
		manager, err := exd.NewManager(ctx, httpDoer, manifester, installer)
		if err != nil {
			panic(err)
		}

		remotePath := "gojek/optimus-extension-valor"

		actualErr := manager.Install(remotePath)

		m.Error(actualErr)
	})

	m.Run("should return error if error when preparing installation", func() {
		providerName := "testing"
		exd.ParseRegistry = []exd.Parser{
			func(remotePath string) (*exd.Metadata, error) {
				return &exd.Metadata{
					ProviderName: providerName,
				}, nil
			},
		}

		client := &mock.Client{}
		client.On("Download", tMock.Anything).Return([]byte{}, nil)
		newClientFactory := &exd.NewClientFactory{}
		newClientFactory.Add(providerName, func(ctx context.Context, httpDoer exd.HTTPDoer) (exd.Client, error) {
			return client, nil
		})
		exd.NewClientRegistry = newClientFactory

		manifester := &mock.Manifester{}
		manifester.On("Load", tMock.Anything).Return(&exd.Manifest{}, nil)

		installer := &mock.Installer{}
		installer.On("Prepare", tMock.Anything).Return(errors.New("random error"))

		ctx := context.Background()
		httpDoer := &mock.HTTPDoer{}
		manager, err := exd.NewManager(ctx, httpDoer, manifester, installer)
		if err != nil {
			panic(err)
		}

		remotePath := "gojek/optimus-extension-valor"

		actualErr := manager.Install(remotePath)

		m.Error(actualErr)
	})

	m.Run("should return error if error when executing installation", func() {
		providerName := "testing"
		exd.ParseRegistry = []exd.Parser{
			func(remotePath string) (*exd.Metadata, error) {
				return &exd.Metadata{
					ProviderName: providerName,
				}, nil
			},
		}

		client := &mock.Client{}
		client.On("Download", tMock.Anything).Return([]byte{}, nil)
		newClientFactory := &exd.NewClientFactory{}
		newClientFactory.Add(providerName, func(ctx context.Context, httpDoer exd.HTTPDoer) (exd.Client, error) {
			return client, nil
		})
		exd.NewClientRegistry = newClientFactory

		manifester := &mock.Manifester{}
		manifester.On("Load", tMock.Anything).Return(&exd.Manifest{}, nil)

		installer := &mock.Installer{}
		installer.On("Prepare", tMock.Anything).Return(nil)
		installer.On("Install", tMock.Anything, tMock.Anything).Return(errors.New("random error"))

		ctx := context.Background()
		httpDoer := &mock.HTTPDoer{}
		manager, err := exd.NewManager(ctx, httpDoer, manifester, installer)
		if err != nil {
			panic(err)
		}

		remotePath := "gojek/optimus-extension-valor"

		actualErr := manager.Install(remotePath)

		m.Error(actualErr)
	})

	m.Run("should update manifest and return nil if no error is encountered", func() {
		providerName := "testing"
		exd.ParseRegistry = []exd.Parser{
			func(remotePath string) (*exd.Metadata, error) {
				return &exd.Metadata{
					ProviderName: providerName,
				}, nil
			},
		}

		client := &mock.Client{}
		client.On("Download", tMock.Anything).Return([]byte{}, nil)
		newClientFactory := &exd.NewClientFactory{}
		newClientFactory.Add(providerName, func(ctx context.Context, httpDoer exd.HTTPDoer) (exd.Client, error) {
			return client, nil
		})
		exd.NewClientRegistry = newClientFactory

		manifester := &mock.Manifester{}
		manifester.On("Load", tMock.Anything).Return(&exd.Manifest{}, nil)
		manifester.On("Flush", tMock.Anything, tMock.Anything).Return(nil)
		defer manifester.AssertCalled(m.T(), "Flush", tMock.Anything, tMock.Anything)

		installer := &mock.Installer{}
		installer.On("Prepare", tMock.Anything).Return(nil)
		installer.On("Install", tMock.Anything, tMock.Anything).Return(nil)

		ctx := context.Background()
		httpDoer := &mock.HTTPDoer{}
		manager, err := exd.NewManager(ctx, httpDoer, manifester, installer)
		if err != nil {
			panic(err)
		}
		remotePath := "gojek/optimus-extension-valor"

		actualErr := manager.Install(remotePath)

		m.NoError(actualErr)
	})
}

func TestManager(t *testing.T) {
	suite.Run(t, &ManagerTestSuite{})
}

func TestNewManager(t *testing.T) {
	t.Run("should return nil and error if context is nil", func(t *testing.T) {
		var ctx context.Context
		httpDoer := &mock.HTTPDoer{}
		manifester := &mock.Manifester{}
		installer := &mock.Installer{}

		actualManager, actualErr := exd.NewManager(ctx, httpDoer, manifester, installer)

		assert.Nil(t, actualManager)
		assert.Error(t, actualErr)
	})

	t.Run("should return nil and error if http doer is nil", func(t *testing.T) {
		ctx := context.Background()
		var httpDoer exd.HTTPDoer
		manifester := &mock.Manifester{}
		installer := &mock.Installer{}

		actualManager, actualErr := exd.NewManager(ctx, httpDoer, manifester, installer)

		assert.Nil(t, actualManager)
		assert.Error(t, actualErr)
	})

	t.Run("should return nil and error if manifester is nil", func(t *testing.T) {
		ctx := context.Background()
		httpDoer := &mock.HTTPDoer{}
		var manifester exd.Manifester
		installer := &mock.Installer{}

		actualManager, actualErr := exd.NewManager(ctx, httpDoer, manifester, installer)

		assert.Nil(t, actualManager)
		assert.Error(t, actualErr)
	})

	t.Run("should return nil and error if installer is nil", func(t *testing.T) {
		ctx := context.Background()
		httpDoer := &mock.HTTPDoer{}
		manifester := &mock.Manifester{}
		var installer exd.Installer

		actualManager, actualErr := exd.NewManager(ctx, httpDoer, manifester, installer)

		assert.Nil(t, actualManager)
		assert.Error(t, actualErr)
	})

	t.Run("should return manager and nil if no error encountered", func(t *testing.T) {
		ctx := context.Background()
		httpDoer := &mock.HTTPDoer{}
		manifester := &mock.Manifester{}
		installer := &mock.Installer{}

		actualManager, actualErr := exd.NewManager(ctx, httpDoer, manifester, installer)

		assert.NotNil(t, actualManager)
		assert.NoError(t, actualErr)
	})
}
