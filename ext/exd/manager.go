package exd

import (
	"context"
	"errors"
	"fmt"
	"time"
)

var ErrUnrecognizedRemotePath = errors.New("remote path is not recognized")

const ExtensionDir = ".optimus/extensions"

type Manager struct {
	ctx        context.Context
	httpDoer   HTTPDoer
	manifester Manifester
	installer  Installer
}

func NewManager(ctx context.Context, httpDoer HTTPDoer, manifester Manifester, installer Installer) (*Manager, error) {
	if err := validate(ctx, httpDoer, manifester, installer); err != nil {
		return nil, fmt.Errorf("error validating parameter: %w", err)
	}
	if installer == nil {
		return nil, errors.New("installer is nil")
	}
	return &Manager{
		ctx:        ctx,
		httpDoer:   httpDoer,
		manifester: manifester,
		installer:  installer,
	}, nil
}

func (m *Manager) Install(sourcePath string) error {
	if sourcePath == "" {
		return errors.New("source path is empty")
	}
	if err := validate(m.ctx, m.httpDoer, m.manifester, m.installer); err != nil {
		return fmt.Errorf("error validating installation: %w", err)
	}
	manifest, err := m.manifester.Load(ExtensionDir)
	if err != nil {
		return fmt.Errorf("error loading manifest: %w", err)
	}
	metadata, err := m.getMetadata(sourcePath)
	if err != nil {
		return fmt.Errorf("error getting metadata: %w", err)
	}
	client, err := m.getClient(metadata.ProviderName)
	if err != nil {
		return fmt.Errorf("error getting client: %w", err)
	}
	asset, err := client.Download(metadata)
	if err != nil {
		return fmt.Errorf("error downloading: %w", err)
	}
	if err := m.installer.Prepare(metadata); err != nil {
		return fmt.Errorf("error preparing installation: %w", err)
	}
	if err := m.installer.Install(asset, metadata); err != nil {
		return fmt.Errorf("error installing asset: %w", err)
	}
	return m.updateManifest(manifest, metadata)
}

func (m *Manager) updateManifest(manifest *Manifest, metadata *Metadata) error {
	manifest.Metadatas = append(manifest.Metadatas, metadata)
	manifest.UpdatedAt = time.Now()
	return m.manifester.Flush(manifest, ExtensionDir)
}

func (m *Manager) getClient(providerName string) (Client, error) {
	newClient, err := NewClientRegistry.Get(providerName)
	if err != nil {
		return nil, fmt.Errorf("error getting new client: %w", err)
	}
	return newClient(m.ctx, m.httpDoer)
}

func (m *Manager) getMetadata(sourcePath string) (*Metadata, error) {
	var metadata *Metadata
	for _, parseFn := range ParseRegistry {
		mtdt, err := parseFn(sourcePath)
		if errors.Is(err, ErrUnrecognizedRemotePath) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("errors parsing source path: %w", err)
		}
		if mtdt != nil {
			metadata = mtdt
			break
		}
	}
	if metadata == nil {
		return nil, fmt.Errorf("[%s] is not recognized", sourcePath)
	}
	return metadata, nil
}

func validate(ctx context.Context, httpDoer HTTPDoer, manifester Manifester, installer Installer) error {
	if ctx == nil {
		return errors.New("ctx is nil")
	}
	if httpDoer == nil {
		return errors.New("http doer is nil")
	}
	if manifester == nil {
		return errors.New("manifester is nils")
	}
	if installer == nil {
		return errors.New("installer is nil")
	}
	return nil
}
