package base

import (
	hplugin "github.com/hashicorp/go-plugin"
	"go.opentelemetry.io/otel"
)

const (
	// ProtocolVersion is the version that must match between core
	// and plugins. This should be bumped whenever a change happens in
	// one or the other that makes it so that they can't safely communicate.
	// This could be adding a new interface value, methods, etc.
	ProtocolVersion = 1

	// Magic values
	// should always remain constant
	MagicCookieKey   = "OP_PLUGIN_MAGIC_COOKIE"
	MagicCookieValue = "ksxR4BqCT81whVF2dVEUpYZXwM3pazSkP4IbVc6f2Kns57ypp2c0z0GzQNMdHSUk"
)

var (
	// Handshake is used to just do a basic handshake between
	// a plugin and host. If the handshake fails, a user friendly error is shown.
	// This prevents users from executing bad plugins or executing a plugin
	// directory. It is a UX feature, not a security feature.
	Handshake = hplugin.HandshakeConfig{
		// Need to be set as needed
		ProtocolVersion: ProtocolVersion,

		// Magic cookie key and value are just there to make sure you want to connect
		// with optimus core, this is not authentication
		MagicCookieKey:   MagicCookieKey,
		MagicCookieValue: MagicCookieValue,
	}

	Tracer = otel.Tracer("optimus/plugin")
)
