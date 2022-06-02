package model

// AssetOperator is a contract to operate on extension asset
type AssetOperator interface {
	// Prepare does preparation before any operation.
	// Such preparation can be in the form of, but not limited to, creating local directory.
	Prepare(localDirPath string) error
	// Install installs an asset according to the specified tag name.
	Install(asset []byte, tagName string) error
	// Uninstall uninstalls asset specified by the tag names.
	Uninstall(tagNames ...string) error
	// Run runs extension specified by the tag name.
	// Arguments can be sent to the extension through the args parameter.
	Run(tagName string, args ...string) error
}
