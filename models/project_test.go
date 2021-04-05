package models_test

import (
	"encoding/base64"
	"testing"

	"github.com/gtank/cryptopasta"
	"github.com/stretchr/testify/assert"

	"github.com/odpf/optimus/models"
)

func TestProjectSpec(t *testing.T) {
	t.Run("ProjectSecrets", func(t *testing.T) {
		tests := []struct {
			name string
			s    models.ProjectSpec
			want string
		}{
			{
				name: "should not serialize secrets when serializing project spec",
				s: models.ProjectSpec{
					Name: "test",
					Secret: models.ProjectSecrets{
						{
							Name:  "name",
							Value: "value",
						},
					},
				},
				want: "test, map[]",
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				if got := tt.s.String(); got != tt.want {
					t.Errorf("String() = %v, want %v", got, tt.want)
				}
			})
		}
	})
	t.Run("ApplicationHash", func(t *testing.T) {
		rawSecret := "super secret string"
		t.Run("should encrypt text correctly with hash", func(t *testing.T) {
			enc, err := models.NewApplicationSecret("test-hashtest-hashtest-hashzzzzz")
			assert.Nil(t, err)

			// encrypt secret
			cipher, err := cryptopasta.Encrypt([]byte(rawSecret), enc.GetKey())
			assert.Nil(t, err)

			dec, err := models.NewApplicationSecret("test-hashtest-hashtest-hashzzzzz")
			assert.Nil(t, err)

			// decrypt secret
			value, err := cryptopasta.Decrypt(cipher, dec.GetKey())
			assert.Nil(t, err)
			assert.Equal(t, rawSecret, string(value))
		})
		t.Run("should fail to decrypt with incorrect hash", func(t *testing.T) {
			enc, err := models.NewApplicationSecret("test-hashtest-hashtest-hashzzzzz")
			assert.Nil(t, err)

			// encrypt secret
			cipher, err := cryptopasta.Encrypt([]byte(rawSecret), enc.GetKey())
			assert.Nil(t, err)

			dec, err := models.NewApplicationSecret("zest-hashtest-hashtest-hashzzzzz")
			assert.Nil(t, err)

			// decrypt secret
			_, err = cryptopasta.Decrypt(cipher, dec.GetKey())
			assert.NotNil(t, err)
		})
		t.Run("should fail to encrypt if hash size is not atleast 32", func(t *testing.T) {
			_, err := models.NewApplicationSecret("test-hashtestzs")
			assert.NotNil(t, err)
		})
		t.Run("should encrypt text correctly with hash and return as base64", func(t *testing.T) {
			enc, err := models.NewApplicationSecret("test-hashtest-hashtest-hashzzzzz")
			assert.Nil(t, err)

			// encrypt secret
			cipher, err := cryptopasta.Encrypt([]byte(rawSecret), enc.GetKey())
			assert.Nil(t, err)

			// base64 it
			base64cipher := base64.StdEncoding.EncodeToString(cipher)

			// decode base64
			cipherAfterBase64Decoded, err := base64.StdEncoding.DecodeString(base64cipher)
			assert.Nil(t, err)
			assert.Equal(t, cipher, cipherAfterBase64Decoded)

			// prepare password for decrypt
			dec, err := models.NewApplicationSecret("test-hashtest-hashtest-hashzzzzz")
			assert.Nil(t, err)

			// decrypt secret
			value, err := cryptopasta.Decrypt(cipherAfterBase64Decoded, dec.GetKey())
			assert.Nil(t, err)
			assert.Equal(t, rawSecret, string(value))
		})
	})
}
