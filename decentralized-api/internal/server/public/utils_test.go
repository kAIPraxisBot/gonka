package public

import (
	"encoding/base64"

	"github.com/cosmos/cosmos-sdk/crypto/keys/secp256k1"
)

// testKey wraps a secp256k1 private key for testing
type testKey struct {
	key *secp256k1.PrivKey
}

func newTestKey() *testKey {
	return &testKey{key: secp256k1.GenPrivKey()}
}

func (t *testKey) GetPubKeyBase64() string {
	return base64.StdEncoding.EncodeToString(t.key.PubKey().Bytes())
}

func (t *testKey) SignBytes(msg []byte) (string, error) {
	signature, err := t.key.Sign(msg)
	if err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(signature), nil
}
