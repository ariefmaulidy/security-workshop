package encryption

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"io"
)

var (
	pubKey  *rsa.PublicKey
	privKey *rsa.PrivateKey
)

func EncryptAES(key string, input []byte) (string, error) {
	keyByte, err := hex.DecodeString(key)
	if err != nil {
		return "", err
	}

	block, err := aes.NewCipher(keyByte)
	if err != nil {
		return "", err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", err
	}
	nonce := make([]byte, gcm.NonceSize())
	_, err = io.ReadFull(rand.Reader, nonce)
	if err != nil {
		return "", err
	}

	cipherTextByte := gcm.Seal(nonce, nonce, input, nil)
	cipherText := base64.StdEncoding.EncodeToString(cipherTextByte)
	fmt.Println(cipherText)
	return cipherText, nil
}

func EncrpytRSA(recipient string, input []byte) (chiperText string, err error) {
	switch recipient {
	case ServiceA:
		err = SetPublicKeyAndPrivateKeyServiceA()
	case ServiceB:
		err = SetPublicKeyAndPrivateKeyServiceB()
	}

	if err != nil {
		return chiperText, err
	}

	ciphertext, err := rsa.EncryptOAEP(sha256.New(), rand.Reader, &privKey.PublicKey, input, nil)
	if err != nil {
		return "", err
	}

	return base64.StdEncoding.EncodeToString(ciphertext), nil
}
