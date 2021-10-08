package encryption

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"io/ioutil"
	"strings"
)

const (
	PrivateKeyServiceA string = "-----BEGIN RSA PRIVATE KEY-----\n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\n-----END RSA PRIVATE KEY-----"
	PublicKeyServiceA  string = "-----BEGIN PUBLIC KEY-----\nMIICIjANBgkqhkiG9w0BAQEFAAOCAg8AMIICCgKCAgEAhIrFgVfLlff3xIUU3are\njAYpzCSC1hU4HzORyG2e2Px4plo1SGeps0CZaTC4wOf8kU3vpNlhK/q8zBZusJBZ\nhoi9hZFnslyTK2oge3Dor8lwzR05NKGTorjZfNkDSTYfpWpbkpFeUrZe+9g99tY3\nPrk+YYVDJaDHJuTShauVSBOea7+EgYxFBuawvuriC/w7anic5z6HUHTHqW8sUd3x\ncQZCPuIeV1PmwXshdH8BJBbUoFVThppjYzohfMnEgO+XCaIkEd9q1C6ccuh9D7+m\n/brbubuFhM3piZWxggzyv3tY9+jf4G/gc8V836M6hcr1c8n+RrEatejHgC7msoT2\nWWx2Ai9PqzbpbcULejWFFr7pvzpDJmgdskBmKCHn/XK7XvpTB5IKxhzT3a6ldjpQ\nQ4/xnMMPcfUM6chAyLhUVdNvrz6KHY6BZLOQoAzaaW54FABidp2oDS+wK55DK9ty\nkOF9h/al07eMiWl0FQTj7CBfMXY9jon0E1w+PmuJEVVTgQlfrEPotNC/iFP5x7/p\nDISncB1FFOz2c0GCuGEA/buHOGRKl+iXNCoI/d56Q77GibBMgfNCSHK4J3JS06NZ\nOMLLjh0MJxZwmvSfYSFAznRFhXcJFcyZg3eJ5A3XLKszoWWrv4RKYPeEtABTE8wR\nZ7rN5G5paiEhybRerdTpeNcCAwEAAQ==\n-----END PUBLIC KEY-----"
	PrivateKeyServiceB string = "-----BEGIN RSA PRIVATE KEY-----\n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\n-----END RSA PRIVATE KEY-----"
	PublicKeyServiceB  string = "-----BEGIN PUBLIC KEY-----\nMIICIjANBgkqhkiG9w0BAQEFAAOCAg8AMIICCgKCAgEArxdIRdwymdDR1h1WEKG8\nWMAnP4RZhG9dHrplspITb0pER2E+NRiKzd9xPAW5XfgzSR5bguWvZjJ2XHz/rTpZ\nAPENLuxrx4S38dyEM2H/PVejC76OHUAkaNucLD+KMNhWX/KO+RDbJ3n57K6cNgV+\nbkZHXB+0mM/hcAJVbgviophvxH1hrjlcrwXOKTdlQi2d7QIHsQhkatLrlK3lZVyi\n+mlNbsnS8L91m9GKrvNGwPh0YX6/aVkZ9tAuKzD6Phicl4/B36mO+hKZGXqTr9r6\nT9LU1aqOOgwTuvcUAfCmFUNC58gOHR/0pgSxSA/ES5GyOGHl+m/o26b9QeJWpT4W\nico/tELUuBhRPkZPeeczVjcEVK8hCqNXU+QSWb/QWyVhzpYv6ovFNJzLAAC4iBnS\nLAE/y0L05Hc3XU8QY55zy+1e/Egx+lK9Bi4dWPWpWpHUYuFD/OVSqg2spGtlceAi\n8Ve3YWO3U4p89qhDZvd/eSKCWh1UthNSxkJ00Bzs69KuKypOfwyrowKHE7p7VgOa\nUcrtbhZhLFlMDwf1xuvVCTis3NxIOWmRauJXdKHaXnZTZejgc01yc+3TqaxBHRG9\npjZYFB9dc9CalmpHqi+86ERXI7J0189YP8E4MLvbe715Q9HB11NBgmx7gmCpbxSG\nFxeftT1n+y1HrzAdyyhQytMCAwEAAQ==\n-----END PUBLIC KEY-----"
	ServiceA           string = "serviceA"
	ServiceB           string = "serviceB"
)

func GenerateKey() []byte {
	c := 32
	b := make([]byte, c)
	_, err := rand.Read(b)
	if err != nil {
		return nil
	}
	return b
}

func SetPublicKeyAndPrivateKeyServiceA() error {
	// Public Key
	pubKeyRead := strings.NewReader(strings.Trim(PrivateKeyServiceA, "\t"))
	publicKey, err := ioutil.ReadAll(pubKeyRead)
	if err != nil {
		return err
	}

	blockPub, _ := pem.Decode(publicKey)
	pubKeyItf, err := x509.ParsePKIXPublicKey(blockPub.Bytes)
	if err != nil {
		return err
	}

	pubKey = pubKeyItf.(*rsa.PublicKey)

	//Private Key
	privKeyRead := strings.NewReader(strings.Trim(PrivateKeyServiceA, "\t"))
	privateKey, err := ioutil.ReadAll(privKeyRead)
	if err != nil {
		return err
	}

	blockPriv, _ := pem.Decode(privateKey)
	privKey, err = x509.ParsePKCS1PrivateKey(blockPriv.Bytes)
	if err != nil {
		return err
	}

	return nil
}

func SetPublicKeyAndPrivateKeyServiceB() error {
	// Public Key
	pubKeyRead := strings.NewReader(strings.Trim(PublicKeyServiceB, "\t"))
	publicKey, err := ioutil.ReadAll(pubKeyRead)
	if err != nil {
		return err
	}

	blockPub, _ := pem.Decode(publicKey)
	pubKeyItf, err := x509.ParsePKIXPublicKey(blockPub.Bytes)
	if err != nil {
		return err
	}

	pubKey = pubKeyItf.(*rsa.PublicKey)

	//Private Key
	privKeyRead := strings.NewReader(strings.Trim(PrivateKeyServiceB, "\t"))
	privateKey, err := ioutil.ReadAll(privKeyRead)
	if err != nil {
		return err
	}

	blockPriv, _ := pem.Decode(privateKey)
	privKey, err = x509.ParsePKCS1PrivateKey(blockPriv.Bytes)
	if err != nil {
		return err
	}

	return nil
}
