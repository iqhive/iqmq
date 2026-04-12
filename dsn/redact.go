package dsn

import "strings"

// credentialNames are the substrings that make a DSN parameter's -- or an
// environment variable's -- name a credential.
//
// It is a substring match and not an exact one on purpose: a driver that adds
// "apisecret" or "producerpassword" tomorrow is covered without this list
// changing, which is the failure mode a name-by-name list has. Over-matching
// costs an over-redacted line; under-matching is a security defect.
var credentialNames = [...]string{
	"password", "passwd", "secret", "token", "credential",
	"apikey", "privatekey", "passphrase", "authparams", "nkey",
}

// IsCredentialName reports whether a parameter or variable of this name
// carries a credential, case-insensitively.
//
// It is the single answer to that question in this repository: [Config.String]
// redacts the parameters it names, and a tool that prints a DSN or an
// environment uses it rather than a list of its own.
func IsCredentialName(name string) bool {
	lower := strings.ToLower(name)
	for _, n := range credentialNames {
		if strings.Contains(lower, n) {
			return true
		}
	}
	return false
}
