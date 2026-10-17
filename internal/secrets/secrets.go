// Package secrets resolves the secret references in the config
package secrets

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
)

// Exit code of `security` when the item doesn't exist
const keychainMissing = 44

// An env reference may name only a variable with this prefix
// so a reference can't point at a token the shell holds for something else
const EnvPrefix = "GATEREACH_"

const hint = "\nuse keychain:<item> or env:" + EnvPrefix + "<name>"

// Checks the form of a reference without looking anything up
// 1. Empty means no password
// 2. keychain:<item> with a non-empty item
// 3. env:<variable> with a variable that starts with GATEREACH_
// Errors never repeat the reference since a password typed in its place would land in a log
func Check(ref string) error {
	if ref == "" {
		return nil
	}
	kind, name, _ := strings.Cut(ref, ":")
	switch {
	case kind != "keychain" && kind != "env":
		return errors.New("unsupported secret reference" + hint)
	case name == "":
		// `security` given an empty item name returns the first item it finds
		return errors.New("secret reference without a name" + hint)
	case kind == "env" && !strings.HasPrefix(name, EnvPrefix):
		return errors.New("env secret must name a " + EnvPrefix + " variable" + hint)
	}
	return nil
}

// The config file holds only a reference and never the secret
// 1. keychain:<item> → macOS Keychain
// 2. env:<variable> → environment variable
// The value goes only into the driver settings so it never lands in a file or a command argument
// `security` stops when ctx ends even while a Keychain prompt is open
func Resolve(ctx context.Context, ref string) (string, error) {
	if err := Check(ref); err != nil || ref == "" {
		return "", err
	}
	kind, name, _ := strings.Cut(ref, ":")
	if kind == "env" {
		v, ok := os.LookupEnv(name)
		if !ok {
			return "", fmt.Errorf("no environment variable: %s", name)
		}
		return v, nil
	}
	out, err := exec.CommandContext(ctx, "security", "find-generic-password", "-s", name, "-w").Output()
	var exit *exec.ExitError
	switch {
	case err == nil:
		// `security` ends the value with one newline that isn't part of it
		return strings.TrimSuffix(string(out), "\n"), nil
	case errors.As(err, &exit) && exit.ExitCode() == keychainMissing:
		return "", fmt.Errorf("no keychain item: %s", name)
	}
	return "", fmt.Errorf("keychain lookup failed: %s: %w", name, err)
}
