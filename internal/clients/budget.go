package clients

import (
	"errors"
	"math"
	"strconv"
	"strings"
)

var errBudgetForm = errors.New("bad --max form")

// Athena's byte suffixes in powers of 1000 from the largest
var byteUnits = []struct {
	suffix string
	scale  float64
}{
	{"TB", 1e12},
	{"GB", 1e9},
	{"MB", 1e6},
	{"KB", 1e3},
}

// Digits only so a sign, an exponent or a space is refused
func parseWhole(text string) (float64, error) {
	if !digits(text) {
		return 0, errBudgetForm
	}
	n, err := strconv.ParseInt(text, 10, 64)
	if err != nil || n < 1 {
		return 0, errBudgetForm
	}
	return float64(n), nil
}

// Digits with an optional fraction
func parseDecimal(text string) (float64, error) {
	whole, frac, dotted := strings.Cut(text, ".")
	if !digits(whole) || (dotted && !digits(frac)) {
		return 0, errBudgetForm
	}
	n, err := strconv.ParseFloat(text, 64)
	if err != nil || n <= 0 {
		return 0, errBudgetForm
	}
	return n, nil
}

// 1. A plain whole number is bytes
// 2. KB, MB, GB and TB scale by powers of 1000 as Athena bills
// 3. The suffix takes any case and may follow a space so the report's own form parses back
func parseBytes(text string) (float64, error) {
	upper := strings.ToUpper(text)
	for _, u := range byteUnits {
		if n, ok := strings.CutSuffix(upper, u.suffix); ok {
			v, err := parseDecimal(strings.TrimSuffix(n, " "))
			if err != nil {
				return 0, err
			}
			// A finite number can still overflow once scaled and JSON can't carry Inf
			if math.IsInf(v*u.scale, 0) {
				return 0, errBudgetForm
			}
			return v * u.scale, nil
		}
	}
	return parseWhole(text)
}

func digits(s string) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

// One decimal place in the largest unit that fits such as 1.2 GB
// A value that rounds up to 1000 moves to the next unit so 999.96 GB reads 1 TB
func formatBytes(amount float64) string {
	for i, u := range byteUnits {
		if amount < u.scale {
			continue
		}
		v := math.Round(amount/u.scale*10) / 10
		if v >= 1000 && i > 0 {
			return "1 " + byteUnits[i-1].suffix
		}
		return strconv.FormatFloat(v, 'f', -1, 64) + " " + u.suffix
	}
	return strconv.FormatFloat(amount, 'f', 0, 64) + " bytes"
}

// Rounds to two places so the float noise of read capacity sums stays out of the report
func formatCount(one, many string) func(float64) string {
	return func(amount float64) string {
		amount = math.Round(amount*100) / 100
		unit := many
		if amount == 1 {
			unit = one
		}
		return strconv.FormatFloat(amount, 'f', -1, 64) + " " + unit
	}
}
