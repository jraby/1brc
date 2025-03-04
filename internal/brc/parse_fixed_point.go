package brc

import "fmt"

func ParseFixedPoint16Unsafe(input []byte) (int16, error) {
	var value, prev int16
	var mult int16 = 1
	for i := len(input) - 1; i >= 0; i-- {
		if input[i] == '-' {
			value = -value
			continue
		}

		if input[i] != '.' {
			prev = value
			value += mult * int16(input[i]-'0')
			mult *= 10
			if (prev ^ value) < 0 {
				return 0, fmt.Errorf("sign change / over/under flow")
			}
		}
	}
	return value, nil
}

// ParseFixedPoint parses input bytes as a float, encoding it as the value * 10
// into a int16, keeping only the first decimal place
// i.e: 12.321 -> 123
func ParseFixedPoint16(input []byte) (int16, error) {
	var value, prev int16
	var decimalSeen bool
	var decimalPlaces int
	var negative bool

	for i, b := range input {
		if i == 0 && b == '-' { // Handle negative sign only at the start
			negative = true
		} else if b >= '0' && b <= '9' {
			if decimalSeen {
				decimalPlaces++
				if decimalPlaces > 1 {
					break // Stop after first decimal place
				}
			}
			prev = value
			value = value*10 + int16(b-'0')
			if (prev ^ value) < 0 {
				return 0, fmt.Errorf("sign change / over/under flow")
			}
		} else if b == '.' {
			if decimalSeen { // Multiple dots? Invalid.
				return 0, fmt.Errorf("multiple dots: %s", string(input))
			}
			decimalSeen = true
		} else { // Invalid character
			return 0, fmt.Errorf("invalid byte: %s (%b)", string(input), b)
		}
	}

	// Ensure we have exactly one decimal place (scale up if necessary)
	if !decimalSeen {
		value *= 10 // "123" → "1230"
	} else if decimalPlaces == 0 {
		value *= 10 // "123." → "1230"
	}

	if negative {
		value = -value
	}

	return value, nil
}
