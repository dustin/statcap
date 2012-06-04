package mapconv

import (
	"log"
	"strconv"
)

// Convert a map with string values to a map with mixed values,
// converting strings to numbers where possible.
func Numerify(in map[string]string, err error) map[string]interface{} {
	rv := map[string]interface{}{}
	if err != nil {
		log.Printf("Error getting stats: %v", err)
		return rv
	}

	for k, v := range in {
		f, err := strconv.ParseFloat(v, 64)
		if err == nil {
			rv[k] = f
		} else {
			rv[k] = v
		}
	}

	return rv
}
