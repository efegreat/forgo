package openapi

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"strings"
)

// validation intentionally ignores unknown, conditional, cross-field, and OR
// rules. Constraints are attached locally, never to a shared component schema.
func (b *schemaBuilder) validation(base Schema, t reflect.Type, tag string) (Schema, bool, error) {
	original := t
	t = indirect(t)
	if t == numberType {
		t = reflect.TypeFor[float64]()
	}
	constraints := Schema{}
	required := false
	omitted := false
	rules := strings.Split(tag, ",")
	for _, rule := range rules {
		if rule == "-" {
			return base, false, nil
		}
		if strings.Contains(rule, "|") {
			continue
		}
		key, value, _ := strings.Cut(rule, "=")
		switch key {
		case "required":
			if !omitted {
				required = true
			}
		case "omitempty", "omitnil": // following constraints apply only to present values
			omitted = true
		case "dive", "keys", "endkeys":
			goto done
		case "min", "max", "len", "gt", "gte", "lt", "lte":
			if t == timeType || t.Kind() == reflect.Struct {
				continue
			}
			k := t.Kind()
			// Encoded byte length differs from Go slice length. Do not attach
			// minItems/maxItems to a base64 JSON string.
			if k == reflect.Slice && t.Elem().Kind() == reflect.Uint8 && base["contentEncoding"] == "base64" {
				continue
			}
			if k == reflect.String || k == reflect.Slice || k == reflect.Array || k == reflect.Map {
				n, err := strconv.ParseInt(value, 10, 64)
				if err != nil || n < 0 {
					return nil, false, fmt.Errorf("invalid validation %q", rule)
				}
				low, high := "minLength", "maxLength"
				if k == reflect.Slice || k == reflect.Array {
					low, high = "minItems", "maxItems"
				}
				if k == reflect.Map {
					low, high = "minProperties", "maxProperties"
				}
				switch key {
				case "min", "gte":
					constraints[low] = n
				case "max", "lte":
					constraints[high] = n
				case "len":
					constraints[low] = n
					constraints[high] = n
				case "gt":
					constraints[low] = n + 1
				case "lt":
					if n == 0 {
						return nil, false, fmt.Errorf("impossible validation %q", rule)
					}
					constraints[high] = n - 1
				}
			} else if isInteger(k) || k == reflect.Float32 || k == reflect.Float64 {
				n, err := validationNumber(value, k)
				if err != nil {
					return nil, false, fmt.Errorf("invalid validation %q: %w", rule, err)
				}
				switch key {
				case "min", "gte":
					constraints["minimum"] = n
				case "max", "lte":
					constraints["maximum"] = n
				case "gt":
					constraints["exclusiveMinimum"] = n
				case "lt":
					constraints["exclusiveMaximum"] = n
				case "len":
					constraints["const"] = n
				}
			}
		case "oneof":
			values, err := oneofValues(value)
			if err != nil {
				return nil, false, err
			}
			var enums []any
			for _, value := range values {
				if t.Kind() == reflect.String {
					enums = append(enums, value)
				} else if isInteger(t.Kind()) || t.Kind() == reflect.Float32 || t.Kind() == reflect.Float64 {
					n, err := validationNumber(value, t.Kind())
					if err != nil {
						return nil, false, err
					}
					enums = append(enums, n)
				}
			}
			if len(enums) > 0 {
				constraints["enum"] = enums
			}
		case "email", "uuid", "uuid3", "uuid4", "uuid5", "hostname", "hostname_rfc1123", "url", "uri", "ipv4", "ipv6":
			if t.Kind() != reflect.String {
				continue
			}
			format := key
			if strings.HasPrefix(key, "uuid") {
				format = "uuid"
			}
			if key == "url" {
				format = "uri"
			}
			if key == "hostname_rfc1123" {
				format = "hostname"
			}
			constraints["format"] = format
		case "ip":
			if t.Kind() == reflect.String {
				constraints["anyOf"] = []Schema{{"format": "ipv4"}, {"format": "ipv6"}}
			}
		}
	}
done:
	if required && (original.Kind() == reflect.Pointer || original.Kind() == reflect.Slice || original.Kind() == reflect.Map) {
		constraints["not"] = Schema{"type": "null"}
	}
	if len(constraints) == 0 {
		return base, required, nil
	}
	// allOf safely composes validation with $ref and nullable schemas in OAS 3.1.
	return Schema{"allOf": []Schema{base, constraints}}, required, nil
}

func validationNumber(s string, k reflect.Kind) (json.Number, error) {
	if isInteger(k) {
		if k >= reflect.Uint {
			if _, err := strconv.ParseUint(s, 10, 64); err != nil {
				return "", err
			}
		} else {
			if _, err := strconv.ParseInt(s, 10, 64); err != nil {
				return "", err
			}
		}
	}
	// Validate JSON syntax without converting through float64 (uint64 precision).
	var value any
	d := json.NewDecoder(strings.NewReader(s))
	d.UseNumber()
	if err := d.Decode(&value); err != nil {
		return "", err
	}
	n, ok := value.(json.Number)
	if !ok || n.String() != s {
		return "", fmt.Errorf("%q is not a JSON number", s)
	}
	return n, nil
}

func oneofValues(s string) ([]string, error) {
	var values []string
	for len(strings.TrimSpace(s)) > 0 {
		s = strings.TrimSpace(s)
		if s[0] == '\'' {
			i := strings.IndexByte(s[1:], '\'')
			if i < 0 {
				return nil, fmt.Errorf("unterminated quoted oneof value")
			}
			values = append(values, s[1:i+1])
			s = s[i+2:]
		} else {
			i := strings.IndexAny(s, " \t\r\n")
			if i < 0 {
				values = append(values, s)
				break
			}
			values = append(values, s[:i])
			s = s[i:]
		}
	}
	return values, nil
}
