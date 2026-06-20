package resp

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
	"strings"
)

// Type represents a RESP2 data type.
type Type byte

const (
	TypeSimpleString Type = '+'
	TypeError        Type = '-'
	TypeInteger      Type = ':'
	TypeBulkString   Type = '$'
	TypeArray        Type = '*'
)

// Value holds a parsed RESP2 value.
type Value struct {
	Type    Type
	Str     string
	Integer int64
	Array   []Value
	IsNull  bool
}

// ---------------------------------------------------------------------------
// Constructors
// ---------------------------------------------------------------------------

func SimpleString(s string) Value { return Value{Type: TypeSimpleString, Str: s} }
func Err(msg string) Value        { return Value{Type: TypeError, Str: msg} }
func Integer(n int64) Value       { return Value{Type: TypeInteger, Integer: n} }
func BulkString(s string) Value   { return Value{Type: TypeBulkString, Str: s} }
func NullBulkString() Value       { return Value{Type: TypeBulkString, IsNull: true} }
func Array(vals ...Value) Value   { return Value{Type: TypeArray, Array: vals} }
func NullArray() Value            { return Value{Type: TypeArray, IsNull: true} }

// ---------------------------------------------------------------------------
// Serializer
// ---------------------------------------------------------------------------

func (v Value) Write(w io.Writer) error {
	switch v.Type {
	case TypeSimpleString:
		_, err := fmt.Fprintf(w, "+%s\r\n", v.Str)
		return err
	case TypeError:
		_, err := fmt.Fprintf(w, "-%s\r\n", v.Str)
		return err
	case TypeInteger:
		_, err := fmt.Fprintf(w, ":%d\r\n", v.Integer)
		return err
	case TypeBulkString:
		if v.IsNull {
			_, err := fmt.Fprint(w, "$-1\r\n")
			return err
		}
		_, err := fmt.Fprintf(w, "$%d\r\n%s\r\n", len(v.Str), v.Str)
		return err
	case TypeArray:
		if v.IsNull {
			_, err := fmt.Fprint(w, "*-1\r\n")
			return err
		}
		if _, err := fmt.Fprintf(w, "*%d\r\n", len(v.Array)); err != nil {
			return err
		}
		for _, elem := range v.Array {
			if err := elem.Write(w); err != nil {
				return err
			}
		}
		return nil
	default:
		return fmt.Errorf("resp: unknown type %q", v.Type)
	}
}

// ---------------------------------------------------------------------------
// Parser
// ---------------------------------------------------------------------------

func readLine(r *bufio.Reader) (string, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return "", err
	}
	if len(line) < 2 || line[len(line)-2] != '\r' {
		return "", fmt.Errorf("resp: missing \\r before \\n")
	}
	return line[:len(line)-2], nil
}

// Parse reads one complete RESP value from r.
func Parse(r *bufio.Reader) (Value, error) {
	b, err := r.ReadByte()
	if err != nil {
		return Value{}, err
	}
	switch Type(b) {
	case TypeSimpleString:
		return parseSimpleString(r)
	case TypeError:
		return parseError(r)
	case TypeInteger:
		return parseInteger(r)
	case TypeBulkString:
		return parseBulkString(r)
	case TypeArray:
		return parseArray(r)
	default:
		if err := r.UnreadByte(); err != nil {
			return Value{}, err
		}
		return parseInline(r)
	}
}

func parseSimpleString(r *bufio.Reader) (Value, error) {
	line, err := readLine(r)
	if err != nil {
		return Value{}, fmt.Errorf("resp: reading simple string: %w", err)
	}
	return SimpleString(line), nil
}

func parseError(r *bufio.Reader) (Value, error) {
	line, err := readLine(r)
	if err != nil {
		return Value{}, fmt.Errorf("resp: reading error: %w", err)
	}
	return Err(line), nil
}

func parseInteger(r *bufio.Reader) (Value, error) {
	line, err := readLine(r)
	if err != nil {
		return Value{}, fmt.Errorf("resp: reading integer: %w", err)
	}
	n, err := strconv.ParseInt(line, 10, 64)
	if err != nil {
		return Value{}, fmt.Errorf("resp: invalid integer %q: %w", line, err)
	}
	return Integer(n), nil
}

func parseBulkString(r *bufio.Reader) (Value, error) {
	line, err := readLine(r)
	if err != nil {
		return Value{}, fmt.Errorf("resp: reading bulk string length: %w", err)
	}
	length, err := strconv.ParseInt(line, 10, 64)
	if err != nil {
		return Value{}, fmt.Errorf("resp: invalid bulk string length %q: %w", line, err)
	}
	if length < -1 {
		return Value{}, fmt.Errorf("resp: invalid bulk string length %d", length)
	}
	if length == -1 {
		return NullBulkString(), nil
	}
	buf := make([]byte, length)
	if _, err := io.ReadFull(r, buf); err != nil {
		return Value{}, fmt.Errorf("resp: reading bulk string data: %w", err)
	}
	crlf := make([]byte, 2)
	if _, err := io.ReadFull(r, crlf); err != nil {
		return Value{}, fmt.Errorf("resp: reading bulk string trailing CRLF: %w", err)
	}
	if crlf[0] != '\r' || crlf[1] != '\n' {
		return Value{}, fmt.Errorf("resp: bulk string not terminated with \\r\\n")
	}
	return BulkString(string(buf)), nil
}

func parseArray(r *bufio.Reader) (Value, error) {
	line, err := readLine(r)
	if err != nil {
		return Value{}, fmt.Errorf("resp: reading array length: %w", err)
	}
	count, err := strconv.ParseInt(line, 10, 64)
	if err != nil {
		return Value{}, fmt.Errorf("resp: invalid array length %q: %w", line, err)
	}
	if count < -1 {
		return Value{}, fmt.Errorf("resp: invalid array length %d", count)
	}
	if count == -1 {
		return NullArray(), nil
	}
	elements := make([]Value, 0, count)
	for i := int64(0); i < count; i++ {
		elem, err := Parse(r)
		if err != nil {
			return Value{}, fmt.Errorf("resp: reading array element %d: %w", i, err)
		}
		elements = append(elements, elem)
	}
	return Array(elements...), nil
}

func parseInline(r *bufio.Reader) (Value, error) {
	line, err := readLine(r)
	if err != nil {
		return Value{}, fmt.Errorf("resp: reading inline command: %w", err)
	}
	parts := strings.Fields(line)
	elements := make([]Value, 0, len(parts))
	for _, p := range parts {
		elements = append(elements, BulkString(p))
	}
	return Array(elements...), nil
}
