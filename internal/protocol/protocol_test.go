package protocol

import (
	"testing"
)

func TestEncoderDecoderInt8(t *testing.T) {
	enc := NewEncoder()
	enc.PutInt8(42)
	enc.PutInt8(-1)
	enc.PutInt8(0)

	d := NewDecoder(enc.Bytes())
	v, err := d.Int8()
	if err != nil || v != 42 {
		t.Fatalf("expected 42, got %d, err=%v", v, err)
	}
	v, err = d.Int8()
	if err != nil || v != -1 {
		t.Fatalf("expected -1, got %d, err=%v", v, err)
	}
	v, err = d.Int8()
	if err != nil || v != 0 {
		t.Fatalf("expected 0, got %d, err=%v", v, err)
	}
}

func TestEncoderDecoderInt16(t *testing.T) {
	enc := NewEncoder()
	enc.PutInt16(256)
	enc.PutInt16(-1)
	enc.PutInt16(0)

	d := NewDecoder(enc.Bytes())
	v, err := d.Int16()
	if err != nil || v != 256 {
		t.Fatalf("expected 256, got %d, err=%v", v, err)
	}
	v, err = d.Int16()
	if err != nil || v != -1 {
		t.Fatalf("expected -1, got %d, err=%v", v, err)
	}
	v, err = d.Int16()
	if err != nil || v != 0 {
		t.Fatalf("expected 0, got %d, err=%v", v, err)
	}
}

func TestEncoderDecoderInt32(t *testing.T) {
	enc := NewEncoder()
	enc.PutInt32(100000)
	enc.PutInt32(-1)

	d := NewDecoder(enc.Bytes())
	v, err := d.Int32()
	if err != nil || v != 100000 {
		t.Fatalf("expected 100000, got %d, err=%v", v, err)
	}
	v, err = d.Int32()
	if err != nil || v != -1 {
		t.Fatalf("expected -1, got %d, err=%v", v, err)
	}
}

func TestEncoderDecoderInt64(t *testing.T) {
	enc := NewEncoder()
	enc.PutInt64(1234567890123)
	enc.PutInt64(-1)

	d := NewDecoder(enc.Bytes())
	v, err := d.Int64()
	if err != nil || v != 1234567890123 {
		t.Fatalf("expected 1234567890123, got %d, err=%v", v, err)
	}
	v, err = d.Int64()
	if err != nil || v != -1 {
		t.Fatalf("expected -1, got %d, err=%v", v, err)
	}
}

func TestEncoderDecoderString(t *testing.T) {
	enc := NewEncoder()
	enc.PutString("hello")
	enc.PutString("")

	d := NewDecoder(enc.Bytes())
	s, err := d.String()
	if err != nil || s != "hello" {
		t.Fatalf("expected 'hello', got '%s', err=%v", s, err)
	}
	s, err = d.String()
	if err != nil || s != "" {
		t.Fatalf("expected '', got '%s', err=%v", s, err)
	}
}

func TestEncoderDecoderNullableString(t *testing.T) {
	enc := NewEncoder()
	hello := "hello"
	enc.PutNullableString(&hello)
	enc.PutNullableString(nil)

	d := NewDecoder(enc.Bytes())
	s, err := d.NullableString()
	if err != nil || s == nil || *s != "hello" {
		t.Fatalf("expected 'hello', got %v, err=%v", s, err)
	}
	s, err = d.NullableString()
	if err != nil || s != nil {
		t.Fatalf("expected nil, got %v, err=%v", s, err)
	}
}

func TestEncoderDecoderBytes(t *testing.T) {
	enc := NewEncoder()
	enc.PutBytes([]byte{1, 2, 3})
	enc.PutBytes(nil)

	d := NewDecoder(enc.Bytes())
	b, err := d.Bytes()
	if err != nil || len(b) != 3 || b[0] != 1 {
		t.Fatalf("expected [1,2,3], got %v, err=%v", b, err)
	}
	b, err = d.Bytes()
	if err != nil || b != nil {
		t.Fatalf("expected nil, got %v, err=%v", b, err)
	}
}

func TestDecoderInsufficientBytes(t *testing.T) {
	d := NewDecoder([]byte{0})
	_, err := d.Int16()
	if err == nil {
		t.Fatal("expected error for insufficient bytes")
	}

	d = NewDecoder([]byte{0, 1})
	_, err = d.Int32()
	if err == nil {
		t.Fatal("expected error for insufficient bytes")
	}
}

func TestRequestHeader(t *testing.T) {
	// Build a request header: api_key=18, api_version=0, correlation_id=1, client_id="test-client"
	enc := NewEncoder()
	enc.PutInt16(18) // api_key
	enc.PutInt16(0)  // api_version
	enc.PutInt32(1)  // correlation_id
	clientID := "test-client"
	enc.PutNullableString(&clientID) // client_id

	d := NewDecoder(enc.Bytes())
	header, err := DecodeRequestHeader(d)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if header.APIKey != 18 {
		t.Fatalf("expected api_key=18, got %d", header.APIKey)
	}
	if header.APIVersion != 0 {
		t.Fatalf("expected api_version=0, got %d", header.APIVersion)
	}
	if header.CorrelationID != 1 {
		t.Fatalf("expected correlation_id=1, got %d", header.CorrelationID)
	}
	if header.ClientID == nil || *header.ClientID != "test-client" {
		t.Fatalf("expected client_id='test-client', got %v", header.ClientID)
	}
}

func TestWriteSizePrefix(t *testing.T) {
	payload := []byte{1, 2, 3, 4}
	msg := WriteSizePrefix(payload)

	if len(msg) != 8 {
		t.Fatalf("expected length 8, got %d", len(msg))
	}
	// First 4 bytes should be big-endian 4
	d := NewDecoder(msg)
	size, err := d.Int32()
	if err != nil || size != 4 {
		t.Fatalf("expected size=4, got %d, err=%v", size, err)
	}
}
