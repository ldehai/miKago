package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"

	"github.com/andy/mikago/internal/protocol"
)

func main() {
	addr := "localhost:19092"
	if len(os.Args) > 1 {
		addr = os.Args[1]
	}
	mode := "produce"
	if len(os.Args) > 2 {
		mode = os.Args[2]
	}

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		fmt.Printf("❌ Connect failed: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close()

	if mode == "produce" {
		for i := 0; i < 3; i++ {
			msg := fmt.Sprintf("persistent message #%d", i)
			sendProduce(conn, int32(i+1), "restart-test", 0, nil, []byte(msg))
			data := readResponse(conn)
			d := protocol.NewDecoder(data)
			d.Int32()
			d.ArrayLength()
			d.String()
			d.ArrayLength()
			d.Int32()
			errCode, _ := d.Int16()
			offset, _ := d.Int64()
			fmt.Printf("  Produced[%d]: error=%d, offset=%d\n", i, errCode, offset)
		}
		fmt.Println("✅ Produced 3 messages")
	} else {
		sendFetch(conn, 10, "restart-test", 0, 0)
		data := readResponse(conn)
		d := protocol.NewDecoder(data)
		d.Int32()
		d.ArrayLength()
		d.String()
		d.ArrayLength()
		d.Int32()
		d.Int16()
		hwm, _ := d.Int64()
		recordSet, _ := d.Bytes()
		fmt.Printf("Fetch: hwm=%d, data_size=%d bytes\n", hwm, len(recordSet))

		md := protocol.NewDecoder(recordSet)
		count := 0
		for md.Remaining() >= 12 {
			off, _ := md.Int64()
			msgSize, _ := md.Int32()
			if md.Remaining() < int(msgSize) {
				break
			}
			md.Int32()
			magic, _ := md.Int8()
			md.Int8()
			if magic >= 1 {
				md.Int64()
			}
			md.Bytes()
			val, _ := md.Bytes()
			fmt.Printf("  Message: offset=%d, value=%s\n", off, string(val))
			count++
		}
		if count > 0 {
			fmt.Printf("✅ Fetched %d messages after restart!\n", count)
		} else {
			fmt.Println("❌ No messages found after restart!")
			os.Exit(1)
		}
	}
}

func sendRequest(conn net.Conn, payload []byte) {
	msg := make([]byte, 4+len(payload))
	binary.BigEndian.PutUint32(msg[:4], uint32(len(payload)))
	copy(msg[4:], payload)
	conn.Write(msg)
}

func readResponse(conn net.Conn) []byte {
	sizeBuf := make([]byte, 4)
	io.ReadFull(conn, sizeBuf)
	size := binary.BigEndian.Uint32(sizeBuf)
	body := make([]byte, size)
	io.ReadFull(conn, body)
	return body
}

func sendProduce(conn net.Conn, corrID int32, topic string, part int32, key, value []byte) {
	inner := protocol.NewEncoder()
	inner.PutInt8(0)
	inner.PutInt8(0)
	if key == nil {
		inner.PutInt32(-1)
	} else {
		inner.PutInt32(int32(len(key)))
		inner.PutRawBytes(key)
	}
	inner.PutInt32(int32(len(value)))
	inner.PutRawBytes(value)
	ib := inner.Bytes()
	ms := protocol.NewEncoder()
	ms.PutInt64(0)
	ms.PutInt32(int32(4 + len(ib)))
	ms.PutInt32(0)
	ms.PutRawBytes(ib)
	msb := ms.Bytes()
	enc := protocol.NewEncoder()
	enc.PutInt16(0)
	enc.PutInt16(0)
	enc.PutInt32(corrID)
	enc.PutInt16(-1)
	enc.PutInt16(1)
	enc.PutInt32(5000)
	enc.PutArrayLength(1)
	enc.PutString(topic)
	enc.PutArrayLength(1)
	enc.PutInt32(part)
	enc.PutBytes(msb)
	sendRequest(conn, enc.Bytes())
}

func sendFetch(conn net.Conn, corrID int32, topic string, part int32, offset int64) {
	enc := protocol.NewEncoder()
	enc.PutInt16(1)
	enc.PutInt16(0)
	enc.PutInt32(corrID)
	enc.PutInt16(-1)
	enc.PutInt32(-1)
	enc.PutInt32(5000)
	enc.PutInt32(1)
	enc.PutArrayLength(1)
	enc.PutString(topic)
	enc.PutArrayLength(1)
	enc.PutInt32(part)
	enc.PutInt64(offset)
	enc.PutInt32(1048576)
	sendRequest(conn, enc.Bytes())
}
