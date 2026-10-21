package main

import (
	"bytes"
	"fmt"
	"io"
	"strings"
)

func EncodeRaw(cmd [][]byte) []byte {
	var b bytes.Buffer
	fmt.Fprintf(&b, "*%d\r\n", len(cmd))
	for _, arg := range cmd {
		fmt.Fprintf(&b, "$%d\r\n", len(arg))
		b.Write(arg)
		b.WriteString("\r\n")
	}
	return b.Bytes()
}

func writeStream(w io.Writer, entries []StreamEntry) {
	var b strings.Builder
	fmt.Fprintf(&b, "*%d\r\n", len(entries))

	for _, e := range entries {
		b.WriteString("*2\r\n")

		id := e.ID.String()
		fmt.Fprintf(&b, "$%d\r\n%s\r\n", len(id), id)

		fmt.Fprintf(&b, "*%d\r\n", len(e.Fields))
		for _, f := range e.Fields {
			fmt.Fprintf(&b, "$%d\r\n%s\r\n", len(f), f)
		}
	}

	w.Write([]byte(b.String()))
}

type streamResult struct {
	key     string
	entries []StreamEntry
}

func writeXRead(w io.Writer, results []streamResult) {
	var b strings.Builder

	fmt.Fprintf(&b, "*%d\r\n", len(results)) // one per stream key

	for _, r := range results {
		b.WriteString("*2\r\n") // [key, entries]
		bulk(&b, r.key)

		fmt.Fprintf(&b, "*%d\r\n", len(r.entries)) // the entries array
		for _, e := range r.entries {
			b.WriteString("*2\r\n") // [id, fields]
			bulk(&b, e.ID.String())

			fmt.Fprintf(&b, "*%d\r\n", len(e.Fields))
			for _, f := range e.Fields {
				bulk(&b, f)
			}
		}
	}

	w.Write([]byte(b.String()))
}

func bulk(b *strings.Builder, s string) {
	fmt.Fprintf(b, "$%d\r\n%s\r\n", len(s), s)
}

func writeArray(w io.Writer, popped []string) {
	var b strings.Builder
	fmt.Fprintf(&b, "*%d\r\n", len(popped))

	for _, item := range popped {
		fmt.Fprintf(&b, "$%d\r\n%s\r\n", len(item), item)
	}

	w.Write([]byte(b.String()))
}

func bulkString(w io.Writer, args ...string) {
	var b strings.Builder

	fmt.Fprintf(&b, "*%d\r\n", len(args))

	for _, arg := range args {
		fmt.Fprintf(&b, "$%d\r\n%s\r\n", len(arg), arg)
	}

	w.Write([]byte(b.String()))
}

func encodeCommand(args ...string) []byte {
	var b strings.Builder
	fmt.Fprintf(&b, "*%d\r\n", len(args))

	for _, arg := range args {
		fmt.Fprintf(&b, "$%d\r\n%s\r\n", len(arg), arg)
	}

	return []byte(b.String())
}
