package main

import (
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/spencerswagger/jinliang/internal/control"
)

// printJSON writes one indented JSON document to stdout.
func printJSON(v any) error {
	enc := json.NewEncoder(os.Stdout)
	enc.SetEscapeHTML(false)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}

// eventPrinter renders runner events for the CLI. Two modes on purpose:
// JSON Lines for machines (jq-friendly), and plain lines for humans.
type eventPrinter struct {
	json bool
	out  io.Writer
}

func newEventPrinter(asJSON bool, out io.Writer) *eventPrinter {
	return &eventPrinter{json: asJSON, out: out}
}

func (p *eventPrinter) print(ev control.Event) {
	if p.json {
		b, err := json.Marshal(ev)
		if err != nil {
			return
		}
		fmt.Fprintf(p.out, "%s\n", b)
		return
	}
	label := ev.PhaseLabel
	if label == "" {
		label = ev.Type
	}
	msg := ev.Message
	if msg == "" {
		msg = ev.Type
	}
	fmt.Fprintf(p.out, "[%s] %s\n", label, msg)
	if ev.Type == "error" {
		if len(ev.Next) > 0 {
			fmt.Fprintln(p.out, "  你现在能做什么：")
			for i, n := range ev.Next {
				fmt.Fprintf(p.out, "    %d. %s\n", i+1, n)
			}
		}
	}
}
