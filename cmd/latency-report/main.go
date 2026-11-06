// latency-report evaluates one independently prepared control run from stdin.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/platform/latency"
)

func main() { os.Exit(run()) }
func run() int {
	format := flag.String("format", "json", "output format: json or prometheus")
	flag.Parse()
	if flag.NArg() != 0 || (*format != "json" && *format != "prometheus") {
		fmt.Fprintln(os.Stderr, "latency-report: expected -format json|prometheus and input JSON on stdin")
		return 2
	}
	var input latency.Input
	decoder := json.NewDecoder(os.Stdin)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&input); err != nil {
		fmt.Fprintln(os.Stderr, "latency-report: invalid input:", err)
		return 2
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		fmt.Fprintln(os.Stderr, "latency-report: expected exactly one JSON input")
		return 2
	}
	report, err := latency.Analyze(input)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}
	if *format == "prometheus" {
		_, err = fmt.Fprint(os.Stdout, report.Prometheus())
	} else {
		encoder := json.NewEncoder(os.Stdout)
		encoder.SetIndent("", "  ")
		err = encoder.Encode(report)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "latency-report: output failed:", err)
		return 2
	}
	if !report.Complete || !report.WithinOneSecond {
		return 1
	}
	return 0
}
