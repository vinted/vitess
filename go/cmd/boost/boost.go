package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"flag"

	"vitess.io/vitess/go/cmd/boost/boost"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/logutil"
)

var (
	targetPort,
	targetGrpcPort int
	targetHost,
	targetGtid,
	targetFilter,
	targetColumns,
	targetTabletType,
	redisAddr string

	boostFlags = []string{
		"host",
		"port",
		"grpcPort",
		"gtid",
		"filter",
		"columns",
		"tabletType",
		"redisAddr",
	}
)

func usage() {
	fmt.Printf("usage of boost:\n")
	for _, name := range boostFlags {
		f := flag.Lookup(name)
		if f == nil {
			panic("unknown flag " + name)
		}
		flagUsage(f)
	}
}

// Cloned from the source to print out the usage for a given flag
func flagUsage(f *flag.Flag) {
	s := fmt.Sprintf("  -%s", f.Name) // Two spaces before -; see next two comments.
	name, usage := flag.UnquoteUsage(f)
	if len(name) > 0 {
		s += " " + name
	}
	// Boolean flags of one ASCII letter are so common we
	// treat them specially, putting their usage on the same line.
	if len(s) <= 4 { // space, space, '-', 'x'.
		s += "\t"
	} else {
		// Four spaces before the tab triggers good alignment
		// for both 4- and 8-space tab stops.
		s += "\n    \t"
	}
	s += usage
	if name == "string" {
		// put quotes on the value
		s += fmt.Sprintf(" (default %q)", f.DefValue)
	} else {
		s += fmt.Sprintf(" (default %v)", f.DefValue)
	}
	fmt.Printf(s + "\n")
}

func init() {
	flag.StringVar(&redisAddr, "redisAddr", "127.0.0.1:6379", "(defaults to 127.0.0.1:6379)")
	flag.StringVar(&targetHost, "host", "127.0.0.1", "(defaults to 127.0.0.1)")
	flag.IntVar(&targetPort, "port", 15306, "(defaults to 15306)")
	flag.IntVar(&targetGrpcPort, "grpcPort", 15991, "(defaults to 15991)")
	flag.StringVar(&targetGtid, "gtid", "{}", "(defaults to {})")
	flag.StringVar(&targetFilter, "filter", "{}", "(defaults to{})")
	flag.StringVar(&targetColumns, "columns", "id,user_id", "(defaults to id)")
	flag.StringVar(&targetTabletType, "tabletType", "master", "(defaults to{})")
	logger := logutil.NewConsoleLogger()
	flag.CommandLine.SetOutput(logutil.NewLoggerWriter(logger))
	flag.Usage = usage
}

func main() {
	flag.Lookup("logtostderr").Value.Set("true")
	flag.Parse()
	defer logutil.Flush()

	boost, err := boost.NewBoost(
		targetPort,
		targetGrpcPort,
		targetHost,
		targetGtid,
		targetFilter,
		targetColumns,
		targetTabletType,
		redisAddr,
	)
	if err != nil {
		os.Exit(1)
	}

	err = boost.Init()
	if err != nil {
		boost.Close()
		os.Exit(1)
	}

	// Catch SIGTERM and SIGINT so we get a chance to clean up.
	ctx, cancel := context.WithCancel(context.Background())
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigChan
		log.Infof("Cancelling due to signal: %v", sig)
		cancel()
	}()
	defer boost.Close()

	err = boost.Play(ctx)
	if err != nil {
		os.Exit(1)
	}

	log.Info("Stopped")
}
