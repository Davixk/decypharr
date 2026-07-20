package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"path/filepath"
	"runtime/debug"
	"syscall"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/logger"

	"github.com/sirrobot01/decypharr/cmd/decypharr"
)

func main() {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("FATAL: Recovered from panic in main: %v\n", r)
			debug.PrintStack()
		}
	}()

	var configPath string
	var pprofAddr string

	// Create a default config directory if it doesn't exist
	flag.StringVar(&configPath, "config", "", "path to the data folder")
	flag.StringVar(&pprofAddr, "pprof", ":6060", "pprof server address (set to empty to disable)")
	flag.Parse()

	// pprof is OFF by default. Enable it with the legacy ENABLE_PPROF or the
	// documented DECYPHARR_DEBUG_PPROF (=1/true/yes). Optionally override the
	// listen address with DECYPHARR_DEBUG_PPROF_ADDR (e.g. 127.0.0.1:6060 to
	// keep it localhost-only and never publicly exposed).
	enablePprof := os.Getenv("ENABLE_PPROF") != "" || isTruthy(os.Getenv("DECYPHARR_DEBUG_PPROF"))
	if addr := os.Getenv("DECYPHARR_DEBUG_PPROF_ADDR"); addr != "" {
		pprofAddr = addr
	}

	if configPath == "" {
		defaultDir, err := os.UserHomeDir()
		if err != nil {
			// If we can't get the user home directory, fallback to current directory
			defaultDir = "."
		}
		defaultConfigDir := filepath.Join(defaultDir, ".decypharr")
		configPath = defaultConfigDir
	}

	config.SetConfigPath(configPath)
	config.Get()

	// Buffer pools are owned by their subsystems: the DFS cache (vfs.NewCache)
	// and the usenet reader each create a buffer.Pool with their own configured
	// RAM budget and disk limit.

	// Start pprof server if enabled
	if pprofAddr != "" && enablePprof {
		go func() {
			log.Printf("Starting pprof server on %s", pprofAddr)
			if err := http.ListenAndServe(pprofAddr, nil); err != nil {
				log.Printf("pprof server error: %v", err)
			}
		}()
	}

	// Create a context canceled on SIGINT/SIGTERM
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Install the on-demand, non-fatal goroutine-dump handler (SIGUSR1 on Unix).
	// This is separate from the SIGINT/SIGTERM graceful-shutdown context above:
	// SIGUSR1 only snapshots goroutine stacks to a file/log and NEVER shuts the
	// process down. See debug.go for operator usage.
	startDebugDumpHandler(ctx, logger.GetLogPath(), logger.Default())

	if err := decypharr.Start(ctx); err != nil {
		log.Fatal(err)
	}
}
