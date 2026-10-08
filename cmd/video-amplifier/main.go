package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"syscall"
	"time"

	"github.com/smford/video-amplifier/internal/app"
	"github.com/smford/video-amplifier/internal/config"
	"github.com/smford/video-amplifier/internal/discovery"
	"github.com/smford/video-amplifier/internal/logging"
)

var (
	Version   = "1.0.0"
	GitCommit = "HEAD"
	BuildDate = "unknown"
)

func main() {
	// Handle "video-amplifier scan [flags]" subcommand for ONVIF camera discovery
	if len(os.Args) > 1 && os.Args[1] == "scan" {
		scanCmd := flag.NewFlagSet("scan", flag.ExitOnError)
		timeoutSec := scanCmd.Int("timeout", 3, "Discovery probe timeout in seconds")
		_ = scanCmd.Parse(os.Args[2:])

		fmt.Printf("Scanning local subnet for ONVIF IP cameras (timeout %ds)...\n", *timeoutSec)
		ctx, cancel := context.WithTimeout(context.Background(), time.Duration(*timeoutSec)*time.Second)
		defer cancel()

		devices, err := discovery.ProbeLocalNetwork(ctx, time.Duration(*timeoutSec)*time.Second)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error during network discovery: %v\n", err)
			os.Exit(1)
		}

		if len(devices) == 0 {
			fmt.Println("No ONVIF cameras responded to discovery probe.")
			os.Exit(0)
		}

		fmt.Printf("Discovered %d camera(s):\n\n", len(devices))
		for i, dev := range devices {
			fmt.Printf("[%d] Host: %s\n", i+1, dev.Address)
			if len(dev.XAddrs) > 0 {
				fmt.Printf("    ONVIF Endpoint: %s\n", dev.XAddrs[0])
			}
			if len(dev.Scopes) > 0 {
				fmt.Printf("    Scopes: %s\n", strings.Join(dev.Scopes, ", "))
			}
			fmt.Println()
		}
		os.Exit(0)
	}

	// Handle "video-amplifier init [flags]" subcommand
	if len(os.Args) > 1 && os.Args[1] == "init" {
		initCmd := flag.NewFlagSet("init", flag.ExitOnError)
		outputPath := initCmd.String("output", "config.yaml", "Destination path for generated configuration file")
		force := initCmd.Bool("force", false, "Overwrite destination file if it already exists")
		printStdout := initCmd.Bool("stdout", false, "Print configuration to stdout instead of writing to file")
		scan := initCmd.Bool("scan", false, "Scan local network for ONVIF cameras and include them in the template")
		_ = initCmd.Parse(os.Args[2:])

		if *scan {
			fmt.Println("Scanning local subnet for ONVIF cameras...")
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			devices, _ := discovery.ProbeLocalNetwork(ctx, 3*time.Second)
			if len(devices) > 0 {
				fmt.Printf("Discovered %d camera(s) on local subnet\n", len(devices))
			}
		}

		if *printStdout {
			fmt.Print(config.SampleConfigYAML())
			os.Exit(0)
		}

		if err := config.WriteSampleConfig(*outputPath, *force); err != nil {
			fmt.Fprintf(os.Stderr, "Error generating configuration file: %v\n", err)
			os.Exit(1)
		}

		fmt.Printf("Successfully generated configuration file at %s\n\nTo run video-amplifier with this configuration:\n  video-amplifier -config %s\n", *outputPath, *outputPath)
		os.Exit(0)
	}

	configPath := flag.String("config", "config.yaml", "Path to YAML configuration file")
	showVersion := flag.Bool("version", false, "Print version information and exit")
	initFlag := flag.Bool("init", false, "Generate a useful configuration file and exit")
	forceFlag := flag.Bool("force", false, "Overwrite existing file when using -init")
	outputFlag := flag.String("output", "config.yaml", "Output path when using -init")
	logLevelFlag := flag.String("log-level", "", "Override log level (DEBUG, INFO, WARN, ERROR)")
	flag.Parse()

	if *showVersion {
		fmt.Printf("video-amplifier version %s (%s) built at %s on %s/%s\n",
			Version, GitCommit, BuildDate, runtime.GOOS, runtime.GOARCH)
		os.Exit(0)
	}

	if *initFlag {
		if err := config.WriteSampleConfig(*outputFlag, *forceFlag); err != nil {
			fmt.Fprintf(os.Stderr, "Error generating configuration file: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("Successfully generated configuration file at %s\n\nTo run video-amplifier with this configuration:\n  video-amplifier -config %s\n", *outputFlag, *outputFlag)
		os.Exit(0)
	}

	// Environment variable fallback for config path
	if envConfig := os.Getenv("VIDEO_AMPLIFIER_CONFIG_FILE"); envConfig != "" && *configPath == "config.yaml" {
		*configPath = envConfig
	}

	// Check if config file exists if specified
	cfg, err := config.LoadConfig(*configPath)
	if err != nil {
		// If default config.yaml is missing, warn and load defaults
		if *configPath == "config.yaml" && os.IsNotExist(err) {
			slog.Warn("No config.yaml found, proceeding with default server configuration")
			cfg = config.DefaultConfig()
		} else {
			slog.Error("Failed to load configuration", slog.String("path", *configPath), slog.Any("error", err))
			os.Exit(1)
		}
	}

	// Override log level if provided via CLI flag
	if *logLevelFlag != "" {
		cfg.Server.LogLevel = *logLevelFlag
	}

	// Initialize production structured logger with URL credential redaction
	logger := logging.SetupDefaultLogger(cfg.Server.LogLevel, cfg.Server.LogFormat)
	logger.Info("Starting video-amplifier streaming proxy",
		slog.String("version", Version),
		slog.String("commit", GitCommit),
		slog.String("go_version", runtime.Version()),
		slog.String("log_level", cfg.Server.LogLevel),
	)

	// Create application instance
	application, err := app.New(cfg, logger)
	if err != nil {
		logger.Error("Failed to initialize application", slog.Any("error", err))
		os.Exit(1)
	}

	// Setup signal trapping for graceful shutdown
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Run application
	if err := application.Run(ctx); err != nil {
		logger.Error("Application terminated with error", slog.Any("error", err))
		os.Exit(1)
	}

	logger.Info("video-amplifier exited cleanly")
}
