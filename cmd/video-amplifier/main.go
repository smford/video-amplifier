package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"runtime"
	"syscall"

	"github.com/smford/video-amplifier/internal/app"
	"github.com/smford/video-amplifier/internal/config"
	"github.com/smford/video-amplifier/internal/logging"
)

var (
	Version   = "1.0.0"
	GitCommit = "HEAD"
	BuildDate = "unknown"
)

func main() {
	// Handle "video-amplifier init [flags]" subcommand
	if len(os.Args) > 1 && os.Args[1] == "init" {
		initCmd := flag.NewFlagSet("init", flag.ExitOnError)
		outputPath := initCmd.String("output", "config.yaml", "Destination path for generated configuration file")
		force := initCmd.Bool("force", false, "Overwrite destination file if it already exists")
		printStdout := initCmd.Bool("stdout", false, "Print configuration to stdout instead of writing to file")
		_ = initCmd.Parse(os.Args[2:])

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
