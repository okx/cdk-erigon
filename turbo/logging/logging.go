package logging

import (
	"context"
	"flag"
	"fmt"
	"github.com/ledgerwatch/erigon-lib/common/metrics"
	"github.com/ledgerwatch/log/v3"
	"github.com/spf13/cobra"
	"github.com/urfave/cli/v2"
	"gopkg.in/natefinch/lumberjack.v2"
	"os"
	"path/filepath"
	"strconv"
)

const timeFormat = "2006-01-02T15:04:05-0700"
const errorKey = "LOG15_ERROR"

// Determine the log dir path based on the given urfave context
func LogDirPath(ctx *cli.Context) string {
	dirPath := ""
	if !ctx.Bool(LogDirDisableFlag.Name) {
		dirPath = ctx.String(LogDirPathFlag.Name)
		if dirPath == "" {
			datadir := ctx.String("datadir")
			if datadir != "" {
				dirPath = filepath.Join(datadir, "logs")
			}
		}
	}
	return dirPath
}

// SetupLoggerCtx performs the logging setup according to the parameters
// containted in the given urfave context. It returns either root logger,
// if rootHandler argument is set to true, or a newly created logger.
// This is to ensure gradual transition to the use of non-root logger thoughout
// the erigon code without a huge change at once.
// This function which is used in Erigon itself.
// Note: urfave and cobra are two CLI frameworks/libraries for the same functionalities
// and it would make sense to choose one over another
func SetupLoggerCtx(filePrefix string, ctx *cli.Context,
	consoleDefaultLevel log.Lvl, dirDefaultLevel log.Lvl, rootHandler bool) log.Logger {
	var consoleJson = ctx.Bool(LogJsonFlag.Name) || ctx.Bool(LogConsoleJsonFlag.Name)
	var dirJson = ctx.Bool(LogDirJsonFlag.Name)
	var asyncLogging = ctx.Bool(LogAsyncFlag.Name)

	metrics.DelayLoggingEnabled = ctx.Bool(LogBlockDelayFlag.Name)

	consoleLevel, lErr := tryGetLogLevel(ctx.String(LogConsoleVerbosityFlag.Name))
	if lErr != nil {
		// try verbosity flag
		consoleLevel, lErr = tryGetLogLevel(ctx.String(LogVerbosityFlag.Name))
		if lErr != nil {
			consoleLevel = consoleDefaultLevel
		}
	}

	dirLevel, dErr := tryGetLogLevel(ctx.String(LogDirVerbosityFlag.Name))
	if dErr != nil {
		dirLevel = dirDefaultLevel
	}

	dirPath := ""
	if !ctx.Bool(LogDirDisableFlag.Name) && dirPath != "/dev/null" {
		dirPath = ctx.String(LogDirPathFlag.Name)
		if dirPath == "" {
			datadir := ctx.String("datadir")
			if datadir != "" {
				dirPath = filepath.Join(datadir, "logs")
			}
		}
		if logDirPrefix := ctx.String(LogDirPrefixFlag.Name); len(logDirPrefix) > 0 {
			filePrefix = logDirPrefix
		}
	}

	var logger log.Logger
	if rootHandler {
		logger = log.Root()
	} else {
		logger = log.New()
	}

	initSeparatedLogging(logger, filePrefix, dirPath, consoleLevel, dirLevel, consoleJson, dirJson, asyncLogging, ctx.Context)
	return logger
}

// SetupLoggerCmd perform the logging for a cobra command, and sets it to the root logger
// This is the function which is NOT used by Erigon itself, but instead by some cobra-based commands,
// for example, rpcdaemon or integration.
// Note: urfave and cobra are two CLI frameworks/libraries for the same functionalities
// and it would make sense to choose one over another
func SetupLoggerCmd(filePrefix string, cmd *cobra.Command) log.Logger {

	logJsonVal, ljerr := cmd.Flags().GetBool(LogJsonFlag.Name)
	if ljerr != nil {
		logJsonVal = false
	}

	logConsoleJsonVal, lcjerr := cmd.Flags().GetBool(LogConsoleJsonFlag.Name)
	if lcjerr != nil {
		logConsoleJsonVal = false
	}

	var consoleJson = logJsonVal || logConsoleJsonVal
	dirJson, djerr := cmd.Flags().GetBool(LogDirJsonFlag.Name)
	if djerr != nil {
		dirJson = false
	}

	consoleLevel, lErr := tryGetLogLevel(cmd.Flags().Lookup(LogConsoleVerbosityFlag.Name).Value.String())
	if lErr != nil {
		// try verbosity flag
		consoleLevel, lErr = tryGetLogLevel(cmd.Flags().Lookup(LogVerbosityFlag.Name).Value.String())
		if lErr != nil {
			consoleLevel = log.LvlInfo
		}
	}

	dirLevel, dErr := tryGetLogLevel(cmd.Flags().Lookup(LogDirVerbosityFlag.Name).Value.String())
	if dErr != nil {
		dirLevel = log.LvlInfo
	}

	dirPath := ""
	disableFileLogging, err := cmd.Flags().GetBool(LogDirDisableFlag.Name)
	if err != nil {
		disableFileLogging = false
	}
	if !disableFileLogging && dirPath != "/dev/null" {
		dirPath = cmd.Flags().Lookup(LogDirPathFlag.Name).Value.String()
		if dirPath == "" {
			datadirFlag := cmd.Flags().Lookup("datadir")
			if datadirFlag != nil {
				datadir := datadirFlag.Value.String()
				if datadir != "" {
					dirPath = filepath.Join(datadir, "logs")
				}
			}
		}
		if logDirPrefix := cmd.Flags().Lookup(LogDirPrefixFlag.Name).Value.String(); len(logDirPrefix) > 0 {
			filePrefix = logDirPrefix
		}
	}

	initSeparatedLogging(log.Root(), filePrefix, dirPath, consoleLevel, dirLevel, consoleJson, dirJson, false, cmd.Context())
	return log.Root()
}

// SetupLoggerCmd perform the logging using parametrs specifying by `flag` package, and sets it to the root logger
// This is the function which is NOT used by Erigon itself, but instead by utility commans
func SetupLogger(filePrefix string) log.Logger {
	var logConsoleVerbosity = flag.String(LogConsoleVerbosityFlag.Name, "", LogConsoleVerbosityFlag.Usage)
	var logDirVerbosity = flag.String(LogDirVerbosityFlag.Name, "", LogDirVerbosityFlag.Usage)
	var logDirPath = flag.String(LogDirPathFlag.Name, "", LogDirPathFlag.Usage)
	var logDirPrefix = flag.String(LogDirPrefixFlag.Name, "", LogDirPrefixFlag.Usage)
	var logVerbosity = flag.String(LogVerbosityFlag.Name, "", LogVerbosityFlag.Usage)
	var logConsoleJson = flag.Bool(LogConsoleJsonFlag.Name, false, LogConsoleJsonFlag.Usage)
	var logJson = flag.Bool(LogJsonFlag.Name, false, LogJsonFlag.Usage)
	var logDirJson = flag.Bool(LogDirJsonFlag.Name, false, LogDirJsonFlag.Usage)
	flag.Parse()

	var consoleJson = *logJson || *logConsoleJson
	var dirJson = logDirJson

	consoleLevel, lErr := tryGetLogLevel(*logConsoleVerbosity)
	if lErr != nil {
		// try verbosity flag
		consoleLevel, lErr = tryGetLogLevel(*logVerbosity)
		if lErr != nil {
			consoleLevel = log.LvlInfo
		}
	}

	dirLevel, dErr := tryGetLogLevel(*logDirVerbosity)
	if dErr != nil {
		dirLevel = log.LvlInfo
	}

	if logDirPrefix != nil && len(*logDirPrefix) > 0 {
		filePrefix = *logDirPrefix
	}

	initSeparatedLogging(log.Root(), filePrefix, *logDirPath, consoleLevel, dirLevel, consoleJson, *dirJson, false, context.Background())
	return log.Root()
}

// initSeparatedLogging construct a log handler accrosing to the configuration parameters passed to it
// and sets the constructed handler to be the handler of the given logger. It then uses that logger
// to report the status of this initialisation
func initSeparatedLogging(
	logger log.Logger,
	filePrefix string,
	dirPath string,
	consoleLevel log.Lvl,
	dirLevel log.Lvl,
	consoleJson bool,
	dirJson bool,
	asyncLogging bool,
	ctx context.Context) {

	var consoleHandler log.Handler

	var format log.Format

	okLogFormatFunc := log.FormatFunc(OkLogV1Format)

	if consoleJson {
		format = okLogFormatFunc
	} else {
		format = log.TerminalFormatNoColor()
	}
	if asyncLogging {
		consoleHandler = log.LvlFilterHandler(consoleLevel, AsyncHandler(os.Stderr, format, ctx))
	} else {
		consoleHandler = log.LvlFilterHandler(consoleLevel, log.StreamHandler(os.Stderr, format))
	}

	logger.SetHandler(consoleHandler)

	if len(dirPath) == 0 {
		logger.Info("console logging only")
		return
	}

	err := os.MkdirAll(dirPath, 0764)
	if err != nil {
		logger.Warn("failed to create log dir, console logging only")
		return
	}
	dirFormat := log.TerminalFormatNoColor()
	if dirJson {
		dirFormat = okLogFormatFunc
	}

	lumberjack := &lumberjack.Logger{
		Filename:   filepath.Join(dirPath, filePrefix+".log"),
		MaxSize:    100, // megabytes
		MaxBackups: 3,
		MaxAge:     28, //days
	}
	var userLog log.Handler
	if asyncLogging {
		userLog = AsyncHandler(lumberjack, dirFormat, ctx)
	} else {
		userLog = log.StreamHandler(lumberjack, dirFormat)
	}

	mux := log.MultiHandler(consoleHandler, log.LvlFilterHandler(dirLevel, userLog))
	logger.SetHandler(mux)
	logger.Info("logging to file system", "log dir", dirPath, "file prefix", filePrefix, "log level", dirLevel, "json", dirJson)
	logger.Info(fmt.Sprintf("Async logging enabled: %v", asyncLogging))
}

func tryGetLogLevel(s string) (log.Lvl, error) {
	lvl, err := log.LvlFromString(s)
	if err != nil {
		l, err := strconv.Atoi(s)
		if err != nil {
			return 0, err
		}
		return log.Lvl(l), nil
	}
	return lvl, nil
}
