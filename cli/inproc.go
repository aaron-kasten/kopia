package cli

import (
	"context"
	"go.uber.org/multierr"
	"io"
	"runtime"

	"github.com/alecthomas/kingpin/v2"

	"github.com/kopia/kopia/internal/releasable"
	"github.com/kopia/kopia/repo/logging"
)

// RunSubcommand executes the subcommand asynchronously in current process
// with flags in an isolated CLI environment and returns standard output and standard error.
func (c *App) RunSubcommand(ctx context.Context, kpapp *kingpin.Application, stdin io.Reader, argsAndFlags []string) (stdout, stderr io.Reader, wait func() error, kill func()) {
	stdoutReader, stdoutWriter := io.Pipe()
	stderrReader, stderrWriter := io.Pipe()

	c.stdinReader = stdin
	c.stdoutWriter = stdoutWriter
	c.stderrWriter = stderrWriter
	c.rootctx = logging.WithLogger(ctx, logging.ToWriter(stderrWriter))
	c.simulatedCtrlC = make(chan bool, 1)
	c.isInProcessTest = true

	releasable.Created("simulated-ctrl-c", c.simulatedCtrlC)

	c.Attach(kpapp)

	// each call-site will send on channel once before process exit.  Close below applies to each subcommand so
	// should only need NumCPU channel slots.
	resultErr := make(chan error, runtime.NumCPU())

	c.exitWithError = func(ec error) {
		resultErr <- ec
	}

	go func() {
		defer func() {
			stdoutWriter.Close() //nolint:errcheck
			stderrWriter.Close() //nolint:errcheck
			close(resultErr)
			close(c.simulatedCtrlC)
			releasable.Released("simulated-ctrl-c", c.simulatedCtrlC)
		}()

		_, err := kpapp.Parse(argsAndFlags)
		if err != nil {
			resultErr <- err
			return
		}

	}()

	return stdoutReader, stderrReader, func() error {
			var err error
			for oneError := range resultErr {
				err = multierr.Append(err, oneError)
			}
			return err
		}, func() {
			// deliver simulated Ctrl-C to the app.
			c.simulatedCtrlC <- true
		}
}
