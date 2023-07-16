package temporal

import (
	"context"
	"errors"
	"fmt"
	"github.com/danthegoodman1/BigHouse/gologger"
	"github.com/danthegoodman1/BigHouse/utils"
	"github.com/uber-go/tally/v4"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/api/failure/v1"
	"go.temporal.io/api/workflowservice/v1"
	"time"

	"github.com/jackc/pgx/v4"
	"github.com/rs/zerolog"
	"github.com/samber/lo"
	"github.com/uber-go/tally/v4/prometheus"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/contrib/opentelemetry"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/interceptor"
	temporalLog "go.temporal.io/sdk/log"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	sdktally "go.temporal.io/sdk/contrib/tally"
)

var (
	globalLogger = gologger.NewLogger()
	tClient      client.Client

	workers              []worker.Worker
	typesWithPreparation = make(map[string]bool)
)

const (
	emailingQueue    = "EMAILING"
	userQueue        = "USER"
	interactionQueue = "INTERACTION"
	backgroundQueue  = "BACKGROUND"
)

const (
	interactionWorkflowPrefix        = "interaction:"
	interactionPrepareWorkflowPrefix = "ia-prep:"
)

var ErrSignalTimeout = errors.New("timeout waiting for signal")

func Run(ctx context.Context, reporter prometheus.Reporter) error {
	tracingInterceptor, err := opentelemetry.NewTracingInterceptor(opentelemetry.TracerOptions{})
	if err != nil {
		return err
	}
	scopeOpts := tally.ScopeOptions{
		CachedReporter:  reporter,
		Separator:       prometheus.DefaultSeparator,
		SanitizeOptions: &sdktally.PrometheusSanitizeOptions,
		Prefix:          "tangia",
	}
	scope, _ := tally.NewRootScope(scopeOpts, time.Second)
	scope = sdktally.NewPrometheusNamingScope(scope)
	options := client.Options{
		Logger:             &logAdapter{l: globalLogger},
		ContextPropagators: []workflow.ContextPropagator{NewContextPropagator()},
		Interceptors:       []interceptor.ClientInterceptor{tracingInterceptor},
		MetricsHandler:     sdktally.NewMetricsHandler(scope),
		FailureConverter:   permFailureConverter{base: temporal.GetDefaultFailureConverter()},
	}

	if utils.Env != "LOCAL" {
		options.HostPort = "temporal-frontend.temporal.svc.cluster.local:7233"
		options.Namespace = "tangia"
	}

	c, err := client.Dial(options)
	if err != nil {
		return err
	}
	tClient = c
	wOpts := worker.Options{
		DisableRegistrationAliasing:      true,
		MaxConcurrentActivityTaskPollers: 4,
		MaxConcurrentWorkflowTaskPollers: 4,
		OnFatalError: func(err error) {
			globalLogger.Error().Err(err).Msg("Temporal Worker encountered FATAL error!")
			// let's see when this happens in real-life. Maybe we need to os.Exit() here
		},
	}
	// these must be started and added to the list at the end of the function
	userWorker := worker.New(c, userQueue, wOpts)
	bgWorkerOpts := wOpts
	bgWorkerOpts.MaxConcurrentActivityTaskPollers = 2
	bgWorkerOpts.MaxConcurrentWorkflowTaskPollers = 2
	backgroundWorker := worker.New(c, backgroundQueue, wOpts)
	workers = []worker.Worker{userWorker, backgroundWorker}

	// workflows
	// emailWorker.RegisterWorkflow(SendPostOnboardingEmailWorkflow)

	// activities - we register with all workers as a workaround until we have found a simpler way to call activities on other queues
	for _, w := range workers {
		// w.RegisterActivity(&PostOnboardingEmailActivities{})

		err = w.Start()
		if err != nil {
			return fmt.Errorf("error in worker start: %w", err)
		}
	}

	err = healthCheck(ctx)
	if err != nil {
		return fmt.Errorf("error in temporal health-check: %w", err)
	}

	return nil
}

// permFailureConverter converts known permanent errors to temporals non-retryable errors
type permFailureConverter struct {
	base converter.FailureConverter
}

func (f permFailureConverter) ErrorToFailure(err error) *failure.Failure {
	if errors.Is(err, pgx.ErrNoRows) {
		err = temporal.NewNonRetryableApplicationError(err.Error(), "entity-not-found", err)
	} else if ok := utils.IsErr[utils.PermError](err); ok {
		err = temporal.NewNonRetryableApplicationError(err.Error(), "app-perm-error", err)
	}
	return f.base.ErrorToFailure(err)
}

func (f permFailureConverter) FailureToError(failure *failure.Failure) error {
	return f.base.FailureToError(failure)
}

func Stop() {
	for _, w := range workers {
		w.Stop()
	}
}

func healthCheck(ctx context.Context) error {
	logger := *zerolog.Ctx(ctx)
	logger.Info().Msg("Temporal health-check started")
	c := tClient
	info, err := c.WorkflowService().GetSystemInfo(ctx, &workflowservice.GetSystemInfoRequest{})
	if err != nil {
		return err
	}
	logger.Info().Str("serverVersion", info.ServerVersion).Msg("Temporal available")
	return nil
}

func GetLogger(ctx workflow.Context) zerolog.Logger {
	info := workflow.GetInfo(ctx)
	l := globalLogger.With().
		Str("TaskQueue", info.TaskQueueName).
		Str("WorkflowID", info.WorkflowExecution.ID).
		Str("RunID", info.WorkflowExecution.RunID)
	if kv, ok := ctx.Value(PropagateKey).(ContextValues); ok {
		for k, v := range kv {
			l = l.Str(k, v)
		}
	}
	return l.
		Logger().Hook(zerolog.HookFunc(func(e *zerolog.Event, level zerolog.Level, message string) {
		if workflow.IsReplaying(ctx) {
			e.Discard()
		}
	}))
}

func WaitForSignal[T any](ctx workflow.Context, timeout time.Duration, signals ...string) (receivedSignal string, val T) {
	sel := workflow.NewSelector(ctx)
	for i := range signals {
		signal := signals[i]
		sel.AddReceive(workflow.GetSignalChannel(ctx, signal), func(c workflow.ReceiveChannel, more bool) {
			c.Receive(ctx, &val)
			receivedSignal = signal
		})
	}
	sel.AddFuture(workflow.NewTimer(ctx, timeout), func(f workflow.Future) {
		_ = f.Get(ctx, nil)
	})
	sel.Select(ctx)
	return receivedSignal, val
}

func drainSignal[T any](ctx workflow.Context, signal string) (data []T) {
	for {
		var t T
		c := workflow.GetSignalChannel(ctx, signal)
		ok := c.ReceiveAsync(&t)
		if !ok {
			return
		}
		data = append(data, t)
	}
}

func sendSignal(ctx context.Context, wfID, signal string, arg any) error {
	logger := zerolog.Ctx(ctx).With().Str("workflowID", wfID).Logger()
	ctx = logger.WithContext(ctx)

	logger.Info().Str("signal", signal).Msg("sending signal to workflow")

	err := tClient.SignalWorkflow(ctx, wfID, "", signal, arg)
	if err != nil {
		return fmt.Errorf("error in SignalWorkflow: %w", convertTemporalError(err))
	}
	return nil
}

type logAdapter struct {
	l zerolog.Logger
}

func (l *logAdapter) Debug(msg string, keyvals ...interface{}) {
	log := l.l.Debug()
	l.log(log, msg, keyvals)
}

func (l *logAdapter) Info(msg string, keyvals ...interface{}) {
	log := l.l.Info()
	l.log(log, msg, keyvals)
}

func (l *logAdapter) Warn(msg string, keyvals ...interface{}) {
	log := l.l.Warn()
	l.log(log, msg, keyvals)
}

func (l *logAdapter) Error(msg string, keyvals ...interface{}) {
	var log *zerolog.Event
	// hack to not get alerted on app errors
	if msg == "Activity error." && lo.ContainsBy(keyvals, func(item interface{}) bool {
		err, ok := item.(error)
		return ok && utils.IsErr[*temporal.ApplicationError](err)
	}) {
		log = l.l.Info()
		msg = "Activity error (app error)."
	} else {
		log = l.l.Error()
	}
	l.log(log, msg, keyvals)
}

func (l *logAdapter) With(keyvals ...interface{}) temporalLog.Logger {
	newLog := l.l.With()
	newLog = addKeyVals(newLog, keyvals)
	return &logAdapter{l: newLog.Logger()}
}

func (l *logAdapter) log(log *zerolog.Event, msg string, keyvals []interface{}) {
	// this wrapper + the outer wrapper + Temporal's own wrappers = 4
	log = log.CallerSkipFrame(4)
	log = addKeyVals(log, keyvals)
	log.Msg(msg)
}

// generic fluent interface baby 😎
type interfaceLogCtx[T any] interface {
	Interface(key string, i interface{}) T
	Err(err error) T
}

func addKeyVals[T interfaceLogCtx[T]](log T, keyvals []interface{}) T {
	for i := 0; i < len(keyvals)-1; i += 2 {
		s, ok := keyvals[i].(string)
		if !ok {
			continue
		}
		if err, ok := keyvals[i+1].(error); ok {
			log = log.Err(err)
			continue
		}
		log = log.Interface(s, keyvals[i+1])
	}
	return log
}

func startWorkflowIO[Tin any, Tout any](ctx context.Context, workflowFunc func(wc workflow.Context, input Tin) (Tout, error), taskQueue, workflowID string, input Tin, timeout time.Duration, idReuse enums.WorkflowIdReusePolicy) (wfID string, err error) {
	logger := zerolog.Ctx(ctx)
	wf, err := tClient.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		TaskQueue:                taskQueue,
		ID:                       workflowID,
		WorkflowExecutionTimeout: timeout,
		WorkflowIDReusePolicy:    idReuse,
	}, workflowFunc, input)
	if err != nil {
		return "", fmt.Errorf("couldn't queue workflow %s: %w", utils.FuncName(workflowFunc), convertTemporalError(err))
	}
	logger.Info().Str("WorkflowID", wf.GetID()).Str("RunID", wf.GetRunID()).Msgf("started %s workflow", utils.FuncName(workflowFunc))
	return wf.GetID(), nil
}

func startWorkflow[Tin any](ctx context.Context, workflowFunc func(wc workflow.Context, input Tin) error, taskQueue, workflowID string, input Tin, timeout time.Duration, idReuse enums.WorkflowIdReusePolicy) (wfID string, err error) {
	logger := zerolog.Ctx(ctx)
	wf, err := tClient.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		TaskQueue:                taskQueue,
		ID:                       workflowID,
		WorkflowExecutionTimeout: timeout,
		WorkflowIDReusePolicy:    idReuse,
	}, workflowFunc, input)
	if err != nil {
		return wfID, fmt.Errorf("couldn't queue workflow %s: %w", utils.FuncName(workflowFunc), convertTemporalError(err))
	}
	logger.Info().Str("WorkflowID", wf.GetID()).Str("RunID", wf.GetRunID()).Msgf("started %s workflow", utils.FuncName(workflowFunc))
	return wf.GetID(), nil
}

func startSendWorkflow[Tin any, Tsig any](ctx context.Context, workflow func(wc workflow.Context, input Tin) error, taskQueue, workflowID string, input Tin, timeout time.Duration, signal string, signalData Tsig) (wfID string, err error) {
	logger := zerolog.Ctx(ctx)
	wf, err := tClient.SignalWithStartWorkflow(ctx, workflowID, signal, signalData, client.StartWorkflowOptions{
		TaskQueue:                taskQueue,
		WorkflowExecutionTimeout: timeout,
	}, workflow, input)
	if err != nil {
		return "", fmt.Errorf("couldn't signal-queue workflow %s: %w", utils.FuncName(workflow), convertTemporalError(err))
	}
	logger.Info().Str("WorkflowID", wf.GetID()).Str("RunID", wf.GetRunID()).Str("signal", signal).Msgf("signal-started %s workflow", utils.FuncName(workflow))
	return wf.GetID(), nil
}

// segmentTrack wraps segment.Enqueue ensuring events don't get re-sent during replay
// func segmentTrack(ctx workflow.Context, msg analytics.Message) {
// 	workflow.SideEffect(ctx, func(ctx workflow.Context) interface{} {
// 		segment.Enqueue(msg)
// 		return nil
// 	})
// }

func sideEffect[T any](ctx workflow.Context, f func(ctx workflow.Context) T) (result T, err error) {
	res := workflow.SideEffect(ctx, func(ctx workflow.Context) interface{} {
		return f(ctx)
	})
	err = res.Get(&result)
	return
}

func WaitForWorkflow(ctx context.Context, wfID string) error {
	wf := tClient.GetWorkflow(ctx, wfID, "")
	var res any
	err := wf.Get(ctx, &res)
	if err != nil {
		return convertTemporalError(err)
	}
	return nil
}

func CancelWorkflow(ctx context.Context, wfID string) error {
	err := tClient.CancelWorkflow(ctx, wfID, "")
	if err != nil {
		return convertTemporalError(err)
	}
	return nil
}
