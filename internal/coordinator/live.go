package coordinator

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"os"
	"sort"
	"strconv"
	"time"

	"training-goodput-lab/internal/fault"
	"training-goodput-lab/internal/metrics"
	"training-goodput-lab/internal/worker"
)

type LiveConfig struct {
	MTBFPerGPU   time.Duration
	Seed         int64
	FaultMix     fault.Mix
	Address, CSV string
	MaxSteps     int
	Logger       *log.Logger
}
type livePeer struct {
	id        string
	conn      net.Conn
	encoder   *json.Encoder
	heartbeat time.Time
}
type liveEvent struct {
	peer    *livePeer
	message worker.Message
	err     error
}

func RunLive(ctx context.Context, cfg Config, live LiveConfig) error {
	listener, err := net.Listen("tcp", live.Address)
	if err != nil {
		return err
	}
	defer listener.Close()
	return ServeLive(ctx, listener, cfg, live)
}

func ServeLive(ctx context.Context, listener net.Listener, cfg Config, live LiveConfig) (resultErr error) {
	if live.MTBFPerGPU < 0 {
		return errors.New("MTBF must be nonnegative")
	}
	if live.FaultMix == (fault.Mix{}) {
		live.FaultMix = fault.DefaultMix()
	}
	if err := live.FaultMix.Validate(); err != nil {
		return err
	}
	if err := cfg.Validate(); err != nil {
		return err
	}
	if live.Logger == nil {
		live.Logger = log.New(os.Stdout, "", log.LstdFlags)
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() { <-ctx.Done(); listener.Close() }()
	events := make(chan liveEvent, 4*(cfg.Workers+cfg.Spares)+16)
	emit := func(e liveEvent) bool {
		select {
		case events <- e:
			return true
		case <-ctx.Done():
			return false
		}
	}
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				connectionCtx, closeConnection := context.WithCancel(ctx)
				defer closeConnection()
				go func() { <-connectionCtx.Done(); conn.Close() }()
				decoder := json.NewDecoder(conn)
				var first worker.Message
				if err := conn.SetReadDeadline(time.Now().Add(10 * time.Second)); err != nil {
					return
				}
				if err := decoder.Decode(&first); err != nil {
					return
				}
				if err := conn.SetReadDeadline(time.Time{}); err != nil {
					return
				}
				if first.Type != "register" || first.ID == "" {
					return
				}
				peer := &livePeer{id: first.ID, conn: conn, encoder: json.NewEncoder(conn), heartbeat: time.Now()}
				if !emit(liveEvent{peer: peer, message: first}) {
					return
				}
				for {
					var m worker.Message
					err := decoder.Decode(&m)
					if !emit(liveEvent{peer: peer, message: m, err: err}) || err != nil {
						return
					}
				}
			}()
		}
	}()
	var output io.Writer = io.Discard
	var file *os.File
	var err error
	if live.CSV != "" {
		file, err = os.Create(live.CSV)
		if err != nil {
			return err
		}
		defer file.Close()
		output = file
	}
	writer := csv.NewWriter(output)
	defer func() { writer.Flush(); resultErr = errors.Join(resultErr, writer.Error()) }()
	if err := writer.Write([]string{"seconds", "event", "step", "checkpoint_step", "worker", "fault", "lost_steps", "generation"}); err != nil {
		return err
	}
	engine := NewEngine(cfg)
	started := time.Now()
	run := metrics.Run{Workers: cfg.Workers}
	faultStarts := map[string]time.Time{}
	checkpointStarted := time.Now()
	recoveryStarted := time.Now()
	defer func() {
		run.WallTime = time.Since(started)
		run.Steps = engine.Step
		run.UsefulTime = time.Duration(engine.Step) * cfg.StepTime
		run.LostSteps = engine.LostSteps
		run.SparesUsed = engine.SparesUsed
		metrics.PrintSummary(os.Stdout, run)
		if live.CSV != "" {
			if err := metrics.WriteCSV(live.CSV+".metrics.csv", run); err != nil {
				resultErr = errors.Join(resultErr, err)
			}
		}
	}()
	record := func(event, id string, kind fault.Kind) {
		run.Timeline = append(run.Timeline, metrics.Point{Time: time.Since(started), Step: engine.Step, Event: event})
		if err := writer.Write([]string{strconv.FormatFloat(time.Since(started).Seconds(), 'f', 6, 64), event, strconv.Itoa(engine.Step), strconv.Itoa(engine.CheckpointStep), id, string(kind), strconv.Itoa(engine.LostSteps), strconv.Itoa(engine.Generation)}); err != nil {
			resultErr = errors.Join(resultErr, err)
			cancel()
		}
		writer.Flush()
		if err := writer.Error(); err != nil {
			resultErr = errors.Join(resultErr, err)
			cancel()
		}
		live.Logger.Printf("%s worker=%s step=%d checkpoint=%d generation=%d lost=%d", event, id, engine.Step, engine.CheckpointStep, engine.Generation, engine.LostSteps)
	}
	active := map[string]*livePeer{}
	peers := map[string]*livePeer{}
	spares := []*livePeer{}
	defer func() {
		for _, p := range peers {
			p.conn.Close()
		}
	}()
	phase := "waiting"
	pending := map[string]bool{}
	checkpointPending := map[string]bool{}
	snapshotPending := map[string]bool{}
	checkpointStep := -1
	checkpointPath := ""
	candidatePath := ""
	stepStart := time.Now()
	send := func(p *livePeer, m worker.Message) error {
		if err := p.conn.SetWriteDeadline(time.Now().Add(time.Second)); err != nil {
			return err
		}
		return p.encoder.Encode(m)
	}
	rng := rand.New(rand.NewSource(live.Seed))
	nextFault := time.Now()
	if live.MTBFPerGPU > 0 {
		nextFault = nextFault.Add(fault.Next(rng, live.MTBFPerGPU/time.Duration(cfg.Workers)))
	}
	startStep := func() error {
		if live.MTBFPerGPU > 0 && !time.Now().Before(nextFault) {
			ids := make([]string, 0, len(active))
			for id := range active {
				ids = append(ids, id)
			}
			sort.Strings(ids)
			target := active[ids[rng.Intn(len(ids))]]
			if err := send(target, worker.Message{Type: "inject", Error: string(live.FaultMix.Sample(rng))}); err != nil {
				emit(liveEvent{peer: target, err: err})
			}
			nextFault = time.Now().Add(fault.Next(rng, live.MTBFPerGPU/time.Duration(cfg.Workers)))
		}
		phase = "step"
		pending = map[string]bool{}
		stepStart = time.Now()
		for id, p := range active {
			pending[id] = true
			if err := send(p, worker.Message{Type: "step", Step: engine.Step + 1, Generation: engine.Generation}); err != nil {
				emit(liveEvent{peer: p, err: err})
			}
		}
		return nil
	}
	startCheckpoint := func(initial bool) error {
		checkpointStarted = time.Now()
		checkpointStep = engine.Step
		candidatePath = ""
		checkpointPending = map[string]bool{}
		if !cfg.Async || initial {
			phase = "checkpoint"
		} else {
			phase = "snapshot"
			snapshotPending = map[string]bool{}
		}
		for id, p := range active {
			checkpointPending[id] = true
			pause, cost := cfg.CheckpointCost, time.Duration(0)
			if cfg.Async && !initial {
				pause, cost = cfg.SnapshotCost, cfg.CheckpointCost
				snapshotPending[id] = true
			}
			if err := send(p, worker.Message{Type: "checkpoint", Step: checkpointStep, Generation: engine.Generation, Async: cfg.Async && !initial, Pause: pause, Cost: cost}); err != nil {
				emit(liveEvent{peer: p, err: err})
			}
		}
		return nil
	}
	recoverWorker := func(id string, kind fault.Kind) error {
		p, ok := active[id]
		if !ok {
			return nil
		}
		if checkpointPath == "" {
			return fmt.Errorf("worker %s failed before initial checkpoint", id)
		}
		if len(spares) == 0 {
			return errors.New("no spare workers remain")
		}
		lostBefore := engine.LostSteps
		recoveryStarted = time.Now()
		detectionStart, known := faultStarts[id]
		if !known {
			detectionStart = stepStart
			if kind == fault.Crash {
				detectionStart = p.heartbeat
			}
		}
		if err := engine.Recover(id, kind, time.Since(started)); err != nil {
			return err
		}
		run.Failures = append(run.Failures, metrics.Failure{Kind: kind, At: detectionStart.Sub(started), Detection: time.Since(detectionStart), LostSteps: engine.LostSteps - lostBefore, Detected: true})
		delete(active, id)
		delete(peers, id)
		p.conn.Close()
		replacement := spares[0]
		spares = spares[1:]
		active[replacement.id] = replacement
		phase = "restore"
		checkpointStep = -1
		checkpointPending = map[string]bool{}
		pending = map[string]bool{}
		record("failure", id, kind)
		record("rollback", id, kind)
		record("promoted", replacement.id, kind)
		for id, p := range active {
			pending[id] = true
			if err := send(p, worker.Message{Type: "restore", Path: checkpointPath, Pause: cfg.RecoveryTime, Step: engine.CheckpointStep, Generation: engine.Generation}); err != nil {
				emit(liveEvent{peer: p, err: err})
			}
		}
		stepStart = time.Now()
		return nil
	}
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	live.Logger.Printf("listening on %s; waiting for %d workers and %d spares", listener.Addr(), cfg.Workers, cfg.Spares)
	for {
		select {
		case <-ctx.Done():
			record("stopped", "", "")
			return writer.Error()
		case e := <-events:
			p := e.peer
			if e.message.Type == "register" {
				if _, ok := peers[p.id]; ok {
					p.conn.Close()
					continue
				}
				peers[p.id] = p
				if len(active) < cfg.Workers && phase == "waiting" {
					active[p.id] = p
					record("active", p.id, "")
				} else {
					spares = append(spares, p)
					record("spare", p.id, "")
				}
				record("registered", p.id, "")
				if phase == "waiting" && len(active) == cfg.Workers && len(spares) >= cfg.Spares {
					if err := startCheckpoint(true); err != nil {
						return err
					}
				}
				continue
			}
			if peers[p.id] != p {
				continue
			}
			if e.err != nil {
				if _, ok := active[p.id]; ok {
					if err := recoverWorker(p.id, fault.Crash); err != nil {
						return err
					}
				} else {
					delete(peers, p.id)
					for i, s := range spares {
						if s == p {
							spares = append(spares[:i], spares[i+1:]...)
							break
						}
					}
				}
				continue
			}
			if e.message.Type == "heartbeat" {
				p.heartbeat = time.Now()
				continue
			}
			m := e.message
			if m.Generation != engine.Generation {
				continue
			}
			switch m.Type {
			case "fault":
				faultStarts[p.id] = time.Now()
			case "snapshotted":
				if phase != "snapshot" || m.Step != checkpointStep || !snapshotPending[p.id] {
					continue
				}
				delete(snapshotPending, p.id)
				if len(snapshotPending) == 0 {
					record("snapshot", p.id, "")
					if err := startStep(); err != nil {
						return err
					}
				}
			case "checkpointed":
				if m.Step != checkpointStep || !checkpointPending[p.id] {
					continue
				}
				if m.Error != "" {
					return fmt.Errorf("checkpoint %s: %s", p.id, m.Error)
				}
				delete(checkpointPending, p.id)
				candidatePath = m.Path
				if len(checkpointPending) == 0 {
					if cfg.Async && phase != "checkpoint" {
						run.CheckpointTime += cfg.SnapshotCost
					} else {
						run.CheckpointTime += time.Since(checkpointStarted)
					}
					engine.CommitCheckpoint(checkpointStep)
					checkpointPath = candidatePath
					checkpointStep = -1
					record("checkpoint", p.id, "")
					if phase == "checkpoint" {
						if err := startStep(); err != nil {
							return err
						}
					}
				}
			case "restored":
				if phase != "restore" || !pending[p.id] {
					continue
				}
				if m.Step != engine.CheckpointStep {
					return fmt.Errorf("worker %s restored step %d, expected %d", p.id, m.Step, engine.CheckpointStep)
				}
				delete(pending, p.id)
				if len(pending) == 0 {
					if len(run.Failures) > 0 {
						run.Failures[len(run.Failures)-1].Recovery = time.Since(recoveryStarted)
					}
					record("recovered", p.id, "")
					if err := startStep(); err != nil {
						return err
					}
				}
			case "done":
				if phase != "step" || m.Step != engine.Step+1 || !pending[p.id] {
					continue
				}
				delete(pending, p.id)
				if engine.Observe(p.id, m.Duration) {
					if err := recoverWorker(p.id, fault.Slow); err != nil {
						return err
					}
					continue
				}
				if len(pending) == 0 {
					due := engine.CompleteStep()
					record("step", "", "")
					if live.MaxSteps > 0 && engine.Step >= live.MaxSteps {
						record("finished", "", "")
						return writer.Error()
					}
					if due && checkpointStep < 0 {
						if err := startCheckpoint(false); err != nil {
							return err
						}
						continue
					}
					if err := startStep(); err != nil {
						return err
					}
				}
			}
		case now := <-ticker.C:
			if phase == "waiting" {
				continue
			}
			failed := ""
			kind := fault.Crash
			for id, p := range active {
				if engine.HeartbeatExpired(now.Sub(p.heartbeat)) {
					failed = id
					break
				}
			}
			if failed == "" && phase == "step" && engine.ProgressExpired(now.Sub(stepStart)) {
				for id := range pending {
					failed = id
					kind = fault.Hang
					break
				}
			}
			if failed != "" {
				if err := recoverWorker(failed, kind); err != nil {
					return err
				}
			}
		}
	}
}
