package join

import (
	"log/slog"
	"os"
	"os/signal"
	"sort"
	"syscall"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

type JoinConfig struct {
	MomHost           string
	MomPort           int
	InputQueue        string
	OutputQueue       string
	SumAmount         int
	SumPrefix         string
	AggregationAmount int
	AggregationPrefix string
	TopSize           int
}

type Join struct {
	inputQueue  middleware.Middleware
	outputQueue middleware.Middleware

	aggregationAmount int
	topSize           int

	fruitMap map[string]map[string]fruititem.FruitItem
	eofCount map[string]int
}

func NewJoin(config JoinConfig) (*Join, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	inputQueue, err := middleware.CreateQueueMiddleware(config.InputQueue, connSettings)
	if err != nil {
		return nil, err
	}

	outputQueue, err := middleware.CreateQueueMiddleware(config.OutputQueue, connSettings)
	if err != nil {
		inputQueue.Close()
		return nil, err
	}

	return &Join{
		inputQueue:        inputQueue,
		outputQueue:       outputQueue,
		aggregationAmount: config.AggregationAmount,
		topSize:           config.TopSize,
		fruitMap:          map[string]map[string]fruititem.FruitItem{},
		eofCount:          map[string]int{},
	}, nil
}

func (join *Join) Run() {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGTERM)

	go func() {
		if err := join.inputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
			join.handleMessage(msg, ack, nack)
		}); err != nil {
			slog.Error("join consumer failed", "err", err)
		}
	}()

	sig := <-sigChan
	slog.Info("Received signal, shutting down", "signal", sig)

	join.inputQueue.Close()
	join.outputQueue.Close()
}

func (join *Join) handleMessage(msg middleware.Message, ack func(), nack func()) {
	defer ack()

	queryID, fruitRecords, isEof, _, err := inner.DeserializeMessageWithID(&msg)
	if err != nil {
		slog.Error("While deserializing message", "err", err)
		return
	}

	// cuando recibo el eof armo el top y lo envío
	if isEof {
		slog.Info("Join received EOF", "queryID", queryID)
		join.eofCount[queryID]++

		if join.eofCount[queryID] >= join.aggregationAmount {
			join.sendFinalTop(queryID)
			delete(join.eofCount, queryID)
		}
		return
	}

	// si no es eof entonces sumo datos
	join.handleData(queryID, fruitRecords)
}

func (join *Join) handleData(queryID string, fruitRecords []fruititem.FruitItem) {
	if _, ok := join.fruitMap[queryID]; !ok {
		join.fruitMap[queryID] = map[string]fruititem.FruitItem{}
	}

	for _, fruitRecord := range fruitRecords {
		current, exists := join.fruitMap[queryID][fruitRecord.Fruit]
		if exists {
			join.fruitMap[queryID][fruitRecord.Fruit] = current.Sum(fruitRecord)
		} else {
			join.fruitMap[queryID][fruitRecord.Fruit] = fruitRecord
		}
	}
}

func (join *Join) sendFinalTop(queryID string) {
	slog.Info("Join building final top", "queryID", queryID)

	fruitMap, ok := join.fruitMap[queryID]
	if !ok {
		return
	}

	fruitItems := make([]fruititem.FruitItem, 0, len(fruitMap))
	for _, item := range fruitMap {
		fruitItems = append(fruitItems, item)
	}

	sort.SliceStable(fruitItems, func(i, j int) bool {
		return fruitItems[j].Less(fruitItems[i])
	})

	if len(fruitItems) > join.topSize {
		fruitItems = fruitItems[:join.topSize]
	}

	msg, err := inner.SerializeMessageWithID(queryID, fruitItems)
	if err != nil {
		slog.Error("While serializing final top", "err", err)
		return
	}

	if err := join.outputQueue.Send(*msg); err != nil {
		slog.Error("While sending final top", "err", err)
		return
	}

	delete(join.fruitMap, queryID)
}
