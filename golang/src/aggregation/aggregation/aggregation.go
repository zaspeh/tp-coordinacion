package aggregation

import (
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sort"
	"syscall"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

type AggregationConfig struct {
	Id                int
	MomHost           string
	MomPort           int
	OutputQueue       string
	SumAmount         int
	SumPrefix         string
	AggregationAmount int
	AggregationPrefix string
	TopSize           int
}

type Aggregation struct {
	outputQueue   middleware.Middleware
	inputExchange middleware.Middleware
	fruitItemMap  map[string]map[string]fruititem.FruitItem
	topSize       int
	sumAmount     int
	totalCount    map[string]*int
	partialCounts map[string]map[int]int
}

func NewAggregation(config AggregationConfig) (*Aggregation, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	outputQueue, err := middleware.CreateQueueMiddleware(config.OutputQueue, connSettings)
	if err != nil {
		return nil, err
	}

	inputExchangeRoutingKey := []string{fmt.Sprintf("%s_%d", config.AggregationPrefix, config.Id)}
	inputExchange, err := middleware.CreateExchangeMiddleware(config.AggregationPrefix, inputExchangeRoutingKey, connSettings)
	if err != nil {
		outputQueue.Close()
		return nil, err
	}

	return &Aggregation{
		outputQueue:   outputQueue,
		inputExchange: inputExchange,
		fruitItemMap:  map[string]map[string]fruititem.FruitItem{},
		topSize:       config.TopSize,
		sumAmount:     config.SumAmount,
		totalCount:    map[string]*int{},
		partialCounts: map[string]map[int]int{},
	}, nil
}

func (aggregation *Aggregation) Run() {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGTERM)

	go func() {
		if err := aggregation.inputExchange.StartConsuming(func(msg middleware.Message, ack, nack func()) {
			aggregation.handleMessage(msg, ack, nack)
		}); err != nil {
			slog.Error("aggregation consumer failed", "err", err)
		}
	}()

	sig := <-sigChan
	slog.Info("Received signal, shutting down", "signal", sig)

	aggregation.inputExchange.Close()
	aggregation.outputQueue.Close()
}

func (aggregation *Aggregation) handleMessage(msg middleware.Message, ack func(), nack func()) {
	queryID, fruitRecords, isEof, _, totalCount, partialCount, sumID, err := inner.DeserializeFullMessage(&msg)
	if err != nil {
		slog.Error("While deserializing message", "err", err)
		nack()
		return
	}

	slog.Info("Received message",
		"queryID", queryID,
		"isEof", isEof,
		"fruits", len(fruitRecords),
		"totalCount", totalCount,
		"partialCount", partialCount,
		"sumID", sumID,
	)

	// partial_count
	if partialCount != nil && sumID != nil {
		if _, ok := aggregation.partialCounts[queryID]; !ok {
			aggregation.partialCounts[queryID] = map[int]int{}
		}
		aggregation.partialCounts[queryID][*sumID] = *partialCount
		slog.Info("Received partial count", "queryID", queryID, "sumID", *sumID, "partialCount", *partialCount)
		aggregation.maybeFlush(queryID)
		ack()
		return
	}

	// EOF + total_cunt
	if isEof {
		if totalCount != nil {
			aggregation.totalCount[queryID] = totalCount
			slog.Info("Received EOF with total count", "queryID", queryID, "totalCount", *totalCount)
		}
		aggregation.maybeFlush(queryID)
		ack()
		return
	}

	aggregation.handleDataMessage(queryID, fruitRecords)
	aggregation.maybeFlush(queryID)
	ack()
}

func (aggregation *Aggregation) handleEndOfRecordsMessage(queryID string) error {
	slog.Info("Received End Of Records message", "queryID", queryID)

	fruitMap, ok := aggregation.fruitItemMap[queryID]
	if !ok {
		slog.Debug("While getting fruitItemMap")
		return nil
	}

	fruitTopRecords := aggregation.buildFruitTop(fruitMap)

	message, err := inner.SerializeMessageWithID(queryID, fruitTopRecords)
	if err != nil {
		slog.Debug("While serializing top message", "err", err)
		return err
	}
	if err := aggregation.outputQueue.Send(*message); err != nil {
		slog.Debug("While sending top message", "err", err)
		return err
	}

	eofMessage, err := inner.SerializeMessageWithID(queryID, []fruititem.FruitItem{})
	if err != nil {
		slog.Debug("While serializing EOF message", "err", err)
		return err
	}

	if err := aggregation.outputQueue.Send(*eofMessage); err != nil {
		slog.Debug("While sending EOF message", "err", err)
		return err
	}

	delete(aggregation.fruitItemMap, queryID)
	delete(aggregation.totalCount, queryID)
	delete(aggregation.partialCounts, queryID)
	return nil
}

func (aggregation *Aggregation) handleDataMessage(queryID string, fruitRecords []fruititem.FruitItem) {
	if _, ok := aggregation.fruitItemMap[queryID]; !ok {
		aggregation.fruitItemMap[queryID] = map[string]fruititem.FruitItem{}
	}

	for _, fruitRecord := range fruitRecords {
		current, exists := aggregation.fruitItemMap[queryID][fruitRecord.Fruit]
		if exists {
			aggregation.fruitItemMap[queryID][fruitRecord.Fruit] = current.Sum(fruitRecord)
		} else {
			aggregation.fruitItemMap[queryID][fruitRecord.Fruit] = fruitRecord
		}
	}
}

func (aggregation *Aggregation) buildFruitTop(fruitMap map[string]fruititem.FruitItem) []fruititem.FruitItem {
	fruitItems := make([]fruititem.FruitItem, 0, len(fruitMap))

	for _, item := range fruitMap {
		fruitItems = append(fruitItems, item)
	}

	sort.SliceStable(fruitItems, func(i, j int) bool {
		return fruitItems[j].Less(fruitItems[i])
	})

	finalTopSize := min(aggregation.topSize, len(fruitItems))
	return fruitItems[:finalTopSize]
}

func (aggregation *Aggregation) maybeFlush(queryID string) {
	total := aggregation.totalCount[queryID]
	if total == nil {
		return
	}

	partials, ok := aggregation.partialCounts[queryID]
	if !ok {
		return
	}

	// valido que tengo el partial_count de todos los sums
	if len(partials) < aggregation.sumAmount {
		return
	}

	// valido que suma de parciales == total
	sum := 0
	for _, p := range partials {
		sum += p
	}

	if sum != *total {
		slog.Info("Partial counts do not match total yet, waiting for more data",
			"queryID", queryID,
			"partialSum", sum,
			"total", *total,
		)
		return
	}

	slog.Info("All data received, flushing", "queryID", queryID, "total", *total)
	if err := aggregation.handleEndOfRecordsMessage(queryID); err != nil {
		slog.Error("While handling end of record message", "err", err)
	}
}
