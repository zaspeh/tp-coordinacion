package sum

import (
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

type SumConfig struct {
	Id                int
	MomHost           string
	MomPort           int
	InputQueue        string
	SumAmount         int
	SumPrefix         string
	AggregationAmount int
	AggregationPrefix string
}

type Sum struct {
	id                int
	inputQueue        middleware.Middleware
	outputExchanges   []middleware.Middleware
	controlExchange   middleware.Middleware
	fruitItemMap      map[string]map[string]fruititem.FruitItem
	processedCount    map[string]int
	flushedQueries    map[string]bool
	fruitMapLock      sync.Mutex
	sumAmount         int
	aggregationAmount int
	aggregationPrefix string
}

func NewSum(config SumConfig) (*Sum, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	inputQueue, err := middleware.CreateQueueMiddleware(config.InputQueue, connSettings)
	if err != nil {
		return nil, err
	}

	outputExchanges := make([]middleware.Middleware, config.AggregationAmount)

	for i := 0; i < config.AggregationAmount; i++ {
		key := fmt.Sprintf("%s_%d", config.AggregationPrefix, i)

		exchange, err := middleware.CreateExchangeMiddleware(
			config.AggregationPrefix,
			[]string{key},
			connSettings,
		)
		if err != nil {
			inputQueue.Close()
			for j := 0; j < i; j++ {
				outputExchanges[j].Close()
			}
			return nil, err
		}

		outputExchanges[i] = exchange
	}

	controlKeys := make([]string, 0, config.SumAmount-1)
	for i := 0; i < config.SumAmount; i++ {
		if i == config.Id {
			continue
		}
		controlKeys = append(controlKeys, fmt.Sprintf("%s_%d", config.SumPrefix, i))
	}

	controlExchange, err := middleware.CreateExchangeMiddleware(
		config.SumPrefix,
		controlKeys,
		connSettings,
	)
	if err != nil {
		inputQueue.Close()
		for _, ex := range outputExchanges {
			ex.Close()
		}
		return nil, err
	}

	return &Sum{
		id:                config.Id,
		inputQueue:        inputQueue,
		outputExchanges:   outputExchanges,
		controlExchange:   controlExchange,
		fruitItemMap:      map[string]map[string]fruititem.FruitItem{},
		processedCount:    map[string]int{},
		flushedQueries:    map[string]bool{},
		fruitMapLock:      sync.Mutex{},
		sumAmount:         config.SumAmount,
		aggregationAmount: config.AggregationAmount,
		aggregationPrefix: config.AggregationPrefix,
	}, nil
}

func (sum *Sum) Run() {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGTERM)

	go func() {
		if err := sum.controlExchange.StartConsuming(func(msg middleware.Message, ack, nack func()) {
			sum.handleControl(msg, ack, nack)
		}); err != nil {
			slog.Error("control consumer failed", "err", err)
		}
	}()

	go func() {
		if err := sum.inputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
			sum.handleData(msg, ack, nack)
		}); err != nil {
			slog.Error("input consumer failed", "err", err)
		}
	}()

	// me quedo esperando hasta sigterm
	sig := <-sigChan
	slog.Info("Received signal, shutting down", "signal", sig)

	sum.inputQueue.Close()
	sum.controlExchange.Close()
	for i, ex := range sum.outputExchanges {
		if err := ex.Close(); err != nil {
			slog.Error("error closing aggregation exchange", "index", i, "err", err)
		}
	}
}

func (sum *Sum) handleData(msg middleware.Message, ack func(), nack func()) {
	queryID, fruitRecords, isEof, propagated, totalCount, err := inner.DeserializeMessageWithID(&msg)
	if err != nil {
		slog.Error("While deserializing message", "err", err)
		nack()
		return
	}

	if isEof {
		if sum.sumAmount > 1 && !propagated {
			if err := sum.propagateEndOfRecordMessage(queryID); err != nil {
				slog.Error("While propagating EOF", "err", err)
				nack()
				return
			}
		}

		if err := sum.handleEndOfRecordMessage(queryID, totalCount); err != nil {
			slog.Error("While handling end of record message", "err", err)
			nack()
			return
		}
		ack()
		return
	}

	if err := sum.handleDataMessage(queryID, fruitRecords); err != nil {
		slog.Error("While handling data message", "err", err)
		nack()
		return
	}
	ack()
}

func (sum *Sum) handleControl(msg middleware.Message, ack func(), nack func()) {
	queryID, _, isEof, _, _, err := inner.DeserializeMessageWithID(&msg)
	if err != nil {
		slog.Error("While deserializing message", "err", err)
		nack()
		return
	}

	if isEof {
		// este EOF viene de otra instancia de Sum
		if err := sum.handleEndOfRecordMessage(queryID, nil); err != nil {
			slog.Error("While handling end of record message", "err", err)
			nack()
			return
		}
	}
	ack()
}

func (sum *Sum) handleEndOfRecordMessage(queryID string, totalCount *int) error {
	slog.Info("Received End Of Records message")

	sum.fruitMapLock.Lock()

	// me interesan las frutas de esta query
	fruitMap, ok := sum.fruitItemMap[queryID]
	if !ok {
		sum.fruitMapLock.Unlock()
		return nil
	}

	// copio los datos
	localCopy := make([]fruititem.FruitItem, 0, len(fruitMap))
	for _, v := range fruitMap {
		localCopy = append(localCopy, v)
	}

	processed := sum.processedCount[queryID]

	delete(sum.fruitItemMap, queryID)
	delete(sum.processedCount, queryID)
	sum.flushedQueries[queryID] = true

	sum.fruitMapLock.Unlock()

	for _, fruitRecord := range localCopy {
		aggID := getAggregationID(fruitRecord.Fruit, sum.aggregationAmount)

		message, err := inner.SerializeMessageWithID(queryID, []fruititem.FruitItem{fruitRecord})
		if err != nil {
			slog.Debug("While serializing message", "err", err)
			return err
		}

		if err := sum.outputExchanges[aggID].Send(*message); err != nil {
			slog.Debug("While sending message", "err", err)
			return err
		}
	}

	for i := 0; i < sum.aggregationAmount; i++ {
		slog.Info("Sending partial count", "queryID", queryID, "processed", processed, "sumID", sum.id)
		message, err := inner.SerializePartialMessage(queryID, processed, sum.id)
		if err != nil {
			slog.Error("While serializing Partial Message", "err", err)
			return err
		}
		if err := sum.outputExchanges[i].Send(*message); err != nil {
			slog.Error("While sending Partial Message", "err", err)
			return err
		}
	}

	for i := 0; i < sum.aggregationAmount; i++ {
		message, err := inner.SerializeMessageWithIDAndPropagationAndTotal(queryID, []fruititem.FruitItem{}, false, totalCount)
		if err != nil {
			slog.Error("While serializing EOF", "err", err)
			return err
		}
		if err := sum.outputExchanges[i].Send(*message); err != nil {
			slog.Error("While sending EOF", "err", err)
			return err
		}
	}

	return nil
}

func (sum *Sum) handleDataMessage(queryID string, fruitRecords []fruititem.FruitItem) error {
	sum.fruitMapLock.Lock()
	flushed := sum.flushedQueries[queryID]
	sum.fruitMapLock.Unlock()

	// si la query ya fue flusheada, los datos que llegan son late data -> envio delta directo
	if flushed {
		sum.fruitMapLock.Lock()
		sum.processedCount[queryID]++
		sum.fruitMapLock.Unlock()
		return sum.sendDelta(queryID, fruitRecords)
	}

	sum.fruitMapLock.Lock()
	defer sum.fruitMapLock.Unlock()

	if _, ok := sum.fruitItemMap[queryID]; !ok {
		sum.fruitItemMap[queryID] = map[string]fruititem.FruitItem{}
	}

	for _, fruitRecord := range fruitRecords {
		current, exists := sum.fruitItemMap[queryID][fruitRecord.Fruit]
		if exists {
			sum.fruitItemMap[queryID][fruitRecord.Fruit] = current.Sum(fruitRecord)
		} else {
			sum.fruitItemMap[queryID][fruitRecord.Fruit] = fruitRecord
		}
	}

	sum.processedCount[queryID]++

	return nil
}

func (sum *Sum) sendDelta(queryID string, fruitRecords []fruititem.FruitItem) error {
	for _, fruitRecord := range fruitRecords {
		aggID := getAggregationID(fruitRecord.Fruit, sum.aggregationAmount)
		message, err := inner.SerializeMessageWithID(queryID, []fruititem.FruitItem{fruitRecord})
		if err != nil {
			slog.Debug("While serializing message", "err", err)
			return err
		}
		if err := sum.outputExchanges[aggID].Send(*message); err != nil {
			slog.Debug("While sending message", "err", err)
			return err
		}
	}

	sum.fruitMapLock.Lock()
	processed := sum.processedCount[queryID]
	sum.fruitMapLock.Unlock()

	for i := 0; i < sum.aggregationAmount; i++ {
		message, err := inner.SerializePartialMessage(queryID, processed, sum.id)
		if err != nil {
			slog.Error("While serializing Partial Message", "err", err)
			return err
		}
		if err := sum.outputExchanges[i].Send(*message); err != nil {
			slog.Error("While sending Partial Message", "err", err)
			return err
		}
	}

	return nil
}

func (sum *Sum) propagateEndOfRecordMessage(queryID string) error {
	eofMsg, err := inner.SerializeMessageWithIDAndPropagation(queryID, []fruititem.FruitItem{}, true)
	if err != nil {
		slog.Error("While serializing propagated EOF", "err", err)
		return err
	}

	if err := sum.controlExchange.Send(*eofMsg); err != nil {
		slog.Error("While sending propagated EOF", "err", err)
		return err
	}

	return nil
}

func getAggregationID(fruit string, aggregationAmount int) int {
	hash := 0
	for _, c := range fruit {
		hash += int(c)
	}
	return hash % aggregationAmount
}
