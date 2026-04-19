package sum

import (
	"fmt"
	"log/slog"
	"sync"

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
	inputQueue        middleware.Middleware
	outputExchanges   []middleware.Middleware
	controlExchange   middleware.Middleware
	fruitItemMap      map[string]map[string]fruititem.FruitItem
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

	controlKeys := make([]string, config.SumAmount)
	for i := 0; i < config.SumAmount; i++ {
		controlKeys[i] = fmt.Sprintf("%s_%d", config.SumPrefix, i)
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
		inputQueue:        inputQueue,
		outputExchanges:   outputExchanges,
		controlExchange:   controlExchange,
		fruitItemMap:      map[string]map[string]fruititem.FruitItem{},
		fruitMapLock:      sync.Mutex{},
		sumAmount:         config.SumAmount,
		aggregationAmount: config.AggregationAmount,
		aggregationPrefix: config.AggregationPrefix,
	}, nil
}

func (sum *Sum) Run() {
	go sum.controlExchange.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		sum.handleMessage(msg, ack, nack)
	})

	sum.inputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		sum.handleMessage(msg, ack, nack)
	})
}

func (sum *Sum) handleMessage(msg middleware.Message, ack func(), nack func()) {
	defer ack()

	queryID, fruitRecords, isEof, propagated, err := inner.DeserializeMessageWithID(&msg)
	if err != nil {
		slog.Error("While deserializing message", "err", err)
		return
	}

	if isEof {

		if !propagated {
			sum.propagateEndOfRecordMessage(queryID)
		}

		if err := sum.handleEndOfRecordMessage(queryID); err != nil {
			slog.Error("While handling end of record message", "err", err)
		}
		return
	}

	if err := sum.handleDataMessage(queryID, fruitRecords); err != nil {
		slog.Error("While handling data message", "err", err)
	}
}

func (sum *Sum) handleEndOfRecordMessage(queryID string) error {
	slog.Info("Received End Of Records message")

	// me interesan las frutas de esta query
	sum.fruitMapLock.Lock()

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

	delete(sum.fruitItemMap, queryID)

	sum.fruitMapLock.Unlock()

	for _, fruitRecord := range localCopy {
		aggID := getAggregationID(fruitRecord.Fruit, sum.aggregationAmount)

		message, err := inner.SerializeMessageWithID(queryID, []fruititem.FruitItem{fruitRecord})
		if err != nil {
			return err
		}

		if err := sum.outputExchanges[aggID].Send(*message); err != nil {
			return err
		}
	}

	// EOF
	for i := 0; i < sum.aggregationAmount; i++ {
		message, _ := inner.SerializeMessageWithID(queryID, []fruititem.FruitItem{})
		sum.outputExchanges[i].Send(*message)
	}

	return nil
}

func (sum *Sum) handleDataMessage(queryID string, fruitRecords []fruititem.FruitItem) error {
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

	return nil
}

func (sum *Sum) propagateEndOfRecordMessage(queryID string) {
	eofMsg, err := inner.SerializeMessageWithIDAndPropagation(queryID, []fruititem.FruitItem{}, true)
	if err != nil {
		slog.Error("While serializing propagated EOF", "err", err)
		return
	}

	if err := sum.controlExchange.Send(*eofMsg); err != nil {
		slog.Error("While sending propagated EOF", "err", err)
	}
}

func getAggregationID(fruit string, aggregationAmount int) int {
	hash := 0
	for _, c := range fruit {
		hash += int(c)
	}
	return hash % aggregationAmount
}
