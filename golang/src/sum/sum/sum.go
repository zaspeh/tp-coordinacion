package sum

import (
	"fmt"
	"log/slog"

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
	inputQueue     middleware.Middleware
	outputExchange middleware.Middleware
	fruitItemMap   map[string]map[string]fruititem.FruitItem
}

func NewSum(config SumConfig) (*Sum, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	inputQueue, err := middleware.CreateQueueMiddleware(config.InputQueue, connSettings)
	if err != nil {
		return nil, err
	}

	outputExchangeRouteKeys := make([]string, config.AggregationAmount)
	for i := range config.AggregationAmount {
		outputExchangeRouteKeys[i] = fmt.Sprintf("%s_%d", config.AggregationPrefix, i)
	}

	outputExchange, err := middleware.CreateExchangeMiddleware(config.AggregationPrefix, outputExchangeRouteKeys, connSettings)
	if err != nil {
		inputQueue.Close()
		return nil, err
	}

	return &Sum{
		inputQueue:     inputQueue,
		outputExchange: outputExchange,
		fruitItemMap:   map[string]map[string]fruititem.FruitItem{},
	}, nil
}

func (sum *Sum) Run() {
	sum.inputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		sum.handleMessage(msg, ack, nack)
	})
}

func (sum *Sum) handleMessage(msg middleware.Message, ack func(), nack func()) {
	defer ack()

	queryID, fruitRecords, isEof, err := inner.DeserializeMessageWithID(&msg)
	if err != nil {
		slog.Error("While deserializing message", "err", err)
		return
	}

	if isEof {
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
	fruitMap, ok := sum.fruitItemMap[queryID]
	if !ok {
		return nil
	}

	for _, fruitRecord := range fruitMap {
		message, err := inner.SerializeMessageWithID(queryID, []fruititem.FruitItem{fruitRecord})
		if err != nil {
			slog.Debug("While serializing message", "err", err)
			return err
		}

		if err := sum.outputExchange.Send(*message); err != nil {
			slog.Debug("While sending message", "err", err)
			return err
		}
	}

	// envio EOF solo de esta query
	message, err := inner.SerializeMessageWithID(queryID, []fruititem.FruitItem{})
	if err != nil {
		slog.Debug("While serializing EOF message", "err", err)
		return err
	}

	if err := sum.outputExchange.Send(*message); err != nil {
		slog.Debug("While sending EOF message", "err", err)
		return err
	}

	delete(sum.fruitItemMap, queryID)

	return nil
}

func (sum *Sum) handleDataMessage(queryID string, fruitRecords []fruititem.FruitItem) error {
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
