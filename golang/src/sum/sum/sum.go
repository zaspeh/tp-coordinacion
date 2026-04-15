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
	inputQueue        middleware.Middleware
	outputExchanges   []middleware.Middleware
	fruitItemMap      map[string]map[string]fruititem.FruitItem
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

	return &Sum{
		inputQueue:        inputQueue,
		outputExchanges:   outputExchanges,
		fruitItemMap:      map[string]map[string]fruititem.FruitItem{},
		sumAmount:         config.SumAmount,
		aggregationAmount: config.AggregationAmount,
		aggregationPrefix: config.AggregationPrefix,
	}, nil
}

func (sum *Sum) Run() {
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
	fruitMap, ok := sum.fruitItemMap[queryID]
	if !ok {
		return nil
	}

	for _, fruitRecord := range fruitMap {
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

	// envio EOF de esta query a todos los aggregations
	for i := 0; i < sum.aggregationAmount; i++ {
		message, err := inner.SerializeMessageWithID(queryID, []fruititem.FruitItem{})
		if err != nil {
			slog.Debug("While serializing EOF message", "err", err)
			return err
		}

		if err := sum.outputExchanges[i].Send(*message); err != nil {
			slog.Debug("While sending EOF message", "err", err)
			return err
		}
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

func (sum *Sum) propagateEndOfRecordMessage(queryID string) {
	for i := 0; i < sum.sumAmount*3; i++ {
		eofMsg, err := inner.SerializeMessageWithIDAndPropagation(queryID, []fruititem.FruitItem{}, true)
		if err != nil {
			slog.Error("While serializing propagated EOF", "err", err)
			continue
		}
		if err := sum.inputQueue.Send(*eofMsg); err != nil {
			slog.Error("While sending propagated EOF", "err", err)
		}
	}
}

func getAggregationID(fruit string, aggregationAmount int) int {
	hash := 0
	for _, c := range fruit {
		hash += int(c)
	}
	return hash % aggregationAmount
}
