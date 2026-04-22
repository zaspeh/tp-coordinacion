package sum

import (
	"fmt"
	"log/slog"
	"os"
	"os/signal"
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
	sumAmount         int
	aggregationAmount int
	aggregationPrefix string
}

type msgWithAck struct {
	msg  middleware.Message
	ack  func()
	nack func()
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
		sumAmount:         config.SumAmount,
		aggregationAmount: config.AggregationAmount,
		aggregationPrefix: config.AggregationPrefix,
	}, nil
}

func (sum *Sum) Run() {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGTERM)

	dataCh := make(chan msgWithAck)
	controlCh := make(chan msgWithAck)

	go func() {
		if err := sum.inputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
			dataCh <- msgWithAck{msg, ack, nack}
		}); err != nil {
			slog.Error("input consumer failed", "err", err)
		}
	}()

	go func() {
		if err := sum.controlExchange.StartConsuming(func(msg middleware.Message, ack, nack func()) {
			controlCh <- msgWithAck{msg, ack, nack}
		}); err != nil {
			slog.Error("control consumer failed", "err", err)
		}
	}()

	for {
		select {
		case m := <-dataCh:
			sum.handleData(m.msg, m.ack, m.nack)
		case m := <-controlCh:
			sum.handleControl(m.msg, m.ack, m.nack)
		case sig := <-sigChan:
			slog.Info("Received signal, shutting down", "signal", sig)
			sum.inputQueue.Close()
			sum.controlExchange.Close()
			for i, ex := range sum.outputExchanges {
				if err := ex.Close(); err != nil {
					slog.Error("error closing aggregation exchange", "index", i, "err", err)
				}
			}
			return
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

	// me interesan las frutas de esta query
	fruitMap := sum.fruitItemMap[queryID]
	processed := sum.processedCount[queryID]

	delete(sum.fruitItemMap, queryID)
	delete(sum.processedCount, queryID)
	sum.flushedQueries[queryID] = true

	for _, fruitRecord := range fruitMap {
		aggID := getAggregationID(fruitRecord.Fruit, sum.aggregationAmount)

		if err := sum.serializeAndSendToAgg(queryID, aggID, fruitRecord); err != nil {
			return err
		}
	}

	if err := sum.serializeAndSendPartialToAllAggs(queryID, processed); err != nil {
		return err
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

	// si la query ya fue flusheada, los datos que llegan son late data -> envio delta directo
	if sum.flushedQueries[queryID] {
		return sum.sendDelta(queryID, fruitRecords)
	}

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
		if err := sum.serializeAndSendToAgg(queryID, aggID, fruitRecord); err != nil {
			return err
		}
	}

	if err := sum.serializeAndSendPartialToAllAggs(queryID, 1); err != nil { // envío 1 solo count, así agg. suma el mensaje late data
		return err
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

func (sum *Sum) serializeAndSendToAgg(queryID string, aggID int, fruitRecord fruititem.FruitItem) error {
	message, err := inner.SerializeSingleFruit(queryID, fruitRecord)
	if err != nil {
		slog.Error("While serializing message", "err", err)
		return err
	}
	if err := sum.outputExchanges[aggID].Send(*message); err != nil {
		slog.Debug("While sending message", "err", err)
		return err
	}
	return nil
}

func (sum *Sum) serializeAndSendPartialToAllAggs(queryID string, partial int) error {
	for i := 0; i < sum.aggregationAmount; i++ {
		slog.Info("Sending partial count", "queryID", queryID, "processed", partial, "sumID", sum.id)
		message, err := inner.SerializePartialMessage(queryID, partial, sum.id)
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

func getAggregationID(fruit string, aggregationAmount int) int {
	hash := 0
	for _, c := range fruit {
		hash += int(c)
	}
	return hash % aggregationAmount
}
