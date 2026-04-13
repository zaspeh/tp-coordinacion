package aggregation

import (
	"fmt"
	"log/slog"
	"sort"

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
	eofCount      map[string]int
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
		eofCount:      map[string]int{},
	}, nil
}

func (aggregation *Aggregation) Run() {
	aggregation.inputExchange.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		aggregation.handleMessage(msg, ack, nack)
	})
}

func (aggregation *Aggregation) handleMessage(msg middleware.Message, ack func(), nack func()) {
	defer ack()

	queryID, fruitRecords, isEof, _, err := inner.DeserializeMessageWithID(&msg)
	if err != nil {
		slog.Error("While deserializing message", "err", err)
		return
	}

	if isEof {
		aggregation.eofCount[queryID]++

		slog.Info("End of record message received",
			"queryID", queryID,
			"count", aggregation.eofCount[queryID],
			"expected", aggregation.sumAmount,
		)

		if aggregation.eofCount[queryID] == aggregation.sumAmount {
			if err := aggregation.handleEndOfRecordsMessage(queryID); err != nil {
				slog.Error("While handling end of record message", "err", err)
			}
			delete(aggregation.eofCount, queryID)
		}

		return
	}

	aggregation.handleDataMessage(queryID, fruitRecords)
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
