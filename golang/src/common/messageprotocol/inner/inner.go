package inner

import (
	"encoding/json"
	"errors"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

type envelope struct {
	QueryID    string          `json:"query_id"`
	Data       [][]interface{} `json:"data"`
	Propagated bool            `json:"propagated,omitempty"`

	TotalCount   *int `json:"total_count,omitempty"`   // viene del EOF del cliente
	PartialCount *int `json:"partial_count,omitempty"` // viene de cada sum
	SumID        *int `json:"sum_id,omitempty"`        // para seguimiento en aggregation
}

func SerializeMessageWithID(queryID string, fruitRecords []fruititem.FruitItem) (*middleware.Message, error) {
	return SerializeMessageWithIDAndPropagationAndTotal(queryID, fruitRecords, false, nil)
}

func SerializeMessageWithIDAndPropagation(queryID string, fruitRecords []fruititem.FruitItem, propagated bool) (*middleware.Message, error) {
	return SerializeMessageWithIDAndPropagationAndTotal(queryID, fruitRecords, propagated, nil)
}

func SerializeSingleFruit(queryID string, fruit fruititem.FruitItem) (*middleware.Message, error) {
	return SerializeMessageWithID(queryID, []fruititem.FruitItem{fruit})
}

func SerializeMessageWithIDAndPropagationAndTotal(
	queryID string,
	fruitRecords []fruititem.FruitItem,
	propagated bool,
	totalCount *int,
) (*middleware.Message, error) {

	data := [][]interface{}{}

	for _, fruitRecord := range fruitRecords {
		datum := []interface{}{
			fruitRecord.Fruit,
			fruitRecord.Amount,
		}
		data = append(data, datum)
	}

	env := envelope{
		QueryID:    queryID,
		Data:       data,
		Propagated: propagated,
		TotalCount: totalCount,
	}

	body, err := json.Marshal(env)
	if err != nil {
		return nil, err
	}

	message := middleware.Message{Body: string(body)}
	return &message, nil
}

func SerializePartialMessage(queryID string, partial int, sumID int) (*middleware.Message, error) {
	env := envelope{
		QueryID:      queryID,
		Data:         [][]interface{}{},
		PartialCount: &partial,
		SumID:        &sumID,
	}

	body, err := json.Marshal(env)
	if err != nil {
		return nil, err
	}

	return &middleware.Message{Body: string(body)}, nil
}

func DeserializeMessageWithID(message *middleware.Message) (string, []fruititem.FruitItem, bool, bool, *int, error) {
	var env envelope

	if err := json.Unmarshal([]byte(message.Body), &env); err != nil {
		return "", nil, false, false, nil, err
	}

	fruitRecords := []fruititem.FruitItem{}

	for _, datum := range env.Data {
		if len(datum) != 2 {
			return "", nil, false, false, nil, errors.New("Datum is not an array")
		}

		fruit, ok := datum[0].(string)
		if !ok {
			return "", nil, false, false, nil, errors.New("Datum is not a (fruit, amount) pair")
		}

		amount, ok := datum[1].(float64)
		if !ok {
			return "", nil, false, false, nil, errors.New("Datum is not a (fruit, amount) pair")
		}

		fruitRecords = append(fruitRecords, fruititem.FruitItem{
			Fruit:  fruit,
			Amount: uint32(amount),
		})
	}

	isEOF := len(fruitRecords) == 0

	return env.QueryID, fruitRecords, isEOF, env.Propagated, env.TotalCount, nil
}

func DeserializeFullMessage(message *middleware.Message) (
	string,
	[]fruititem.FruitItem,
	bool,
	bool,
	*int,
	*int,
	*int,
	error,
) {
	var env envelope

	if err := json.Unmarshal([]byte(message.Body), &env); err != nil {
		return "", nil, false, false, nil, nil, nil, err
	}

	fruitRecords := []fruititem.FruitItem{}

	for _, datum := range env.Data {
		if len(datum) != 2 {
			return "", nil, false, false, nil, nil, nil, errors.New("invalid datum")
		}

		fruit := datum[0].(string)
		amount := uint32(datum[1].(float64))

		fruitRecords = append(fruitRecords, fruititem.FruitItem{
			Fruit:  fruit,
			Amount: amount,
		})
	}

	isEOF := len(fruitRecords) == 0

	return env.QueryID, fruitRecords, isEOF, env.Propagated, env.TotalCount, env.PartialCount, env.SumID, nil
}
