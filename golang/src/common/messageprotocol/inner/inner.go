package inner

import (
	"encoding/json"
	"errors"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

type envelope struct {
	QueryID string          `json:"query_id"`
	Data    [][]interface{} `json:"data"`
}

func serializeJson(message []interface{}) ([]byte, error) {
	return json.Marshal(message)
}

func deserializeJson(message []byte) ([]interface{}, error) {
	var data []interface{}
	if err := json.Unmarshal(message, &data); err != nil {
		return nil, err
	}
	return data, nil
}

func SerializeMessageWithID(queryID string, fruitRecords []fruititem.FruitItem) (*middleware.Message, error) {
	data := [][]interface{}{}

	for _, fruitRecord := range fruitRecords {
		datum := []interface{}{
			fruitRecord.Fruit,
			fruitRecord.Amount,
		}
		data = append(data, datum)
	}

	env := envelope{
		QueryID: queryID,
		Data:    data,
	}

	body, err := json.Marshal(env)
	if err != nil {
		return nil, err
	}

	message := middleware.Message{Body: string(body)}
	return &message, nil
}

func DeserializeMessageWithID(message *middleware.Message) (string, []fruititem.FruitItem, bool, error) {
	var env envelope

	if err := json.Unmarshal([]byte(message.Body), &env); err != nil {
		return "", nil, false, err
	}

	fruitRecords := []fruititem.FruitItem{}

	for _, datum := range env.Data {
		if len(datum) != 2 {
			return "", nil, false, errors.New("Datum is not an array")
		}

		fruit, ok := datum[0].(string)
		if !ok {
			return "", nil, false, errors.New("Datum is not a (fruit, amount) pair")
		}

		amount, ok := datum[1].(float64)
		if !ok {
			return "", nil, false, errors.New("Datum is not a (fruit, amount) pair")
		}

		fruitRecords = append(fruitRecords, fruititem.FruitItem{
			Fruit:  fruit,
			Amount: uint32(amount),
		})
	}

	return env.QueryID, fruitRecords, len(fruitRecords) == 0, nil
}
