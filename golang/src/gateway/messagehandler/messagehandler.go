package messagehandler

import (
	"fmt"
	"log/slog"
	"time"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

type MessageHandler struct {
	id string
}

func NewMessageHandler() MessageHandler {
	return MessageHandler{
		id: fmt.Sprintf("%d", time.Now().UnixNano()),
	}
}

func (messageHandler *MessageHandler) SerializeDataMessage(fruitRecord fruititem.FruitItem) (*middleware.Message, error) {
	data := []fruititem.FruitItem{fruitRecord}
	return inner.SerializeMessageWithID(messageHandler.id, data)
}

func (messageHandler *MessageHandler) SerializeEOFMessage() (*middleware.Message, error) {
	data := []fruititem.FruitItem{}
	return inner.SerializeMessageWithID(messageHandler.id, data)
}

func (messageHandler *MessageHandler) DeserializeResultMessage(message *middleware.Message) ([]fruititem.FruitItem, error) {
	queryID, fruitRecords, _, _, err := inner.DeserializeMessageWithID(message)
	if err != nil {
		return nil, err
	}
	slog.Info("Received message", "queryID", queryID, "handlerID", messageHandler.id)

	if queryID != messageHandler.id { // este mensaje no es para este cliene
		return nil, nil
	}

	return fruitRecords, nil
}
