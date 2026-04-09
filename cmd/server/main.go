package main

import (
	"fmt"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	fmt.Println("Starting Peril server...")
	connection := "amqp://guest:guest@localhost:5672/"
	conn, err := amqp.Dial(connection)
	if err != nil {
		fmt.Println("Error when Dialing connection.")
		return
	}
	defer conn.Close()
	fmt.Println("Connection succedeed.")

	newChannel, err := conn.Channel()
	if err != nil {
		fmt.Println("Error creating new channel.")
		return
	}

	_, _, err = pubsub.DeclareAndBind(conn, routing.ExchangePerilTopic, routing.GameLogSlug, "game_logs.*", pubsub.Durable)
	if err != nil {
		fmt.Println("Error binding to queue: ", err)
	}
	err = pubsub.Subscribe(conn, routing.ExchangePerilTopic, routing.GameLogSlug, "game_logs.*", pubsub.Durable, handlerLogs(), pubsub.UnmarshalGob)

	if err != nil {
		fmt.Println("Error binding to queue: ", "game_logs")
		return
	}

	gamelogic.PrintServerHelp()
	for run := true; run; {
		input := gamelogic.GetInput()
		if len(input) == 0 {
			continue
		}
		switch input[0] {
		case "pause":
			fmt.Println("Sending pause message.")
			pubsub.PublishJSON(newChannel, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{IsPaused: true})
		case "resume":
			fmt.Println("Sending resume message.")
			pubsub.PublishJSON(newChannel, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{IsPaused: false})
		case "quit":
			fmt.Println("Shutting down.")
			run = false
		default:
			fmt.Println("Command unavailable or inexistant.")
		}

	}
}
