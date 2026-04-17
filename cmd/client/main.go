package main

import (
	"fmt"
	"strconv"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"

	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	fmt.Println("Starting Peril client...")
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

	username, err := gamelogic.ClientWelcome()
	if err != nil {
		fmt.Println("Error getting username.")
		return
	}

	namePause := routing.PauseKey + "." + username

	gamestate := gamelogic.NewGameState(username)

	err = pubsub.Subscribe(conn, routing.ExchangePerilDirect, namePause, routing.PauseKey, pubsub.Transient, handlerPause(gamestate), pubsub.UnmarshalJSON)
	if err != nil {
		fmt.Println("Error binding to queue: ", namePause)
	}
	nameMove := routing.ArmyMovesPrefix + "." + username

	err = pubsub.Subscribe(conn, routing.ExchangePerilTopic, nameMove, routing.ArmyMovesPrefix+".*", pubsub.Transient, handlerMove(gamestate, newChannel), pubsub.UnmarshalJSON)

	if err != nil {
		fmt.Println("Error binding to queue: ", nameMove)
		return
	}

	err = pubsub.Subscribe(conn, routing.ExchangePerilTopic, routing.WarRecognitionsPrefix, routing.WarRecognitionsPrefix+".*", pubsub.Durable, handlerWar(gamestate, newChannel), pubsub.UnmarshalJSON)

	if err != nil {
		fmt.Println("Error binding to queue: ", "War")
		return
	}

	for run := true; run; {
		input := gamelogic.GetInput()
		if len(input) == 0 {
			continue
		}
		switch input[0] {
		case "spawn":
			err := gamestate.CommandSpawn(input)
			if err != nil {
				fmt.Println("Error spawning unit: ", err)
			}
		case "move":
			move, err := gamestate.CommandMove(input)
			if err != nil {
				fmt.Println("Moving error: ", err)
				return
			} else {
				fmt.Println("Successful move")
			}
			err = pubsub.PublishJSON(newChannel, routing.ExchangePerilTopic, routing.ArmyMovesPrefix+"."+move.Player.Username, move)
			if err != nil {
				fmt.Println("Error publishing move: ", err)
				return
			} else {
				fmt.Println("Publisjed successfully.")
			}

		case "status":
			gamestate.CommandStatus()
		case "help":
			gamelogic.PrintClientHelp()
		case "spam":
			if len(input) < 2 || input[1] == "" {
				break
			}
			spamAmount, err := strconv.Atoi(input[1])
			if err != nil {
				fmt.Println("Error converting input spam amount: ", err)
				return
			}
			// key := "game_logs." + gamestate.GetUsername()
			for range spamAmount {
				err = pubsub.PubGLog(newChannel, gamestate.GetUsername(), gamelogic.GetMaliciousLog())
				// err = pubsub.PublishJSON(newChannel, routing.ExchangePerilTopic, key, gamelogic.GetMaliciousLog())
				if err != nil {
					fmt.Println("Error Publishing malicious log: ", err)
					return
				}
			}
		case "quit":
			gamelogic.PrintQuit()
			run = false
		default:
			fmt.Println("Unknown command.")
		}
	}

}
