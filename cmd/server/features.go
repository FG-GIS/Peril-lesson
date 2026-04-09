package main

import (
	"fmt"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
)

func handlerLogs() func(routing.GameLog) pubsub.Acktype {
	log := func(log routing.GameLog) pubsub.Acktype {
		defer fmt.Print("> ")
		gamelogic.WriteLog(log)
		return pubsub.Ack
	}
	return log
}
