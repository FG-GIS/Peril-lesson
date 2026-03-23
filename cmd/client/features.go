package main

import (
	"fmt"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
)

func handlerPause(gs *gamelogic.GameState) func(routing.PlayingState) {
	pause := func(ps routing.PlayingState) {
		defer fmt.Print("> ")
		gs.HandlePause(ps)
	}
	return pause
}

func handlerMove(gs *gamelogic.GameState) func(mv gamelogic.ArmyMove) {
	pause := func(mv gamelogic.ArmyMove) {
		defer fmt.Print("> ")
		gs.HandleMove(mv)
	}
	return pause
}
