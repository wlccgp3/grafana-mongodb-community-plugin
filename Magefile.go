//go:build mage
// +build mage

package main

import (
	"fmt"
	// mage:import
	build "github.com/grafana/grafana-plugin-sdk-go/build"
	"github.com/magefile/mage/mg"
)

// Hello prints a message (shows that you can define custom Mage targets).
func Hello() {
	fmt.Println("hello plugin developer!")
}

// Default configures the default target.
// var Default = build.BuildAll
var Default = BuildLinux

func BuildLinux() {
	b := build.Build{}
	mg.Deps(b.Linux)
}
