package models 

import (
	"encoding/xml"
)
type ErrorResult struct {
	XMLName xml.Name   	`xml:"Error"`
	Code string 		`xml:"Code"`
}