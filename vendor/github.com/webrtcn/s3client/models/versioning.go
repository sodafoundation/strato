package models

import "encoding/xml"

//VersioningStatus Versioning Configuration for bucket
type VersioningStatus string

//Sets the versioning state of the bucket. Valid Values: Suspended/Enabled
const (
	VersioningEnabled   = VersioningStatus("Enabled")
	VersioningSuspended = VersioningStatus("Suspended")
)

//Versioning version
type Versioning struct {
	XMLName xml.Name         `xml:"VersioningConfiguration"`
	Status  VersioningStatus `xml:"Status"`
}
