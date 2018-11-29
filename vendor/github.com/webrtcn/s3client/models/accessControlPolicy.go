package models

import "encoding/xml"

//AccessControlPolicy Access Control Policy
type AccessControlPolicy struct {
	XMLName           xml.Name `xml:"AccessControlPolicy"`
	Owner             Owner
	AccessControlList AccessControlList
}

//AccessControlList Access Control List
type AccessControlList struct {
	Grant []Grant
}

//Grant Grant
type Grant struct {
	Grantee    Grantee
	Permission string
}

//Grantee Grantee
type Grantee struct {
	URI         string
	UserID      string
	DisplayName string
}
