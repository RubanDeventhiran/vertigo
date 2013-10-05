package vertigo

import (
	"log"
)

var TrafficLogger *log.Logger

const (
	AuthenticationOK                = 0
	AuthenticationKerberosV5        = 2
	AuthenticationCleartextPassword = 3
	AuthenticationCryptPassword     = 4
	AuthenticationMD5Password       = 5
	AuthenticationSCAMCredential    = 6
	AuthenticationGSS               = 7
	AuthenticationGSSContinue       = 8
	AuthenticationSSPI              = 9
)
