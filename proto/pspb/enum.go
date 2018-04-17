package pspb

// ActionResponseCode api actionResponseCode enum
type ActionResponseCode = int32

const (
	ActionRESPCODENOTLEADER    ActionResponseCode = 0
	ActionRESPCODENOLEADER                        = 1
	ActionRESPCODENOPARTITION                     = 2
	ActionRESPCODEKEYEXISTS                       = 3
	ActionRESPCODEKEYNOTEXISTS                    = 4
	ActionRESPCODEOK                              = 200
	ActionRESPCODETIMEOUT                         = 504
)
