package fred

type fredObservation struct {
	Date  string `json:"date"`
	Value string `json:"value"`
}

type fredResponse struct {
	Observations []fredObservation `json:"observations"`
}
