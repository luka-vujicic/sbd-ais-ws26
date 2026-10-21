package model

type Order struct {
	Amount    uint64 `json:"amount"`
	CreatedAt string `json:"created_at"`
	DrinkID   uint64 `json:"drink_id"`
}