package repository

import "ordersystem/model"

type DatabaseHandler struct {
    drinks []model.Drink
    orders []model.Order
}

func NewDatabaseHandler() *DatabaseHandler {
    drinks := []model.Drink{
        {ID: 1, Name: "Beer", Description: "Hagenberger Gold", Price: 2.00},
        {ID: 2, Name: "Spritzer", Description: "Wine with soda", Price: 1.40},
        {ID: 3, Name: "Coffee", Description: "Mifare isn't that secure ;)", Price: 0.00},
    }

    orders := []model.Order{
        {DrinkID: 1, CreatedAt: "2026-10-21T20:00:00Z", Amount: 25},
        {DrinkID: 2, CreatedAt: "2026-10-21T20:05:00Z", Amount: 15},
        {DrinkID: 3, CreatedAt: "2026-10-21T20:10:00Z", Amount: 10},
    }

    return &DatabaseHandler{
        drinks: drinks,
        orders: orders,
    }
}

func (db *DatabaseHandler) GetDrinks() []model.Drink {
    return db.drinks
}

func (db *DatabaseHandler) GetOrders() []model.Order {
    return db.orders
}

func (db *DatabaseHandler) GetTotalledOrders() map[uint64]uint64 {
    totalledOrders := make(map[uint64]uint64)
    for _, o := range db.orders {
        totalledOrders[o.DrinkID] += o.Amount
    }
    return totalledOrders
}

func (db *DatabaseHandler) AddOrder(order *model.Order) {
    db.orders = append(db.orders, *order)
}
