using MyFinanceApp.Models;

namespace MyFinanceApp.Services;

public class TransactionService : ITransactionService
{
    private readonly List<Transaction> _data = new() {
        new Transaction { Id = 1, Description = "Ужин в ресторане", Amount = 1200, Category = Category.Food },
        new Transaction { Id = 2, Description = "Бензин", Amount = 3000, Category = Category.Transport },
        new Transaction { Id = 3, Description = "Квартира", Amount = 45000, Category = Category.Rent },
        new Transaction { Id = 4, Description = "Билеты в кино", Amount = 800, Category = Category.Entertainment }
    };

    public List<Transaction> GetAll() => _data;

    public Dictionary<string, double> GetCategoryData() =>
        _data.GroupBy(t => t.Category)
             .ToDictionary(g => g.Key.ToString(), g => (double)g.Sum(t => t.Amount));
}