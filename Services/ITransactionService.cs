using MyFinanceApp.Models;

namespace MyFinanceApp.Services;

public interface ITransactionService
{
    List<Transaction> GetAll();
    Dictionary<string, double> GetCategoryData();
}