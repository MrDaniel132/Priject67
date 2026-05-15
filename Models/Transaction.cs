namespace MyFinanceApp.Models;

public enum Category { Food, Transport, Rent, Entertainment, Health }

public class Transaction
{
    public int Id { get; set; }
    public string Description { get; set; } = string.Empty;
    public decimal Amount { get; set; }
    public Category Category { get; set; }
}