
using System.ComponentModel.DataAnnotations;

namespace MyFinanceApp.Models;

public class SalesOrderRequest : IValidatableObject
{
    [Required(ErrorMessage = "Не переданы настройки подключения к базе данных.")]
    public DbSettings Settings { get; set; } = new();

    [Required(ErrorMessage = "Не выбрана таблица товаров.")]
    [StringLength(64, ErrorMessage = "Название таблицы товаров слишком длинное.")]
    public string InventoryTableName { get; set; } = "parts";

    [Required]
    [StringLength(64)]
    public string SalesTableName { get; set; } = "sales";

    [Required]
    [StringLength(64)]
    public string SaleItemsTableName { get; set; } = "sale_items";

    [Required(ErrorMessage = "Укажи имя клиента.")]
    [StringLength(150, ErrorMessage = "Имя клиента не должно быть длиннее 150 символов.")]
    public string CustomerName { get; set; } = string.Empty;

    [RegularExpression(@"^[0-9+()\-\s]{7,25}$", ErrorMessage = "Телефон должен содержать от 7 до 25 символов: цифры, пробелы, +, скобки и дефисы.")]
    public string? CustomerPhone { get; set; }

    [EmailAddress(ErrorMessage = "Укажи корректный email клиента.")]
    [StringLength(150, ErrorMessage = "Email клиента не должен быть длиннее 150 символов.")]
    public string? CustomerEmail { get; set; }

    [StringLength(500, ErrorMessage = "Комментарий слишком длинный.")]
    public string? Note { get; set; }

    [Required(ErrorMessage = "Укажи способ оплаты.")]
    [StringLength(30)]
    public string PaymentMethod { get; set; } = "cash";

    [Required(ErrorMessage = "Укажи статус продажи.")]
    [StringLength(30)]
    public string Status { get; set; } = "completed";

    [MinLength(1, ErrorMessage = "Добавьте в заказ хотя бы одну позицию.")]
    public List<SalesOrderItemRequest> Items { get; set; } = new();

    public IEnumerable<ValidationResult> Validate(ValidationContext validationContext)
    {
        var allowedPaymentMethods = new[] { "cash", "card", "invoice", "transfer" };
        if (!allowedPaymentMethods.Contains(PaymentMethod, StringComparer.OrdinalIgnoreCase))
            yield return new ValidationResult("Выбран недопустимый способ оплаты.", [nameof(PaymentMethod)]);

        var allowedStatuses = new[] { "completed", "pending", "reserved" };
        if (!allowedStatuses.Contains(Status, StringComparer.OrdinalIgnoreCase))
            yield return new ValidationResult("Выбран недопустимый статус продажи.", [nameof(Status)]);
    }
}

public class SalesOrderItemRequest
{
    [StringLength(50)]
    public string? PartId { get; set; }

    [StringLength(100)]
    public string? PartNumber { get; set; }

    [Required(ErrorMessage = "Укажи название товара в заказе.")]
    [StringLength(200, ErrorMessage = "Название товара не должно быть длиннее 200 символов.")]
    public string PartName { get; set; } = string.Empty;

    [Range(typeof(decimal), "0.01", "999999999", ErrorMessage = "Цена продажи должна быть больше нуля.")]
    public decimal SalePrice { get; set; }

    [Range(typeof(decimal), "0", "999999999", ErrorMessage = "Закупочная цена не может быть отрицательной.")]
    public decimal? PurchasePrice { get; set; }

    [Range(1, 100000, ErrorMessage = "Количество товара должно быть не меньше 1.")]
    public int Quantity { get; set; }
}
