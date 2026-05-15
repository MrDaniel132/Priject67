
using System.ComponentModel.DataAnnotations;

namespace MyFinanceApp.Models;

public class InventoryTableRequest
{
    [Required(ErrorMessage = "Не переданы настройки подключения к базе данных.")]
    public DbSettings Settings { get; set; } = new();

    [Required(ErrorMessage = "Не выбрана таблица товаров.")]
    [StringLength(64, ErrorMessage = "Название таблицы товаров слишком длинное.")]
    public string InventoryTableName { get; set; } = "parts";
}

public class ProductCardRequest : InventoryTableRequest, IValidatableObject
{
    [StringLength(50)]
    public string? PartId { get; set; }

    [StringLength(100)]
    public string? PartNumber { get; set; }

    [Range(1, 100, ErrorMessage = "Количество движений должно быть от 1 до 100.")]
    public int MovementsLimit { get; set; } = 20;

    public IEnumerable<ValidationResult> Validate(ValidationContext validationContext)
    {
        if (string.IsNullOrWhiteSpace(PartId) && string.IsNullOrWhiteSpace(PartNumber))
            yield return new ValidationResult("Нужно передать идентификатор товара или артикул.", [nameof(PartId), nameof(PartNumber)]);
    }
}

public class PurchaseCreateRequest : InventoryTableRequest, IValidatableObject
{
    [Required]
    [StringLength(64)]
    public string PurchasesTableName { get; set; } = "purchases";

    [Required]
    [StringLength(64)]
    public string PurchaseItemsTableName { get; set; } = "purchase_items";

    [Required]
    [StringLength(64)]
    public string MovementsTableName { get; set; } = "inventory_movements";

    [Required(ErrorMessage = "Укажи поставщика.")]
    [StringLength(150, ErrorMessage = "Название поставщика не должно быть длиннее 150 символов.")]
    public string SupplierName { get; set; } = string.Empty;

    [RegularExpression(@"^[0-9+()\-\s]{7,25}$", ErrorMessage = "Телефон поставщика должен содержать от 7 до 25 символов: цифры, пробелы, +, скобки и дефисы.")]
    public string? SupplierPhone { get; set; }

    [StringLength(100, ErrorMessage = "Номер документа не должен быть длиннее 100 символов.")]
    public string? DocumentNumber { get; set; }

    [StringLength(500, ErrorMessage = "Комментарий слишком длинный.")]
    public string? Note { get; set; }

    [Required(ErrorMessage = "Укажи статус закупки.")]
    [StringLength(30)]
    public string Status { get; set; } = "received";

    [MinLength(1, ErrorMessage = "Добавь хотя бы одну позицию в закупку.")]
    public List<PurchaseItemRequest> Items { get; set; } = new();

    public IEnumerable<ValidationResult> Validate(ValidationContext validationContext)
    {
        var allowedStatuses = new[] { "received", "pending" };
        if (!allowedStatuses.Contains(Status, StringComparer.OrdinalIgnoreCase))
            yield return new ValidationResult("Выбран недопустимый статус закупки.", [nameof(Status)]);
    }
}

public class PurchaseItemRequest
{
    [StringLength(50)]
    public string? PartId { get; set; }

    [StringLength(100)]
    public string? PartNumber { get; set; }

    [Required(ErrorMessage = "Укажи название товара в закупке.")]
    [StringLength(200, ErrorMessage = "Название товара не должно быть длиннее 200 символов.")]
    public string PartName { get; set; } = string.Empty;

    [Range(typeof(decimal), "0.01", "999999999", ErrorMessage = "Цена закупки должна быть больше нуля.")]
    public decimal PurchasePrice { get; set; }

    [Range(1, 100000, ErrorMessage = "Количество товара должно быть не меньше 1.")]
    public int Quantity { get; set; }
}

public class PurchaseHistoryRequest : InventoryTableRequest
{
    [Required]
    [StringLength(64)]
    public string PurchasesTableName { get; set; } = "purchases";

    [Required]
    [StringLength(64)]
    public string PurchaseItemsTableName { get; set; } = "purchase_items";

    [StringLength(100, ErrorMessage = "Поисковый запрос слишком длинный.")]
    public string? Search { get; set; }

    [Range(1, 200, ErrorMessage = "Лимит должен быть в диапазоне от 1 до 200.")]
    public int? Limit { get; set; }
}

public class ReturnCreateRequest : InventoryTableRequest
{
    [Required]
    [StringLength(64)]
    public string SalesTableName { get; set; } = "sales";

    [Required]
    [StringLength(64)]
    public string SaleItemsTableName { get; set; } = "sale_items";

    [Required]
    [StringLength(64)]
    public string SalesReturnsTableName { get; set; } = "sales_returns";

    [Required]
    [StringLength(64)]
    public string SalesReturnItemsTableName { get; set; } = "sales_return_items";

    [Required]
    [StringLength(64)]
    public string MovementsTableName { get; set; } = "inventory_movements";

    [Range(1, int.MaxValue, ErrorMessage = "Не выбран заказ для возврата.")]
    public int SaleId { get; set; }

    [StringLength(150, ErrorMessage = "Имя клиента не должно быть длиннее 150 символов.")]
    public string? CustomerName { get; set; }

    [StringLength(500, ErrorMessage = "Комментарий к возврату слишком длинный.")]
    public string? Note { get; set; }

    [MinLength(1, ErrorMessage = "Добавь хотя бы одну позицию для возврата.")]
    public List<ReturnItemRequest> Items { get; set; } = new();

    public string? Reason { get; set; }
}

public class ReturnItemRequest
{
    [Range(1, int.MaxValue, ErrorMessage = "Выбери позицию заказа для возврата.")]
    public int SaleItemId { get; set; }

    [Range(1, 100000, ErrorMessage = "Количество для возврата должно быть не меньше 1.")]
    public int Quantity { get; set; }
}
