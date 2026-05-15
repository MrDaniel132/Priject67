
using System.ComponentModel.DataAnnotations;

namespace MyFinanceApp.Models;

public class MasterDataRequest : InventoryTableRequest
{
    [Required]
    [StringLength(64)]
    public string SuppliersTableName { get; set; } = "suppliers";

    [Required]
    [StringLength(64)]
    public string CategoriesTableName { get; set; } = "categories";

    [Required]
    [StringLength(64)]
    public string PurchasesTableName { get; set; } = "purchases";

    [Required]
    [StringLength(64)]
    public string PurchaseItemsTableName { get; set; } = "purchase_items";

    [Required]
    [StringLength(64)]
    public string SalesTableName { get; set; } = "sales";

    [Required]
    [StringLength(64)]
    public string SaleItemsTableName { get; set; } = "sale_items";

    [StringLength(100, ErrorMessage = "Поисковый запрос слишком длинный.")]
    public string? Search { get; set; }

    [Range(1, 200, ErrorMessage = "Лимит должен быть в диапазоне от 1 до 200.")]
    public int? Limit { get; set; }
}

public class SupplierSaveRequest : MasterDataRequest
{
    public int? Id { get; set; }

    [Required(ErrorMessage = "Укажи название поставщика.")]
    [StringLength(150, ErrorMessage = "Название поставщика не должно быть длиннее 150 символов.")]
    public string Name { get; set; } = string.Empty;

    [RegularExpression(@"^[0-9+()\-\s]{7,25}$", ErrorMessage = "Телефон должен содержать от 7 до 25 символов: цифры, пробелы, +, скобки и дефисы.")]
    public string? Phone { get; set; }

    [EmailAddress(ErrorMessage = "Укажи корректный email поставщика.")]
    [StringLength(150, ErrorMessage = "Email поставщика не должен быть длиннее 150 символов.")]
    public string? Email { get; set; }

    [StringLength(100, ErrorMessage = "Имя контактного лица слишком длинное.")]
    public string? ContactPerson { get; set; }

    [StringLength(200, ErrorMessage = "Адрес не должен быть длиннее 200 символов.")]
    public string? Address { get; set; }

    [StringLength(500, ErrorMessage = "Комментарий слишком длинный.")]
    public string? Note { get; set; }
}

public class CategorySaveRequest : MasterDataRequest
{
    public int? Id { get; set; }

    [Required(ErrorMessage = "Укажи название категории.")]
    [StringLength(100, ErrorMessage = "Название категории не должно быть длиннее 100 символов.")]
    public string Name { get; set; } = string.Empty;

    [StringLength(500, ErrorMessage = "Описание категории слишком длинное.")]
    public string? Description { get; set; }

    public bool IsActive { get; set; } = true;
}

public class AnalyticsDashboardRequest : MasterDataRequest, IValidatableObject
{
    [Required(ErrorMessage = "Укажи период аналитики.")]
    [StringLength(20)]
    public string Period { get; set; } = "month";

    public IEnumerable<ValidationResult> Validate(ValidationContext validationContext)
    {
        var allowed = new[] { "month", "today" };
        if (!allowed.Contains(Period, StringComparer.OrdinalIgnoreCase))
            yield return new ValidationResult("Выбран недопустимый период аналитики.", [nameof(Period)]);
    }
}
