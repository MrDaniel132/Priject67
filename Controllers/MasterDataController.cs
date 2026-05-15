using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Authorization;
using MyFinanceApp.Models;
using MyFinanceApp.Services;
using MySqlConnector;
using System.Globalization;
using Microsoft.AspNetCore.Authorization.Infrastructure;

namespace MyFinanceApp.Controllers;

[Authorize(Roles = "Администратор,Кассир,Менеджер,Кладовщик")]
public class MasterDataController : Controller
{
    private readonly IAuditLogService _auditLogService;

    public MasterDataController(IAuditLogService auditLogService)
    {
        _auditLogService = auditLogService;
    }

    [HttpPost]
    public async Task<IActionResult> GetLookups([FromBody] MasterDataRequest request)
    {
        var validationErrors = RequestValidationHelper.ValidateObjectGraph(request);
        if (validationErrors.Count > 0)
            return Json(new { success = false, message = validationErrors[0], errors = validationErrors });

        try
        {
            await _auditLogService.RememberDbSettingsAsync(HttpContext, request.Settings);
            await _auditLogService.FlushPendingLoginAsync(request.Settings, HttpContext);
            using var connection = new MySqlConnection(request.Settings.GetConnectionString());
            await connection.OpenAsync();
            await CatalogSchemaHelper.EnsureReferenceTablesAndColumnsAsync(connection, request.InventoryTableName, request.SuppliersTableName, request.CategoriesTableName);
            var suppliers = await CatalogSchemaHelper.GetSuppliersAsync(connection, request.SuppliersTableName);
            var categories = await CatalogSchemaHelper.GetCategoriesAsync(connection, request.CategoriesTableName);
            return Json(new { success = true, suppliers, categories });
        }
        catch (Exception ex)
        {
            return Json(new { success = false, message = ex.Message });
        }
    }

    [HttpPost]
    public async Task<IActionResult> GetSuppliers([FromBody] MasterDataRequest request)
    {
        var validationErrors = RequestValidationHelper.ValidateObjectGraph(request);
        if (validationErrors.Count > 0)
            return Json(new { success = false, message = validationErrors[0], errors = validationErrors });

        try
        {
            using var connection = new MySqlConnection(request.Settings.GetConnectionString());
            await connection.OpenAsync();
            await CatalogSchemaHelper.EnsureReferenceTablesAndColumnsAsync(connection, request.InventoryTableName, request.SuppliersTableName, request.CategoriesTableName);
            await EnsurePurchaseTablesAsync(connection, request.PurchasesTableName, request.PurchaseItemsTableName);

            var suppliers = await CatalogSchemaHelper.GetSuppliersAsync(connection, request.SuppliersTableName);
            var productCounts = await GetCountByForeignKeyAsync(connection, request.InventoryTableName, "supplier_id");
            var purchaseStats = await GetPurchaseStatsAsync(connection, request.PurchasesTableName);

            var items = suppliers
                .Where(s => string.IsNullOrWhiteSpace(request.Search) || s.Name.Contains(request.Search, StringComparison.OrdinalIgnoreCase) || s.Phone.Contains(request.Search ?? string.Empty, StringComparison.OrdinalIgnoreCase))
                .Select(s => new
                {
                    id = s.Id,
                    name = s.Name,
                    phone = s.Phone,
                    email = s.Email,
                    contactPerson = s.ContactPerson,
                    address = s.Address,
                    note = s.Note,
                    linkedProducts = productCounts.TryGetValue(s.Id, out var linkedProducts) ? linkedProducts : 0,
                    totalPurchases = purchaseStats.TryGetValue(s.Name, out var amount) ? amount : 0m
                })
                .OrderBy(x => x.name)
                .ToList();

            return Json(new
            {
                success = true,
                suppliers = items,
                summary = new
                {
                    count = items.Count,
                    linkedProducts = items.Sum(x => x.linkedProducts),
                    totalPurchases = items.Sum(x => x.totalPurchases)
                }
            });
        }
        catch (Exception ex)
        {
            return Json(new { success = false, message = ex.Message });
        }
    }

    [HttpPost]
    public async Task<IActionResult> SaveSupplier([FromBody] SupplierSaveRequest request)
    {
        var validationErrors = RequestValidationHelper.ValidateObjectGraph(request);
        if (validationErrors.Count > 0)
            return Json(new { success = false, message = validationErrors[0], errors = validationErrors });

        if (string.IsNullOrWhiteSpace(request.Name))
            return Json(new { success = false, message = "Укажи название поставщика." });

        try
        {
            using var connection = new MySqlConnection(request.Settings.GetConnectionString());
            await connection.OpenAsync();
            await CatalogSchemaHelper.EnsureReferenceTablesAndColumnsAsync(connection, request.InventoryTableName, request.SuppliersTableName, request.CategoriesTableName);

            if (request.Id.HasValue && request.Id.Value > 0)
            {
                string updateSql = $@"
                    UPDATE `{request.SuppliersTableName}`
                    SET name=@name, phone=@phone, email=@email, contact_person=@contact, address=@address, note=@note
                    WHERE id=@id;";
                using var updateCmd = new MySqlCommand(updateSql, connection);
                FillSupplierParameters(updateCmd, request);
                updateCmd.Parameters.AddWithValue("@id", request.Id.Value);
                await updateCmd.ExecuteNonQueryAsync();
            }
            else
            {
                string insertSql = $@"
                    INSERT INTO `{request.SuppliersTableName}`
                    (name, phone, email, contact_person, address, note)
                    VALUES (@name, @phone, @email, @contact, @address, @note);";
                using var insertCmd = new MySqlCommand(insertSql, connection);
                FillSupplierParameters(insertCmd, request);
                await insertCmd.ExecuteNonQueryAsync();
            }

            await _auditLogService.LogAsync(request.Settings, new AuditLogEntry
            {
                Username = User.Identity?.Name ?? "unknown",
                Action = request.Id.HasValue && request.Id.Value > 0 ? "update_supplier" : "create_supplier",
                EntityType = request.SuppliersTableName,
                EntityId = request.Id?.ToString(),
                Details = $"Поставщик: {request.Name}, телефон: {request.Phone}"
            }, HttpContext);

            return Json(new { success = true, message = "Поставщик сохранён." });
        }
        catch (Exception ex)
        {
            return Json(new { success = false, message = ex.Message });
        }
    }

    [HttpPost]
    public async Task<IActionResult> GetCategories([FromBody] MasterDataRequest request)
    {
        var validationErrors = RequestValidationHelper.ValidateObjectGraph(request);
        if (validationErrors.Count > 0)
            return Json(new { success = false, message = validationErrors[0], errors = validationErrors });

        try
        {
            using var connection = new MySqlConnection(request.Settings.GetConnectionString());
            await connection.OpenAsync();
            await CatalogSchemaHelper.EnsureReferenceTablesAndColumnsAsync(connection, request.InventoryTableName, request.SuppliersTableName, request.CategoriesTableName);
            await EnsureSalesTablesAsync(connection, request.SalesTableName, request.SaleItemsTableName);

            var categories = await CatalogSchemaHelper.GetCategoriesAsync(connection, request.CategoriesTableName);
            var productCounts = await GetCountByForeignKeyAsync(connection, request.InventoryTableName, "category_id");
            var revenueByCategory = await GetCategoryRevenueAsync(connection, request);

            var items = categories
                .Where(c => string.IsNullOrWhiteSpace(request.Search) || c.Name.Contains(request.Search, StringComparison.OrdinalIgnoreCase))
                .Select(c => new
                {
                    id = c.Id,
                    name = c.Name,
                    description = c.Description,
                    isActive = c.IsActive,
                    linkedProducts = productCounts.TryGetValue(c.Id, out var linkedProducts) ? linkedProducts : 0,
                    revenue = revenueByCategory.TryGetValue(c.Id, out var revenue) ? revenue : 0m
                })
                .OrderBy(x => x.name)
                .ToList();

            return Json(new
            {
                success = true,
                categories = items,
                summary = new
                {
                    count = items.Count,
                    linkedProducts = items.Sum(x => x.linkedProducts),
                    revenue = items.Sum(x => x.revenue)
                }
            });
        }
        catch (Exception ex)
        {
            return Json(new { success = false, message = ex.Message });
        }
    }

    [HttpPost]
    public async Task<IActionResult> SaveCategory([FromBody] CategorySaveRequest request)
    {
        var validationErrors = RequestValidationHelper.ValidateObjectGraph(request);
        if (validationErrors.Count > 0)
            return Json(new { success = false, message = validationErrors[0], errors = validationErrors });

        if (string.IsNullOrWhiteSpace(request.Name))
            return Json(new { success = false, message = "Укажи название категории." });

        try
        {
            using var connection = new MySqlConnection(request.Settings.GetConnectionString());
            await connection.OpenAsync();
            await CatalogSchemaHelper.EnsureReferenceTablesAndColumnsAsync(connection, request.InventoryTableName, request.SuppliersTableName, request.CategoriesTableName);

            if (request.Id.HasValue && request.Id.Value > 0)
            {
                string updateSql = $@"
                    UPDATE `{request.CategoriesTableName}`
                    SET name=@name, description=@description, is_active=@isActive
                    WHERE id=@id;";
                using var updateCmd = new MySqlCommand(updateSql, connection);
                FillCategoryParameters(updateCmd, request);
                updateCmd.Parameters.AddWithValue("@id", request.Id.Value);
                await updateCmd.ExecuteNonQueryAsync();
            }
            else
            {
                string insertSql = $@"
                    INSERT INTO `{request.CategoriesTableName}`
                    (name, description, is_active)
                    VALUES (@name, @description, @isActive);";
                using var insertCmd = new MySqlCommand(insertSql, connection);
                FillCategoryParameters(insertCmd, request);
                await insertCmd.ExecuteNonQueryAsync();
            }

            await _auditLogService.LogAsync(request.Settings, new AuditLogEntry
            {
                Username = User.Identity?.Name ?? "unknown",
                Action = request.Id.HasValue && request.Id.Value > 0 ? "update_category" : "create_category",
                EntityType = request.CategoriesTableName,
                EntityId = request.Id?.ToString(),
                Details = $"Категория: {request.Name}, активна: {request.IsActive}"
            }, HttpContext);

            return Json(new { success = true, message = "Категория сохранена." });
        }
        catch (Exception ex)
        {
            return Json(new { success = false, message = ex.Message });
        }
    }

    private static void FillSupplierParameters(MySqlCommand cmd, SupplierSaveRequest request)
    {
        cmd.Parameters.AddWithValue("@name", request.Name.Trim());
        cmd.Parameters.AddWithValue("@phone", (object?)request.Phone ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@email", (object?)request.Email ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@contact", (object?)request.ContactPerson ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@address", (object?)request.Address ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@note", (object?)request.Note ?? DBNull.Value);
    }

    private static void FillCategoryParameters(MySqlCommand cmd, CategorySaveRequest request)
    {
        cmd.Parameters.AddWithValue("@name", request.Name.Trim());
        cmd.Parameters.AddWithValue("@description", (object?)request.Description ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@isActive", request.IsActive);
    }

    private static async Task<Dictionary<int, int>> GetCountByForeignKeyAsync(MySqlConnection connection, string tableName, string foreignKeyColumn)
    {
        var columns = await CatalogSchemaHelper.GetColumnsAsync(connection, tableName);
        if (!columns.Contains(foreignKeyColumn)) return new Dictionary<int, int>();

        string sql = $@"SELECT `{foreignKeyColumn}` AS ref_id, COUNT(*) AS total FROM `{tableName}` WHERE `{foreignKeyColumn}` IS NOT NULL GROUP BY `{foreignKeyColumn}`;";
        using var cmd = new MySqlCommand(sql, connection);
        var result = new Dictionary<int, int>();
        using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            int id = CatalogSchemaHelper.ParseInt(reader["ref_id"]);
            result[id] = CatalogSchemaHelper.ParseInt(reader["total"]);
        }
        return result;
    }

    private static async Task<Dictionary<string, decimal>> GetPurchaseStatsAsync(MySqlConnection connection, string purchasesTableName)
    {
        string sql = $@"SELECT supplier_name, COALESCE(SUM(total_amount), 0) AS total_amount FROM `{purchasesTableName}` GROUP BY supplier_name;";
        using var cmd = new MySqlCommand(sql, connection);
        var result = new Dictionary<string, decimal>(StringComparer.OrdinalIgnoreCase);
        using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            var name = reader.IsDBNull(0) ? string.Empty : reader.GetString(0);
            result[name] = CatalogSchemaHelper.ParseDecimal(reader[1]);
        }
        return result;
    }

    private static async Task<Dictionary<int, decimal>> GetCategoryRevenueAsync(MySqlConnection connection, MasterDataRequest request)
    {
        var columns = await CatalogSchemaHelper.GetColumnsAsync(connection, request.InventoryTableName);
        if (!columns.Contains("category_id")) return new Dictionary<int, decimal>();

        string? inventoryIdColumn = CatalogSchemaHelper.ResolveColumn(columns, "id");
        string? inventoryPartNumberColumn = CatalogSchemaHelper.ResolveColumn(columns, "part_number", "sku", "article", "артикул");
        string joinCondition = !string.IsNullOrWhiteSpace(inventoryIdColumn)
            ? $"si.part_id = p.`{inventoryIdColumn}`"
            : !string.IsNullOrWhiteSpace(inventoryPartNumberColumn)
                ? $"si.part_number = p.`{inventoryPartNumberColumn}`"
                : string.Empty;

        if (string.IsNullOrWhiteSpace(joinCondition)) return new Dictionary<int, decimal>();

        string sql = $@"
            SELECT p.category_id, COALESCE(SUM(si.line_total), 0) AS revenue
            FROM `{request.SaleItemsTableName}` si
            INNER JOIN `{request.InventoryTableName}` p ON {joinCondition}
            WHERE p.category_id IS NOT NULL
            GROUP BY p.category_id;";

        using var cmd = new MySqlCommand(sql, connection);
        var result = new Dictionary<int, decimal>();
        using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            int categoryId = CatalogSchemaHelper.ParseInt(reader[0]);
            result[categoryId] = CatalogSchemaHelper.ParseDecimal(reader[1]);
        }
        return result;
    }

    private static async Task EnsurePurchaseTablesAsync(MySqlConnection connection, string purchasesTableName, string purchaseItemsTableName)
    {
        string purchasesSql = $@"
            CREATE TABLE IF NOT EXISTS `{purchasesTableName}` (
                `id` INT AUTO_INCREMENT PRIMARY KEY,
                `purchase_date` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                `supplier_name` VARCHAR(150) NOT NULL,
                `supplier_phone` VARCHAR(50) NULL,
                `document_number` VARCHAR(100) NULL,
                `note` VARCHAR(255) NULL,
                `total_amount` DECIMAL(10,2) NOT NULL DEFAULT 0,
                `status` VARCHAR(50) NOT NULL DEFAULT 'received'
            );";

        string purchaseItemsSql = $@"
            CREATE TABLE IF NOT EXISTS `{purchaseItemsTableName}` (
                `id` INT AUTO_INCREMENT PRIMARY KEY,
                `purchase_id` INT NOT NULL,
                `part_id` INT NULL,
                `part_number` VARCHAR(100) NULL,
                `part_name` VARCHAR(150) NOT NULL,
                `quantity` INT NOT NULL,
                `purchase_price` DECIMAL(10,2) NOT NULL,
                `line_total` DECIMAL(10,2) NOT NULL
            );";

        using (var cmd = new MySqlCommand(purchasesSql, connection))
            await cmd.ExecuteNonQueryAsync();
        using (var cmd = new MySqlCommand(purchaseItemsSql, connection))
            await cmd.ExecuteNonQueryAsync();
    }

    private static async Task EnsureSalesTablesAsync(MySqlConnection connection, string salesTableName, string saleItemsTableName)
    {
        string salesSql = $@"
            CREATE TABLE IF NOT EXISTS `{salesTableName}` (
                `id` INT AUTO_INCREMENT PRIMARY KEY,
                `sale_date` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                `customer_name` VARCHAR(150) NOT NULL,
                `customer_phone` VARCHAR(50) NULL,
                `customer_email` VARCHAR(150) NULL,
                `note` VARCHAR(255) NULL,
                `total_amount` DECIMAL(10,2) NOT NULL DEFAULT 0,
                `payment_method` VARCHAR(50) NOT NULL DEFAULT 'cash',
                `status` VARCHAR(50) NOT NULL DEFAULT 'completed'
            );";

        string saleItemsSql = $@"
            CREATE TABLE IF NOT EXISTS `{saleItemsTableName}` (
                `id` INT AUTO_INCREMENT PRIMARY KEY,
                `sale_id` INT NOT NULL,
                `part_id` INT NULL,
                `part_number` VARCHAR(100) NULL,
                `part_name` VARCHAR(150) NOT NULL,
                `quantity` INT NOT NULL,
                `sale_price` DECIMAL(10,2) NOT NULL,
                `purchase_price` DECIMAL(10,2) NOT NULL DEFAULT 0,
                `line_total` DECIMAL(10,2) NOT NULL
            );";

        using (var cmd = new MySqlCommand(salesSql, connection))
            await cmd.ExecuteNonQueryAsync();
        using (var cmd = new MySqlCommand(saleItemsSql, connection))
            await cmd.ExecuteNonQueryAsync();
    }
}
