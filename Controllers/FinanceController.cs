using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Authorization;
using MyFinanceApp.Models;
using MyFinanceApp.Services;
using MySqlConnector;
using System.Globalization;

namespace MyFinanceApp.Controllers;

[Authorize(Roles = "Администратор,Менеджер")]
public class FinanceController : Controller
{
    public IActionResult Index() => View();

    [HttpPost]
    public async Task<IActionResult> GetDashboard([FromBody] AnalyticsDashboardRequest request)
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
            await EnsurePurchaseTablesAsync(connection, request.PurchasesTableName, request.PurchaseItemsTableName);

            var inventoryColumns = await CatalogSchemaHelper.GetColumnsAsync(connection, request.InventoryTableName);
            if (inventoryColumns.Count == 0)
                return Json(new { success = false, message = $"Таблица '{request.InventoryTableName}' не найдена." });

            var metadata = ResolveInventoryMetadata(inventoryColumns);
            var kpis = await GetKpisAsync(connection, request, metadata);
            var dailySales = await GetDailySalesAsync(connection, request.SalesTableName);
            var topProducts = await GetTopProductsAsync(connection, request.SaleItemsTableName);
            var lowStockItems = await GetLowStockItemsAsync(connection, request.InventoryTableName, metadata);
            var categoryDistribution = await GetCategoryDistributionAsync(connection, request, metadata);
            var supplierPurchases = await GetSupplierPurchasesAsync(connection, request.PurchasesTableName);
            var recentOrders = await GetRecentOrdersAsync(connection, request.SalesTableName, request.SaleItemsTableName);

            return Json(new
            {
                success = true,
                kpis,
                dailySales,
                topProducts,
                lowStockItems,
                categoryDistribution,
                supplierPurchases,
                recentOrders
            });
        }
        catch (Exception ex)
        {
            return Json(new { success = false, message = ex.Message });
        }
    }

    private static InventoryMetadata ResolveInventoryMetadata(HashSet<string> columns)
    {
        return new InventoryMetadata
        {
            IdColumn = CatalogSchemaHelper.ResolveColumn(columns, "id"),
            PartNumberColumn = CatalogSchemaHelper.ResolveColumn(columns, "part_number", "sku", "article", "артикул"),
            PartNameColumn = CatalogSchemaHelper.ResolveColumn(columns, "part_name", "name", "название", "наименование", "товар"),
            QuantityColumn = CatalogSchemaHelper.ResolveColumn(columns, "quantity", "qty", "stock", "остаток", "количество") ?? "quantity",
            MinQuantityColumn = CatalogSchemaHelper.ResolveColumn(columns, "min_quantity", "min_stock", "reorder_level", "минимальный_остаток") ?? "min_quantity",
            SalePriceColumn = CatalogSchemaHelper.ResolveColumn(columns, "sale_price", "retail_price", "selling_price", "unit_price", "price", "цена"),
            PurchasePriceColumn = CatalogSchemaHelper.ResolveColumn(columns, "purchase_price", "cost_price", "buy_price", "cost", "закупочная_цена"),
            CategoryIdColumn = CatalogSchemaHelper.ResolveColumn(columns, "category_id"),
            CategoryNameColumn = CatalogSchemaHelper.ResolveColumn(columns, "category", "category_name", "категория"),
            SupplierIdColumn = CatalogSchemaHelper.ResolveColumn(columns, "supplier_id"),
            SupplierNameColumn = CatalogSchemaHelper.ResolveColumn(columns, "supplier", "supplier_name", "vendor", "поставщик")
        };
    }

    private static async Task<object> GetKpisAsync(MySqlConnection connection, AnalyticsDashboardRequest request, InventoryMetadata metadata)
    {
        decimal revenueToday = await ExecuteDecimalAsync(connection, $"SELECT COALESCE(SUM(total_amount), 0) FROM `{request.SalesTableName}` WHERE DATE(sale_date) = CURDATE();");
        decimal revenueMonth = await ExecuteDecimalAsync(connection, $"SELECT COALESCE(SUM(total_amount), 0) FROM `{request.SalesTableName}` WHERE YEAR(sale_date)=YEAR(CURDATE()) AND MONTH(sale_date)=MONTH(CURDATE());");
        int ordersMonth = await ExecuteIntAsync(connection, $"SELECT COUNT(*) FROM `{request.SalesTableName}` WHERE YEAR(sale_date)=YEAR(CURDATE()) AND MONTH(sale_date)=MONTH(CURDATE());");
        decimal averageCheck = ordersMonth > 0 ? revenueMonth / ordersMonth : 0;
        decimal monthProfit = await ExecuteDecimalAsync(connection, $"SELECT COALESCE(SUM((sale_price - purchase_price) * quantity), 0) FROM `{request.SaleItemsTableName}` si INNER JOIN `{request.SalesTableName}` s ON s.id = si.sale_id WHERE YEAR(s.sale_date)=YEAR(CURDATE()) AND MONTH(s.sale_date)=MONTH(CURDATE());");

        string priceColumn = metadata.SalePriceColumn ?? metadata.PurchasePriceColumn ?? metadata.QuantityColumn;
        decimal inventoryValue = await ExecuteDecimalAsync(connection, $"SELECT COALESCE(SUM(COALESCE(`{metadata.QuantityColumn}`,0) * COALESCE(`{priceColumn}`,0)), 0) FROM `{request.InventoryTableName}`;");
        int totalPositions = await ExecuteIntAsync(connection, $"SELECT COUNT(*) FROM `{request.InventoryTableName}`;");
        int lowStockCount = await ExecuteIntAsync(connection, $"SELECT COUNT(*) FROM `{request.InventoryTableName}` WHERE COALESCE(`{metadata.QuantityColumn}`,0) <= COALESCE(`{metadata.MinQuantityColumn}`,5);");

        return new
        {
            revenueToday,
            revenueMonth,
            ordersMonth,
            averageCheck,
            monthProfit,
            inventoryValue,
            totalPositions,
            lowStockCount
        };
    }

    private static async Task<List<object>> GetDailySalesAsync(MySqlConnection connection, string salesTableName)
    {
        string sql = $@"
            SELECT DATE(sale_date) AS day_key, COALESCE(SUM(total_amount),0) AS revenue, COUNT(*) AS orders_count
            FROM `{salesTableName}`
            WHERE sale_date >= DATE_SUB(CURDATE(), INTERVAL 9 DAY)
            GROUP BY DATE(sale_date)
            ORDER BY DATE(sale_date);";

        using var cmd = new MySqlCommand(sql, connection);
        var result = new List<object>();
        using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            DateTime day = Convert.ToDateTime(reader["day_key"], CultureInfo.InvariantCulture);
            result.Add(new
            {
                day = day.ToString("dd.MM", CultureInfo.InvariantCulture),
                revenue = CatalogSchemaHelper.ParseDecimal(reader["revenue"]),
                ordersCount = CatalogSchemaHelper.ParseInt(reader["orders_count"])
            });
        }
        return result;
    }

    private static async Task<List<object>> GetTopProductsAsync(MySqlConnection connection, string saleItemsTableName)
    {
        string sql = $@"
            SELECT part_number, part_name, COALESCE(SUM(quantity),0) AS qty, COALESCE(SUM(line_total),0) AS revenue
            FROM `{saleItemsTableName}`
            GROUP BY part_number, part_name
            ORDER BY qty DESC, revenue DESC
            LIMIT 5;";
        using var cmd = new MySqlCommand(sql, connection);
        var result = new List<object>();
        using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            result.Add(new
            {
                partNumber = reader.IsDBNull(reader.GetOrdinal("part_number")) ? string.Empty : reader.GetString(reader.GetOrdinal("part_number")),
                partName = reader.IsDBNull(reader.GetOrdinal("part_name")) ? string.Empty : reader.GetString(reader.GetOrdinal("part_name")),
                quantity = CatalogSchemaHelper.ParseInt(reader["qty"]),
                revenue = CatalogSchemaHelper.ParseDecimal(reader["revenue"])
            });
        }
        return result;
    }

    private static async Task<List<object>> GetLowStockItemsAsync(MySqlConnection connection, string inventoryTableName, InventoryMetadata metadata)
    {
        string partNameSelect = !string.IsNullOrWhiteSpace(metadata.PartNameColumn) ? $"`{metadata.PartNameColumn}`" : "NULL";
        string partNumberSelect = !string.IsNullOrWhiteSpace(metadata.PartNumberColumn) ? $"`{metadata.PartNumberColumn}`" : "NULL";
        string sql = $@"
            SELECT {partNumberSelect} AS part_number,
                   {partNameSelect} AS part_name,
                   COALESCE(`{metadata.QuantityColumn}`,0) AS quantity,
                   COALESCE(`{metadata.MinQuantityColumn}`,5) AS min_quantity
            FROM `{inventoryTableName}`
            WHERE COALESCE(`{metadata.QuantityColumn}`,0) <= COALESCE(`{metadata.MinQuantityColumn}`,5)
            ORDER BY COALESCE(`{metadata.QuantityColumn}`,0) ASC, {partNameSelect}
            LIMIT 8;";
        using var cmd = new MySqlCommand(sql, connection);
        var result = new List<object>();
        using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            result.Add(new
            {
                partNumber = reader.IsDBNull(reader.GetOrdinal("part_number")) ? string.Empty : reader["part_number"]?.ToString() ?? string.Empty,
                partName = reader.IsDBNull(reader.GetOrdinal("part_name")) ? string.Empty : reader["part_name"]?.ToString() ?? string.Empty,
                quantity = CatalogSchemaHelper.ParseInt(reader["quantity"]),
                minQuantity = CatalogSchemaHelper.ParseInt(reader["min_quantity"])
            });
        }
        return result;
    }

    private static async Task<List<object>> GetCategoryDistributionAsync(MySqlConnection connection, AnalyticsDashboardRequest request, InventoryMetadata metadata)
    {
        var result = new List<object>();
        string? joinCondition = BuildInventoryJoinCondition(metadata);
        if (string.IsNullOrWhiteSpace(joinCondition)) return result;

        if (!string.IsNullOrWhiteSpace(metadata.CategoryIdColumn))
        {
            string sql = $@"
                SELECT COALESCE(c.name, 'Без категории') AS category_name, COALESCE(SUM(si.line_total),0) AS revenue
                FROM `{request.SaleItemsTableName}` si
                INNER JOIN `{request.InventoryTableName}` p ON {joinCondition}
                LEFT JOIN `{request.CategoriesTableName}` c ON c.id = p.`{metadata.CategoryIdColumn}`
                GROUP BY COALESCE(c.name, 'Без категории')
                ORDER BY revenue DESC
                LIMIT 6;";
            using var cmd = new MySqlCommand(sql, connection);
            using var reader = await cmd.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                result.Add(new
                {
                    name = reader["category_name"]?.ToString() ?? "Без категории",
                    revenue = CatalogSchemaHelper.ParseDecimal(reader["revenue"])
                });
            }
            return result;
        }

        if (!string.IsNullOrWhiteSpace(metadata.CategoryNameColumn))
        {
            string sql = $@"
                SELECT COALESCE(p.`{metadata.CategoryNameColumn}`, 'Без категории') AS category_name, COALESCE(SUM(si.line_total),0) AS revenue
                FROM `{request.SaleItemsTableName}` si
                INNER JOIN `{request.InventoryTableName}` p ON {joinCondition}
                GROUP BY COALESCE(p.`{metadata.CategoryNameColumn}`, 'Без категории')
                ORDER BY revenue DESC
                LIMIT 6;";
            using var cmd = new MySqlCommand(sql, connection);
            using var reader = await cmd.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                result.Add(new
                {
                    name = reader["category_name"]?.ToString() ?? "Без категории",
                    revenue = CatalogSchemaHelper.ParseDecimal(reader["revenue"])
                });
            }
        }

        return result;
    }

    private static async Task<List<object>> GetSupplierPurchasesAsync(MySqlConnection connection, string purchasesTableName)
    {
        string sql = $@"
            SELECT supplier_name, COALESCE(SUM(total_amount),0) AS total_amount, COUNT(*) AS purchases_count
            FROM `{purchasesTableName}`
            GROUP BY supplier_name
            ORDER BY total_amount DESC
            LIMIT 6;";
        using var cmd = new MySqlCommand(sql, connection);
        var result = new List<object>();
        using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            result.Add(new
            {
                name = reader.IsDBNull(reader.GetOrdinal("supplier_name")) ? "—" : reader.GetString(reader.GetOrdinal("supplier_name")),
                totalAmount = CatalogSchemaHelper.ParseDecimal(reader["total_amount"]),
                purchasesCount = CatalogSchemaHelper.ParseInt(reader["purchases_count"])
            });
        }
        return result;
    }

    private static async Task<List<object>> GetRecentOrdersAsync(MySqlConnection connection, string salesTableName, string saleItemsTableName)
    {
        string sql = $@"
            SELECT s.id,
                   s.sale_date,
                   s.customer_name,
                   s.total_amount,
                   COALESCE(SUM((si.sale_price - si.purchase_price) * si.quantity),0) AS profit
            FROM `{salesTableName}` s
            LEFT JOIN `{saleItemsTableName}` si ON si.sale_id = s.id
            GROUP BY s.id, s.sale_date, s.customer_name, s.total_amount
            ORDER BY s.sale_date DESC, s.id DESC
            LIMIT 6;";
        using var cmd = new MySqlCommand(sql, connection);
        var result = new List<object>();
        using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            result.Add(new
            {
                id = CatalogSchemaHelper.ParseInt(reader["id"]),
                saleDate = Convert.ToDateTime(reader["sale_date"], CultureInfo.InvariantCulture).ToString("dd.MM.yyyy HH:mm", CultureInfo.InvariantCulture),
                customerName = reader.IsDBNull(reader.GetOrdinal("customer_name")) ? string.Empty : reader.GetString(reader.GetOrdinal("customer_name")),
                totalAmount = CatalogSchemaHelper.ParseDecimal(reader["total_amount"]),
                profit = CatalogSchemaHelper.ParseDecimal(reader["profit"])
            });
        }
        return result;
    }

    private static string? BuildInventoryJoinCondition(InventoryMetadata metadata)
    {
        if (!string.IsNullOrWhiteSpace(metadata.IdColumn))
            return $"si.part_id = p.`{metadata.IdColumn}`";
        if (!string.IsNullOrWhiteSpace(metadata.PartNumberColumn))
            return $"si.part_number = p.`{metadata.PartNumberColumn}`";
        return null;
    }

    private static async Task<decimal> ExecuteDecimalAsync(MySqlConnection connection, string sql)
    {
        using var cmd = new MySqlCommand(sql, connection);
        var value = await cmd.ExecuteScalarAsync();
        return CatalogSchemaHelper.ParseDecimal(value);
    }

    private static async Task<int> ExecuteIntAsync(MySqlConnection connection, string sql)
    {
        using var cmd = new MySqlCommand(sql, connection);
        var value = await cmd.ExecuteScalarAsync();
        return CatalogSchemaHelper.ParseInt(value);
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

    private sealed class InventoryMetadata
    {
        public string? IdColumn { get; set; }
        public string? PartNumberColumn { get; set; }
        public string? PartNameColumn { get; set; }
        public string QuantityColumn { get; set; } = "quantity";
        public string MinQuantityColumn { get; set; } = "min_quantity";
        public string? SalePriceColumn { get; set; }
        public string? PurchasePriceColumn { get; set; }
        public string? CategoryIdColumn { get; set; }
        public string? CategoryNameColumn { get; set; }
        public string? SupplierIdColumn { get; set; }
        public string? SupplierNameColumn { get; set; }
    }
}
