using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Authorization;
using MyFinanceApp.Models;
using MyFinanceApp.Services;
using MySqlConnector;
using System.Data;
using System.Globalization;
using System.Text;

namespace MyFinanceApp.Controllers;

[Authorize(Roles = "Администратор,Менеджер,Кассир")]
public class ExportController : Controller
{
    [HttpPost]
    public async Task<IActionResult> PartsCsv([FromBody] MasterDataRequest request)
    {
        try
        {
            using var connection = new MySqlConnection(request.Settings.GetConnectionString());
            await connection.OpenAsync();
            await CatalogSchemaHelper.EnsureReferenceTablesAndColumnsAsync(connection, request.InventoryTableName, request.SuppliersTableName, request.CategoriesTableName);
            var suppliers = (await CatalogSchemaHelper.GetSuppliersAsync(connection, request.SuppliersTableName)).ToDictionary(x => x.Id, x => x.Name);
            var categories = (await CatalogSchemaHelper.GetCategoriesAsync(connection, request.CategoriesTableName)).ToDictionary(x => x.Id, x => x.Name);

            string sql = $"SELECT * FROM `{request.InventoryTableName}` LIMIT 1000;";
            using var adapter = new MySqlDataAdapter(sql, connection);
            var table = new DataTable();
            adapter.Fill(table);
            var columns = table.Columns.Cast<DataColumn>().Select(c => c.ColumnName).ToList();

            string? partNumberColumn = ResolveTextColumn(table, "part_number", "sku", "article", "артикул");
            string? partNameColumn = ResolveTextColumn(table, "part_name", "name", "название", "наименование");
            string? brandColumn = ResolveTextColumn(table, "brand", "manufacturer", "бренд");
            string? carModelColumn = ResolveTextColumn(table, "car_model", "model", "модель");
            string? priceColumn = ResolveTextColumn(table, "sale_price", "retail_price", "selling_price", "unit_price", "price", "цена");
            string? quantityColumn = ResolveTextColumn(table, "quantity", "qty", "stock", "остаток", "количество");
            string? categoryIdColumn = ResolveTextColumn(table, "category_id");
            string? categoryNameColumn = ResolveTextColumn(table, "category", "category_name", "категория");
            string? supplierIdColumn = ResolveTextColumn(table, "supplier_id");
            string? supplierNameColumn = ResolveTextColumn(table, "supplier", "supplier_name", "vendor", "поставщик");

            var sb = new StringBuilder();
            sb.AppendLine("Артикул;Название;Бренд;Модель авто;Категория;Поставщик;Цена;Остаток");
            foreach (DataRow row in table.Rows)
            {
                string category = categoryNameColumn is not null ? row[categoryNameColumn]?.ToString() ?? string.Empty : string.Empty;
                if (string.IsNullOrWhiteSpace(category) && categoryIdColumn is not null)
                {
                    int categoryId = CatalogSchemaHelper.ParseInt(row[categoryIdColumn]);
                    category = categories.TryGetValue(categoryId, out var categoryName) ? categoryName : string.Empty;
                }

                string supplier = supplierNameColumn is not null ? row[supplierNameColumn]?.ToString() ?? string.Empty : string.Empty;
                if (string.IsNullOrWhiteSpace(supplier) && supplierIdColumn is not null)
                {
                    int supplierId = CatalogSchemaHelper.ParseInt(row[supplierIdColumn]);
                    supplier = suppliers.TryGetValue(supplierId, out var supplierName) ? supplierName : string.Empty;
                }

                sb.AppendLine(string.Join(';', new[]
                {
                    Csv(row, partNumberColumn),
                    Csv(row, partNameColumn),
                    Csv(row, brandColumn),
                    Csv(row, carModelColumn),
                    Escape(category),
                    Escape(supplier),
                    Escape((priceColumn is null ? string.Empty : row[priceColumn]?.ToString()) ?? string.Empty),
                    Escape((quantityColumn is null ? string.Empty : row[quantityColumn]?.ToString()) ?? string.Empty)
                }));
            }

            return CsvFile(sb.ToString(), $"parts_export_{DateTime.Now:yyyyMMdd_HHmm}.csv");
        }
        catch (Exception ex)
        {
            return BadRequest(ex.Message);
        }
    }

    [HttpPost]
    public async Task<IActionResult> SalesCsv([FromBody] MasterDataRequest request)
    {
        try
        {
            using var connection = new MySqlConnection(request.Settings.GetConnectionString());
            await connection.OpenAsync();
            await EnsureSalesTablesAsync(connection, request.SalesTableName, request.SaleItemsTableName);

            string sql = $@"
                SELECT s.id, s.sale_date, s.customer_name, s.customer_phone, s.payment_method, s.status, s.total_amount,
                       COUNT(si.id) AS positions, COALESCE(SUM(si.quantity),0) AS total_quantity,
                       COALESCE(SUM((si.sale_price - si.purchase_price) * si.quantity),0) AS profit
                FROM `{request.SalesTableName}` s
                LEFT JOIN `{request.SaleItemsTableName}` si ON si.sale_id = s.id
                GROUP BY s.id, s.sale_date, s.customer_name, s.customer_phone, s.payment_method, s.status, s.total_amount
                ORDER BY s.sale_date DESC, s.id DESC;";
            using var cmd = new MySqlCommand(sql, connection);
            var sb = new StringBuilder();
            sb.AppendLine("Заказ;Дата;Клиент;Телефон;Оплата;Статус;Позиции;Единиц;Сумма;Прибыль");
            using var reader = await cmd.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                sb.AppendLine(string.Join(';', new[]
                {
                    Escape("#" + reader["id"]?.ToString()),
                    Escape(Convert.ToDateTime(reader["sale_date"], CultureInfo.InvariantCulture).ToString("dd.MM.yyyy HH:mm", CultureInfo.InvariantCulture)),
                    Escape(reader["customer_name"]?.ToString() ?? string.Empty),
                    Escape(reader["customer_phone"]?.ToString() ?? string.Empty),
                    Escape(reader["payment_method"]?.ToString() ?? string.Empty),
                    Escape(reader["status"]?.ToString() ?? string.Empty),
                    Escape(reader["positions"]?.ToString() ?? string.Empty),
                    Escape(reader["total_quantity"]?.ToString() ?? string.Empty),
                    Escape(CatalogSchemaHelper.ParseDecimal(reader["total_amount"]).ToString(CultureInfo.InvariantCulture)),
                    Escape(CatalogSchemaHelper.ParseDecimal(reader["profit"]).ToString(CultureInfo.InvariantCulture))
                }));
            }

            return CsvFile(sb.ToString(), $"sales_export_{DateTime.Now:yyyyMMdd_HHmm}.csv");
        }
        catch (Exception ex)
        {
            return BadRequest(ex.Message);
        }
    }

    [HttpPost]
    public async Task<IActionResult> AnalyticsCsv([FromBody] AnalyticsDashboardRequest request)
    {
        try
        {
            using var connection = new MySqlConnection(request.Settings.GetConnectionString());
            await connection.OpenAsync();
            await CatalogSchemaHelper.EnsureReferenceTablesAndColumnsAsync(connection, request.InventoryTableName, request.SuppliersTableName, request.CategoriesTableName);
            await EnsureSalesTablesAsync(connection, request.SalesTableName, request.SaleItemsTableName);

            decimal revenueToday = await ExecuteDecimalAsync(connection, $"SELECT COALESCE(SUM(total_amount), 0) FROM `{request.SalesTableName}` WHERE DATE(sale_date) = CURDATE();");
            decimal revenueMonth = await ExecuteDecimalAsync(connection, $"SELECT COALESCE(SUM(total_amount), 0) FROM `{request.SalesTableName}` WHERE YEAR(sale_date)=YEAR(CURDATE()) AND MONTH(sale_date)=MONTH(CURDATE());");
            int ordersMonth = await ExecuteIntAsync(connection, $"SELECT COUNT(*) FROM `{request.SalesTableName}` WHERE YEAR(sale_date)=YEAR(CURDATE()) AND MONTH(sale_date)=MONTH(CURDATE());");
            decimal averageCheck = ordersMonth > 0 ? revenueMonth / ordersMonth : 0;
            decimal profitMonth = await ExecuteDecimalAsync(connection, $"SELECT COALESCE(SUM((sale_price - purchase_price) * quantity),0) FROM `{request.SaleItemsTableName}` si INNER JOIN `{request.SalesTableName}` s ON s.id = si.sale_id WHERE YEAR(s.sale_date)=YEAR(CURDATE()) AND MONTH(s.sale_date)=MONTH(CURDATE());");

            var sb = new StringBuilder();
            sb.AppendLine("Показатель;Значение");
            sb.AppendLine($"Выручка за сегодня;{revenueToday.ToString(CultureInfo.InvariantCulture)}");
            sb.AppendLine($"Выручка за месяц;{revenueMonth.ToString(CultureInfo.InvariantCulture)}");
            sb.AppendLine($"Количество продаж за месяц;{ordersMonth}");
            sb.AppendLine($"Средний чек;{averageCheck.ToString(CultureInfo.InvariantCulture)}");
            sb.AppendLine($"Прибыль за месяц;{profitMonth.ToString(CultureInfo.InvariantCulture)}");
            sb.AppendLine();
            sb.AppendLine("Топ товаров;;");
            sb.AppendLine("Артикул;Название;Количество;Выручка");
            string topSql = $@"SELECT part_number, part_name, COALESCE(SUM(quantity),0) AS qty, COALESCE(SUM(line_total),0) AS revenue FROM `{request.SaleItemsTableName}` GROUP BY part_number, part_name ORDER BY qty DESC, revenue DESC LIMIT 10;";
            using (var cmd = new MySqlCommand(topSql, connection))
            using (var reader = await cmd.ExecuteReaderAsync())
            {
                while (await reader.ReadAsync())
                {
                    sb.AppendLine(string.Join(';', new[]
                    {
                        Escape(reader["part_number"]?.ToString() ?? string.Empty),
                        Escape(reader["part_name"]?.ToString() ?? string.Empty),
                        Escape(reader["qty"]?.ToString() ?? string.Empty),
                        Escape(CatalogSchemaHelper.ParseDecimal(reader["revenue"]).ToString(CultureInfo.InvariantCulture))
                    }));
                }
            }

            return CsvFile(sb.ToString(), $"analytics_export_{DateTime.Now:yyyyMMdd_HHmm}.csv");
        }
        catch (Exception ex)
        {
            return BadRequest(ex.Message);
        }
    }

    private static FileContentResult CsvFile(string content, string fileName)
    {
        var bytes = Encoding.UTF8.GetPreamble().Concat(Encoding.UTF8.GetBytes(content)).ToArray();
        return new FileContentResult(bytes, "text/csv; charset=utf-8") { FileDownloadName = fileName };
    }

    private static string Csv(DataRow row, string? columnName)
    {
        return Escape(columnName is null ? string.Empty : row[columnName]?.ToString() ?? string.Empty);
    }

    private static string Escape(string value)
    {
        return '"' + (value ?? string.Empty).Replace("\"", "\"\"") + '"';
    }

    private static string? ResolveTextColumn(DataTable table, params string[] names)
    {
        return table.Columns.Cast<DataColumn>()
            .Select(c => c.ColumnName)
            .FirstOrDefault(name => names.Any(n => string.Equals(name, n, StringComparison.OrdinalIgnoreCase) || CatalogSchemaHelper.Normalize(name).Contains(CatalogSchemaHelper.Normalize(n), StringComparison.OrdinalIgnoreCase)));
    }

    private static async Task EnsureSalesTablesAsync(MySqlConnection connection, string salesTableName, string saleItemsTableName)
    {
        string salesSql = $@"CREATE TABLE IF NOT EXISTS `{salesTableName}` (`id` INT AUTO_INCREMENT PRIMARY KEY, `sale_date` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP, `customer_name` VARCHAR(150) NOT NULL, `customer_phone` VARCHAR(50) NULL, `customer_email` VARCHAR(150) NULL, `note` VARCHAR(255) NULL, `total_amount` DECIMAL(10,2) NOT NULL DEFAULT 0, `payment_method` VARCHAR(50) NOT NULL DEFAULT 'cash', `status` VARCHAR(50) NOT NULL DEFAULT 'completed');";
        string saleItemsSql = $@"CREATE TABLE IF NOT EXISTS `{saleItemsTableName}` (`id` INT AUTO_INCREMENT PRIMARY KEY, `sale_id` INT NOT NULL, `part_id` INT NULL, `part_number` VARCHAR(100) NULL, `part_name` VARCHAR(150) NOT NULL, `quantity` INT NOT NULL, `sale_price` DECIMAL(10,2) NOT NULL, `purchase_price` DECIMAL(10,2) NOT NULL DEFAULT 0, `line_total` DECIMAL(10,2) NOT NULL);";
        using (var cmd = new MySqlCommand(salesSql, connection)) await cmd.ExecuteNonQueryAsync();
        using (var cmd = new MySqlCommand(saleItemsSql, connection)) await cmd.ExecuteNonQueryAsync();
    }

    private static async Task<decimal> ExecuteDecimalAsync(MySqlConnection connection, string sql)
    {
        using var cmd = new MySqlCommand(sql, connection);
        return CatalogSchemaHelper.ParseDecimal(await cmd.ExecuteScalarAsync());
    }

    private static async Task<int> ExecuteIntAsync(MySqlConnection connection, string sql)
    {
        using var cmd = new MySqlCommand(sql, connection);
        return CatalogSchemaHelper.ParseInt(await cmd.ExecuteScalarAsync());
    }
}
