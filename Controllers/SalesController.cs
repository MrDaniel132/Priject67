using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Authorization;
using MyFinanceApp.Models;
using MyFinanceApp.Services;
using MySqlConnector;
using System.Data;
using System.Globalization;
using System.ComponentModel.DataAnnotations;

namespace MyFinanceApp.Controllers;

[Authorize(Roles = "Администратор,Менеджер,Кассир")]
public class SalesController : Controller
{
    private readonly IAuditLogService _auditLogService;

    public SalesController(IAuditLogService auditLogService)
    {
        _auditLogService = auditLogService;
    }

    public IActionResult Index() => View();

    [HttpPost]
    public async Task<IActionResult> CreateOrder([FromBody] SalesOrderRequest request)
    {
       
       

        try
        {
            if (request == null)
                return Json(new { success = false, message = "Запрос не получен или пришел в неверном формате JSON." });

            var validationErrors = RequestValidationHelper.ValidateObjectGraph(request);
            if (validationErrors.Count > 0)
                return Json(new { success = false, message = validationErrors[0], errors = validationErrors });

            if (request.Settings == null)
                return Json(new { success = false, message = "Не переданы настройки подключения к базе данных." });

            if (string.IsNullOrWhiteSpace(request.InventoryTableName))
                return Json(new { success = false, message = "Не выбрана таблица товаров." });

            if (request.Items == null || request.Items.Count == 0)
                return Json(new { success = false, message = "Добавьте в заказ хотя бы одну позицию." });

            if (request.Items.Any(i => i.Quantity <= 0))
                return Json(new { success = false, message = "Количество товаров должно быть больше нуля." });

            request.SalesTableName = string.IsNullOrWhiteSpace(request.SalesTableName) ? "sales" : request.SalesTableName;
            request.SaleItemsTableName = string.IsNullOrWhiteSpace(request.SaleItemsTableName) ? "sale_items" : request.SaleItemsTableName;

            await _auditLogService.FlushPendingLoginAsync(request.Settings, HttpContext);
            string connectionString = request.Settings.GetConnectionString();

            await using var connection = new MySqlConnection(connectionString);
            await connection.OpenAsync();

            await EnsureSalesTablesAsync(connection, request.SalesTableName, request.SaleItemsTableName);
            await EnsureInventorySupportTablesAsync(connection);

            var inventoryMetadata = await GetInventoryTableMetadataAsync(connection, request.InventoryTableName);

            var inventoryRecords = new List<InventoryRecord>();

            foreach (var item in request.Items)
            {
                var inventoryRecord = await GetInventoryRowAsync(connection, request.InventoryTableName, inventoryMetadata, item);

                if (inventoryRecord == null)
                    return Json(new { success = false, message = $"Товар '{item.PartName}' не найден в таблице {request.InventoryTableName}." });

                if (inventoryRecord.Quantity < item.Quantity)
                    return Json(new { success = false, message = $"Недостаточно остатка для товара '{item.PartName}'. Доступно: {inventoryRecord.Quantity}." });

                inventoryRecords.Add(inventoryRecord);
            }

            decimal totalAmount = request.Items.Sum(i => i.SalePrice * i.Quantity);
            int saleId = await InsertSaleAsync(connection, request, totalAmount);

            for (int index = 0; index < request.Items.Count; index++)
            {
                var item = request.Items[index];
                var inventoryRecord = inventoryRecords[index];
                decimal purchasePrice = item.PurchasePrice ?? inventoryRecord.PurchasePrice;
                decimal lineTotal = item.SalePrice * item.Quantity;

                await InsertSaleItemAsync(connection, request, saleId, item, inventoryRecord, purchasePrice, lineTotal);
                await UpdateInventoryQuantityAsync(connection, request.InventoryTableName, inventoryRecord, item.Quantity);
                await InsertInventoryMovementAsync(connection, inventoryRecord, item, saleId);
            }

            await _auditLogService.LogAsync(request.Settings, new AuditLogEntry
            {
                Username = User.Identity?.Name ?? "unknown",
                Action = "create_sale",
                EntityType = request.SalesTableName,
                EntityId = saleId.ToString(CultureInfo.InvariantCulture),
                Details = $"Оформлена продажа на сумму {totalAmount.ToString(CultureInfo.InvariantCulture)}; позиций: {request.Items.Count}; клиент: {request.CustomerName}"
            }, HttpContext);

            return Json(new { success = true, saleId, message = "Продажа успешно оформлена." });
        }
        catch (Exception ex)
        {
            return Json(new
            {
                success = false,
                message = ex.Message,
                details = ex.InnerException?.Message
            });
        }
    }

    [HttpPost]
    public async Task<IActionResult> GetOrders([FromBody] SalesHistoryRequest request)
    {
        var validationErrors = RequestValidationHelper.ValidateObjectGraph(request);
        if (validationErrors.Count > 0)
            return Json(new { success = false, message = validationErrors[0], errors = validationErrors });

        if (request.Settings == null)
            return Json(new { success = false, message = "Не переданы настройки подключения к базе данных." });

        try
        {
            await _auditLogService.RememberDbSettingsAsync(HttpContext, request.Settings);
            await _auditLogService.RememberDbSettingsAsync(HttpContext, request.Settings);
            using var connection = new MySqlConnection(request.Settings.GetConnectionString());
            await connection.OpenAsync();
            await EnsureSalesTablesAsync(connection, request.SalesTableName, request.SaleItemsTableName);

            string filter = request.Search?.Trim() ?? string.Empty;
            int limit = request.Limit is > 0 and <= 200 ? request.Limit.Value : 50;

            string sql = $@"
                SELECT s.id,
                       s.sale_date,
                       s.customer_name,
                       s.customer_phone,
                       s.customer_email,
                       s.note,
                       s.total_amount,
                       s.payment_method,
                       s.status,
                       COALESCE(SUM(si.quantity), 0) AS total_quantity,
                       COUNT(si.id) AS positions
                FROM `{request.SalesTableName}` s
                LEFT JOIN `{request.SaleItemsTableName}` si ON si.sale_id = s.id
                WHERE (@search = '' OR s.customer_name LIKE CONCAT('%', @search, '%')
                    OR s.customer_phone LIKE CONCAT('%', @search, '%')
                    OR s.customer_email LIKE CONCAT('%', @search, '%')
                    OR CAST(s.id AS CHAR) LIKE CONCAT('%', @search, '%'))
                GROUP BY s.id, s.sale_date, s.customer_name, s.customer_phone, s.customer_email, s.note, s.total_amount, s.payment_method, s.status
                ORDER BY s.sale_date DESC, s.id DESC
                LIMIT @limit;";

            using var cmd = new MySqlCommand(sql, connection);
            cmd.Parameters.AddWithValue("@search", filter);
            cmd.Parameters.AddWithValue("@limit", limit);

            var orders = new List<object>();
            decimal totalRevenue = 0;
            int totalPositions = 0;
            int totalItems = 0;

            using var reader = await cmd.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                int positions = SafeGetInt(reader, "positions");
                int totalQuantity = SafeGetInt(reader, "total_quantity");
                decimal totalAmount = SafeGetDecimal(reader, "total_amount");

                totalRevenue += totalAmount;
                totalPositions += positions;
                totalItems += totalQuantity;

                orders.Add(new
                {
                    id = SafeGetInt(reader, "id"),
                    saleDate = SafeGetDateTime(reader, "sale_date")?.ToString("dd.MM.yyyy HH:mm", CultureInfo.InvariantCulture) ?? "—",
                    customerName = SafeGetString(reader, "customer_name"),
                    customerPhone = SafeGetString(reader, "customer_phone"),
                    customerEmail = SafeGetString(reader, "customer_email"),
                    note = SafeGetString(reader, "note"),
                    paymentMethod = SafeGetString(reader, "payment_method"),
                    status = SafeGetString(reader, "status"),
                    totalAmount = totalAmount,
                    totalQuantity,
                    positions
                });
            }

            return Json(new
            {
                success = true,
                orders,
                summary = new
                {
                    count = orders.Count,
                    totalRevenue,
                    totalPositions,
                    totalItems
                }
            });
        }
        catch (Exception ex)
        {
            return Json(new { success = false, message = ex.Message });
        }
    }

    [HttpPost]
    public async Task<IActionResult> GetOrderDetails([FromBody] SalesDetailsRequest request)
    {
        var validationErrors = RequestValidationHelper.ValidateObjectGraph(request);
        if (validationErrors.Count > 0)
            return Json(new { success = false, message = validationErrors[0], errors = validationErrors });

        if (request.Settings == null)
            return Json(new { success = false, message = "Не переданы настройки подключения к базе данных." });

        try
        {
            using var connection = new MySqlConnection(request.Settings.GetConnectionString());
            await connection.OpenAsync();
            await EnsureSalesTablesAsync(connection, request.SalesTableName, request.SaleItemsTableName);
            await EnsureInventorySupportTablesAsync(connection);

            string saleSql = $@"
                SELECT id, sale_date, customer_name, customer_phone, customer_email, note, total_amount, payment_method, status
                FROM `{request.SalesTableName}`
                WHERE id = @saleId
                LIMIT 1;";

            using var saleCmd = new MySqlCommand(saleSql, connection);
            saleCmd.Parameters.AddWithValue("@saleId", request.SaleId);
            using var saleReader = await saleCmd.ExecuteReaderAsync();

            if (!await saleReader.ReadAsync())
                return Json(new { success = false, message = "Заказ не найден." });

            var order = new
            {
                id = SafeGetInt(saleReader, "id"),
                saleDate = SafeGetDateTime(saleReader, "sale_date")?.ToString("dd.MM.yyyy HH:mm", CultureInfo.InvariantCulture) ?? "—",
                customerName = SafeGetString(saleReader, "customer_name"),
                customerPhone = SafeGetString(saleReader, "customer_phone"),
                customerEmail = SafeGetString(saleReader, "customer_email"),
                note = SafeGetString(saleReader, "note"),
                paymentMethod = SafeGetString(saleReader, "payment_method"),
                status = SafeGetString(saleReader, "status"),
                totalAmount = SafeGetDecimal(saleReader, "total_amount")
            };

            await saleReader.CloseAsync();

            string itemsSql = $@"
                SELECT si.id,
                       si.part_id,
                       si.part_number,
                       si.part_name,
                       si.quantity,
                       si.sale_price,
                       si.purchase_price,
                       si.line_total,
                       COALESCE(r.returned_quantity, 0) AS returned_quantity
                FROM `{request.SaleItemsTableName}` si
                LEFT JOIN (
                    SELECT sale_item_id, SUM(quantity) AS returned_quantity
                    FROM `sales_return_items`
                    GROUP BY sale_item_id
                ) r ON r.sale_item_id = si.id
                WHERE si.sale_id = @saleId
                ORDER BY si.id;";

            using var itemsCmd = new MySqlCommand(itemsSql, connection);
            itemsCmd.Parameters.AddWithValue("@saleId", request.SaleId);

            var items = new List<object>();
            decimal estimatedProfit = 0;
            int totalQuantity = 0;

            using var itemsReader = await itemsCmd.ExecuteReaderAsync();
            while (await itemsReader.ReadAsync())
            {
                int quantity = SafeGetInt(itemsReader, "quantity");
                decimal salePrice = SafeGetDecimal(itemsReader, "sale_price");
                decimal purchasePrice = SafeGetDecimal(itemsReader, "purchase_price");
                decimal lineTotal = SafeGetDecimal(itemsReader, "line_total");
                estimatedProfit += (salePrice - purchasePrice) * quantity;
                totalQuantity += quantity;

                items.Add(new
                {
                    id = SafeGetInt(itemsReader, "id"),
                    partId = SafeGetNullableInt(itemsReader, "part_id"),
                    partNumber = SafeGetString(itemsReader, "part_number"),
                    partName = SafeGetString(itemsReader, "part_name"),
                    quantity,
                    salePrice,
                    purchasePrice,
                    lineTotal,
                    returnedQuantity = SafeGetInt(itemsReader, "returned_quantity"),
                    availableReturnQuantity = Math.Max(0, quantity - SafeGetInt(itemsReader, "returned_quantity"))
                });
            }

            return Json(new
            {
                success = true,
                order,
                items,
                summary = new
                {
                    totalQuantity,
                    estimatedProfit
                }
            });
        }
        catch (Exception ex)
        {
            return Json(new { success = false, message = ex.Message });
        }
    }

    private static async Task<int> InsertSaleAsync(MySqlConnection connection, SalesOrderRequest request, decimal totalAmount)
    {
        string insertSaleSql = $@"
            INSERT INTO `{request.SalesTableName}`
            (sale_date, customer_name, customer_phone, customer_email, note, total_amount, payment_method, status)
            VALUES (@sale_date, @customer_name, @customer_phone, @customer_email, @note, @total_amount, @payment_method, @status);";

        await using var insertSaleCmd = new MySqlCommand(insertSaleSql, connection);
        insertSaleCmd.Parameters.AddWithValue("@sale_date", DateTime.Now);
        insertSaleCmd.Parameters.AddWithValue("@customer_name", request.CustomerName);
        insertSaleCmd.Parameters.AddWithValue("@customer_phone", (object?)request.CustomerPhone ?? DBNull.Value);
        insertSaleCmd.Parameters.AddWithValue("@customer_email", (object?)request.CustomerEmail ?? DBNull.Value);
        insertSaleCmd.Parameters.AddWithValue("@note", (object?)request.Note ?? DBNull.Value);
        insertSaleCmd.Parameters.AddWithValue("@total_amount", totalAmount);
        insertSaleCmd.Parameters.AddWithValue("@payment_method", request.PaymentMethod);
        insertSaleCmd.Parameters.AddWithValue("@status", request.Status);
        await insertSaleCmd.ExecuteNonQueryAsync();
        return Convert.ToInt32(insertSaleCmd.LastInsertedId, CultureInfo.InvariantCulture);
    }

    private static async Task InsertSaleItemAsync(
        MySqlConnection connection,
        SalesOrderRequest request,
        int saleId,
        SalesOrderItemRequest item,
        InventoryRecord inventoryRecord,
        decimal purchasePrice,
        decimal lineTotal)
    {
        string insertItemSql = $@"
            INSERT INTO `{request.SaleItemsTableName}`
            (sale_id, part_id, part_number, part_name, quantity, sale_price, purchase_price, line_total)
            VALUES (@sale_id, @part_id, @part_number, @part_name, @quantity, @sale_price, @purchase_price, @line_total);";

        await using var insertItemCmd = new MySqlCommand(insertItemSql, connection);
        insertItemCmd.Parameters.AddWithValue("@sale_id", saleId);
        insertItemCmd.Parameters.AddWithValue("@part_id", inventoryRecord.PartId.HasValue ? inventoryRecord.PartId.Value : DBNull.Value);
        insertItemCmd.Parameters.AddWithValue("@part_number", inventoryRecord.PartNumber);
        insertItemCmd.Parameters.AddWithValue("@part_name", item.PartName);
        insertItemCmd.Parameters.AddWithValue("@quantity", item.Quantity);
        insertItemCmd.Parameters.AddWithValue("@sale_price", item.SalePrice);
        insertItemCmd.Parameters.AddWithValue("@purchase_price", purchasePrice);
        insertItemCmd.Parameters.AddWithValue("@line_total", lineTotal);
        await insertItemCmd.ExecuteNonQueryAsync();
    }

    private static async Task UpdateInventoryQuantityAsync(MySqlConnection connection, string inventoryTableName, InventoryRecord inventoryRecord, int quantity)
    {
        string whereClause = inventoryRecord.PartId.HasValue && !string.IsNullOrWhiteSpace(inventoryRecord.IdColumn)
            ? $"`{inventoryRecord.IdColumn}` = @whereValue"
            : $"`{inventoryRecord.IdentifierColumn}` = @whereValue";
        string updateSql = $"UPDATE `{inventoryTableName}` SET `{inventoryRecord.QuantityColumn}` = `{inventoryRecord.QuantityColumn}` - @qty WHERE {whereClause};";

        await using var updateCmd = new MySqlCommand(updateSql, connection);
        updateCmd.Parameters.AddWithValue("@qty", quantity);
        updateCmd.Parameters.AddWithValue("@whereValue", inventoryRecord.PartId.HasValue
            ? inventoryRecord.PartId.Value
            : inventoryRecord.IdentifierValue);
        await updateCmd.ExecuteNonQueryAsync();
    }


    private static async Task EnsureInventorySupportTablesAsync(MySqlConnection connection)
    {
        string movementsSql = @"
            CREATE TABLE IF NOT EXISTS `inventory_movements` (
                `id` INT AUTO_INCREMENT PRIMARY KEY,
                `movement_date` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                `movement_type` VARCHAR(50) NOT NULL,
                `part_id` INT NULL,
                `part_number` VARCHAR(100) NULL,
                `part_name` VARCHAR(150) NULL,
                `quantity_change` INT NOT NULL,
                `unit_price` DECIMAL(10,2) NOT NULL DEFAULT 0,
                `reference_type` VARCHAR(50) NULL,
                `reference_id` INT NULL,
                `comment` VARCHAR(255) NULL,
                INDEX (`part_id`),
                INDEX (`part_number`)
            );";

        string returnsSql = @"
            CREATE TABLE IF NOT EXISTS `sales_returns` (
                `id` INT AUTO_INCREMENT PRIMARY KEY,
                `sale_id` INT NOT NULL,
                `return_date` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                `customer_name` VARCHAR(150) NULL,
                `note` VARCHAR(255) NULL,
                `total_amount` DECIMAL(10,2) NOT NULL DEFAULT 0,
                `status` VARCHAR(50) NOT NULL DEFAULT 'completed',
                INDEX (`sale_id`)
            );";

        string returnItemsSql = @"
            CREATE TABLE IF NOT EXISTS `sales_return_items` (
                `id` INT AUTO_INCREMENT PRIMARY KEY,
                `return_id` INT NOT NULL,
                `sale_item_id` INT NOT NULL,
                `part_id` INT NULL,
                `part_number` VARCHAR(100) NULL,
                `part_name` VARCHAR(150) NOT NULL,
                `quantity` INT NOT NULL,
                `sale_price` DECIMAL(10,2) NOT NULL,
                `line_total` DECIMAL(10,2) NOT NULL,
                INDEX (`return_id`),
                INDEX (`sale_item_id`)
            );";

        foreach (var sql in new[] { movementsSql, returnsSql, returnItemsSql })
        {
            using var cmd = new MySqlCommand(sql, connection);
            await cmd.ExecuteNonQueryAsync();
        }
    }

    private static async Task InsertInventoryMovementAsync(MySqlConnection connection, InventoryRecord inventoryRecord, SalesOrderItemRequest item, int saleId)
    {
        string sql = @"
            INSERT INTO `inventory_movements`
            (movement_date, movement_type, part_id, part_number, part_name, quantity_change, unit_price, reference_type, reference_id, comment)
            VALUES (@movement_date, @movement_type, @part_id, @part_number, @part_name, @quantity_change, @unit_price, @reference_type, @reference_id, @comment);";

        using var cmd = new MySqlCommand(sql, connection);
        cmd.Parameters.AddWithValue("@movement_date", DateTime.Now);
        cmd.Parameters.AddWithValue("@movement_type", "sale");
        cmd.Parameters.AddWithValue("@part_id", inventoryRecord.PartId.HasValue ? inventoryRecord.PartId.Value : DBNull.Value);
        cmd.Parameters.AddWithValue("@part_number", inventoryRecord.PartNumber);
        cmd.Parameters.AddWithValue("@part_name", item.PartName);
        cmd.Parameters.AddWithValue("@quantity_change", -item.Quantity);
        cmd.Parameters.AddWithValue("@unit_price", item.SalePrice);
        cmd.Parameters.AddWithValue("@reference_type", "sale");
        cmd.Parameters.AddWithValue("@reference_id", saleId);
        cmd.Parameters.AddWithValue("@comment", $"Продажа по заказу #{saleId}");
        await cmd.ExecuteNonQueryAsync();
    }

    private static async Task EnsureSalesTablesAsync(MySqlConnection connection, string salesTableName, string saleItemsTableName)
    {
        string createSalesSql = $@"
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

        string createSaleItemsSql = $@"
            CREATE TABLE IF NOT EXISTS `{saleItemsTableName}` (
                `id` INT AUTO_INCREMENT PRIMARY KEY,
                `sale_id` INT NOT NULL,
                `part_id` INT NULL,
                `part_number` VARCHAR(100) NULL,
                `part_name` VARCHAR(150) NOT NULL,
                `quantity` INT NOT NULL,
                `sale_price` DECIMAL(10,2) NOT NULL,
                `purchase_price` DECIMAL(10,2) NOT NULL DEFAULT 0,
                `line_total` DECIMAL(10,2) NOT NULL,
                INDEX (`sale_id`)
            );";

        using var cmd1 = new MySqlCommand(createSalesSql, connection);
        await cmd1.ExecuteNonQueryAsync();
        using var cmd2 = new MySqlCommand(createSaleItemsSql, connection);
        await cmd2.ExecuteNonQueryAsync();
    }

    private static async Task<InventoryTableMetadata> GetInventoryTableMetadataAsync(MySqlConnection connection, string tableName)
    {
        const string schemaSql = @"
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = @tableName;";

        using var cmd = new MySqlCommand(schemaSql, connection);
        cmd.Parameters.AddWithValue("@tableName", tableName);

        var columns = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            if (!reader.IsDBNull(0))
            {
                var columnName = reader.GetString(0);
                if (!string.IsNullOrWhiteSpace(columnName))
                    columns.Add(columnName);
            }
        }

        if (columns.Count == 0)
            throw new Exception($"Таблица '{tableName}' не найдена или не содержит колонок.");

        string? quantityColumn = ResolveColumn(columns, "quantity", "qty", "stock", "остаток", "количество", "кол_во", "кол-во");
        if (string.IsNullOrWhiteSpace(quantityColumn))
            throw new Exception($"В таблице '{tableName}' отсутствует колонка остатка. Нужна quantity, stock или похожее поле.");

        return new InventoryTableMetadata
        {
            Columns = columns,
            IdColumn = ResolveColumn(columns, "id"),
            PartNumberColumn = ResolveColumn(columns, "part_number", "sku", "article", "artikul", "артикул", "код"),
            PurchasePriceColumn = ResolveColumn(columns, "purchase_price", "cost_price", "cost", "zakup_price", "buy_price", "purchaseprice", "price"),
            QuantityColumn = quantityColumn
        };
    }

    private static string? ResolveColumn(HashSet<string> columns, params string[] candidates)
    {
        foreach (var candidate in candidates)
        {
            var exact = columns.FirstOrDefault(c => string.Equals(c, candidate, StringComparison.OrdinalIgnoreCase));
            if (exact != null) return exact;
        }

        foreach (var candidate in candidates)
        {
            var normalizedCandidate = Normalize(candidate);
            var fuzzy = columns.FirstOrDefault(c => Normalize(c).Contains(normalizedCandidate, StringComparison.OrdinalIgnoreCase));
            if (fuzzy != null) return fuzzy;
        }

        return null;
    }

    private static string Normalize(string value)
    {
        return value.Trim().Replace(" ", "").Replace("_", "").Replace("-", "").ToLowerInvariant();
    }

    private static async Task<InventoryRecord?> GetInventoryRowAsync(MySqlConnection connection, string tableName, InventoryTableMetadata metadata, SalesOrderItemRequest item)
    {
        string? identifierColumn = metadata.IdColumn != null && !string.IsNullOrWhiteSpace(item.PartId)
            ? metadata.IdColumn
            : metadata.PartNumberColumn != null && !string.IsNullOrWhiteSpace(item.PartNumber)
                ? metadata.PartNumberColumn
                : null;

        if (identifierColumn == null)
            throw new Exception($"Не удалось определить ключ товара в таблице '{tableName}'. Ожидается id, part_number, SKU или article.");

        string purchasePriceSelect = metadata.PurchasePriceColumn != null
            ? $"`{metadata.PurchasePriceColumn}`"
            : "0";

        string partNumberSelect = metadata.PartNumberColumn != null
            ? $"`{metadata.PartNumberColumn}`"
            : "NULL";

        string idSelect = metadata.IdColumn != null
            ? $"`{metadata.IdColumn}`"
            : "NULL";

        string query = $@"
            SELECT {idSelect} AS part_id,
                   {partNumberSelect} AS part_number,
                   `{metadata.QuantityColumn}` AS quantity,
                   {purchasePriceSelect} AS purchase_price
            FROM `{tableName}`
            WHERE `{identifierColumn}` = @identifier
            LIMIT 1;";

        using var cmd = new MySqlCommand(query, connection);
        object identifierValue = identifierColumn.Equals(metadata.IdColumn, StringComparison.OrdinalIgnoreCase)
            ? ParsePartId(item.PartId)
            : item.PartNumber ?? string.Empty;

        cmd.Parameters.AddWithValue("@identifier", identifierValue);

        using var reader = await cmd.ExecuteReaderAsync();
        if (!await reader.ReadAsync())
            return null;

        var record = new InventoryRecord
        {
            PartId = SafeGetNullableInt(reader, "part_id"),
            PartNumber = SafeGetString(reader, "part_number") is { Length: > 0 } number ? number : item.PartNumber ?? string.Empty,
            Quantity = SafeGetInt(reader, "quantity"),
            PurchasePrice = SafeGetDecimal(reader, "purchase_price"),
            IdentifierColumn = identifierColumn,
            IdentifierValue = Convert.ToString(identifierValue, CultureInfo.InvariantCulture) ?? string.Empty,
            QuantityColumn = metadata.QuantityColumn,
            IdColumn = metadata.IdColumn
        };

        return record;
    }

    private static int ParsePartId(string? partId)
    {
        if (string.IsNullOrWhiteSpace(partId) || !int.TryParse(partId, NumberStyles.Integer, CultureInfo.InvariantCulture, out int parsedId))
            throw new Exception("Не удалось определить идентификатор товара для оформления продажи.");

        return parsedId;
    }

    private static int SafeGetInt(MySqlDataReader reader, string columnName)
    {
        int ordinal = reader.GetOrdinal(columnName);
        return reader.IsDBNull(ordinal) ? 0 : Convert.ToInt32(reader.GetValue(ordinal), CultureInfo.InvariantCulture);
    }

    private static int? SafeGetNullableInt(MySqlDataReader reader, string columnName)
    {
        int ordinal = reader.GetOrdinal(columnName);
        return reader.IsDBNull(ordinal) ? null : Convert.ToInt32(reader.GetValue(ordinal), CultureInfo.InvariantCulture);
    }

    private static decimal SafeGetDecimal(MySqlDataReader reader, string columnName)
    {
        int ordinal = reader.GetOrdinal(columnName);
        return reader.IsDBNull(ordinal) ? 0 : Convert.ToDecimal(reader.GetValue(ordinal), CultureInfo.InvariantCulture);
    }

    private static string SafeGetString(MySqlDataReader reader, string columnName)
    {
        int ordinal = reader.GetOrdinal(columnName);
        return reader.IsDBNull(ordinal) ? string.Empty : reader.GetValue(ordinal)?.ToString() ?? string.Empty;
    }

    private static DateTime? SafeGetDateTime(MySqlDataReader reader, string columnName)
    {
        int ordinal = reader.GetOrdinal(columnName);
        return reader.IsDBNull(ordinal) ? null : Convert.ToDateTime(reader.GetValue(ordinal), CultureInfo.InvariantCulture);
    }

    private sealed class InventoryTableMetadata
    {
        public HashSet<string> Columns { get; set; } = new(StringComparer.OrdinalIgnoreCase);
        public string? IdColumn { get; set; }
        public string? PartNumberColumn { get; set; }
        public string? PurchasePriceColumn { get; set; }
        public string QuantityColumn { get; set; } = "quantity";
    }

    private sealed class InventoryRecord
    {
        public int? PartId { get; set; }
        public string PartNumber { get; set; } = string.Empty;
        public int Quantity { get; set; }
        public decimal PurchasePrice { get; set; }
        public string IdentifierColumn { get; set; } = string.Empty;
        public string IdentifierValue { get; set; } = string.Empty;
        public string QuantityColumn { get; set; } = "quantity";
        public string? IdColumn { get; set; }
    }


public sealed class SalesHistoryRequest
{
    [Required(ErrorMessage = "Не переданы настройки подключения к базе данных.")]
    public DbSettings Settings { get; set; } = new();

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

public sealed class SalesDetailsRequest
{
    [Required(ErrorMessage = "Не переданы настройки подключения к базе данных.")]
    public DbSettings Settings { get; set; } = new();

    [Required]
    [StringLength(64)]
    public string SalesTableName { get; set; } = "sales";

    [Required]
    [StringLength(64)]
    public string SaleItemsTableName { get; set; } = "sale_items";

    [Range(1, int.MaxValue, ErrorMessage = "Не выбран заказ для просмотра.")]
    public int SaleId { get; set; }
}
}
