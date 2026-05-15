window.currentDbSettings = window.currentDbSettings || null;
let loadedRows = [];
let loadedColumns = [];
const lookupState = { suppliers: [], categories: [] };

async function apiRequest(url, data) {
    const response = await fetch(url, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(data)
    });
    return await response.json();
}

async function apiRequestRaw(url, data) {
    return await fetch(url, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(data)
    });
}

function hasPartsPage() {
    return !!document.getElementById('tableComboBox');
}

function parseNumber(value) {
    if (value === null || value === undefined) return 0;
    if (typeof value === 'number') return Number.isFinite(value) ? value : 0;
    const normalized = String(value).trim().replace(/\s+/g, '').replace(/,/g, '.');
    const parsed = Number(normalized);
    return Number.isFinite(parsed) ? parsed : 0;
}

function formatMoney(value) {
    return `${parseNumber(value).toLocaleString('ru-RU', { minimumFractionDigits: 0, maximumFractionDigits: 2 })} BYN`;
}


const AppValidation = (() => {
    const phoneRegex = /^[0-9+()\-\s]{7,25}$/;
    const phonePattern = phoneRegex.source;
    const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;

    function byId(id) {
        return typeof id === 'string' ? document.getElementById(id) : id;
    }

    function setAttributes(id, attrs) {
        const element = byId(id);
        if (!element) return;
        Object.entries(attrs).forEach(([name, value]) => {
            if (value === false || value === null || value === undefined) {
                element.removeAttribute(name);
                if (name === 'required') element.required = false;
                return;
            }
            if (name === 'required') {
                element.required = Boolean(value);
                return;
            }
            if (name === 'type') {
                element.type = value;
                return;
            }
            element.setAttribute(name, String(value));
        });
    }

    function clearInvalid(element) {
        const input = byId(element);
        if (!input) return;
        input.classList.remove('is-invalid');
        input.setCustomValidity('');
    }

    function setInvalid(element, message) {
        const input = byId(element);
        if (!input) return;
        input.classList.add('is-invalid');
        input.setCustomValidity(message);
    }

    function validateElements(elements) {
        const list = (elements || []).map(byId).filter(Boolean);
        let firstInvalid = null;

        list.forEach((element) => {
            clearInvalid(element);
            if (element.disabled) return;

            const rawValue = element.type === 'checkbox' ? (element.checked ? '1' : '') : String(element.value ?? '');
            const value = rawValue.trim();
            const label = element.dataset.label || element.getAttribute('aria-label') || element.name || 'поле';

            if (element.required && !value) {
                setInvalid(element, `Заполни поле: ${label}.`);
                firstInvalid ??= element;
                return;
            }

            if (value && element.type === 'email' && !emailRegex.test(value)) {
                setInvalid(element, `Укажи корректный email для поля: ${label}.`);
                firstInvalid ??= element;
                return;
            }

            const pattern = element.getAttribute('pattern');
            if (value && pattern) {
                try {
                    const regex = new RegExp(`^(?:${pattern})$`);
                    if (!regex.test(value)) {
                        setInvalid(element, element.dataset.invalidMessage || `Некорректный формат поля: ${label}.`);
                        firstInvalid ??= element;
                        return;
                    }
                } catch { }
            }

            const maxLength = Number(element.getAttribute('maxlength'));
            if (value && Number.isFinite(maxLength) && maxLength > 0 && value.length > maxLength) {
                setInvalid(element, `Поле ${label} не должно быть длиннее ${maxLength} символов.`);
                firstInvalid ??= element;
                return;
            }

            const isNumeric = element.type === 'number' || element.dataset.validateNumber === 'true';
            if (value && isNumeric) {
                const numericValue = parseNumber(value);
                const min = element.getAttribute('min');
                const max = element.getAttribute('max');
                if (min !== null && numericValue < Number(min)) {
                    setInvalid(element, `Значение поля ${label} не может быть меньше ${min}.`);
                    firstInvalid ??= element;
                    return;
                }
                if (max !== null && numericValue > Number(max)) {
                    setInvalid(element, `Значение поля ${label} не может быть больше ${max}.`);
                    firstInvalid ??= element;
                    return;
                }
            }
        });

        if (firstInvalid) {
            firstInvalid.reportValidity();
            firstInvalid.focus();
            return false;
        }

        return true;
    }

    function validateDbSettings(prefix) {
        return validateElements([
            `${prefix}Server`,
            `${prefix}Port`,
            `${prefix}Name`,
            `${prefix}User`,
            `${prefix}Pass`
        ]);
    }

    function validateSalesOrderForm() {
        if (!document.querySelector('#salesCartTableBody tr')) {
            alert('Добавь хотя бы один товар в корзину продажи.');
            return false;
        }
        const cartInputs = [...document.querySelectorAll('#salesCartTableBody input[type="number"]')];
        return validateElements(['customerName', 'customerPhone', 'customerEmail', ...cartInputs]);
    }

    function validatePurchaseForm() {
        if (!document.querySelector('#purchaseCartTableBody tr')) {
            alert('Добавь хотя бы один товар в корзину закупки.');
            return false;
        }
        const cartInputs = [...document.querySelectorAll('#purchaseCartTableBody input[type="number"]')];
        return validateElements(['supplierName', 'supplierPhone', 'purchaseDocumentNumber', ...cartInputs]);
    }

    function validateEditForm() {
        const form = document.getElementById('editForm');
        if (!form) return true;
        const fields = [...form.querySelectorAll('input, select, textarea')].filter(field => !field.readOnly && field.type !== 'hidden');
        return validateElements(fields);
    }

    function applyDynamicEditValidation(container = document.getElementById('dynamicFormFields')) {
        if (!container) return;
        container.querySelectorAll('input, select, textarea').forEach((field) => {
            const name = (field.name || '').toLowerCase();
            field.dataset.label = field.name || 'поле';

            if (name.includes('email')) {
                field.type = 'email';
                field.setAttribute('maxlength', '150');
            }
            if (name.includes('phone') || name.includes('телефон')) {
                field.type = 'tel';
                field.setAttribute('pattern', phonePattern);
                field.setAttribute('maxlength', '25');
                field.dataset.invalidMessage = 'Телефон может содержать только цифры, пробелы, +, скобки и дефисы.';
            }
            if (name.includes('price') || name.includes('cost') || name.includes('amount') || name.includes('цена') || name.includes('стоимость')) {
                field.type = 'number';
                field.setAttribute('step', '0.01');
                field.setAttribute('min', '0');
                field.dataset.validateNumber = 'true';
            }
            if (name.includes('quantity') || name.includes('stock') || name.includes('остат') || name.includes('count') || name.includes('min_quantity')) {
                field.type = 'number';
                field.setAttribute('step', '1');
                field.setAttribute('min', '0');
                field.dataset.validateNumber = 'true';
            }
            if (['name', 'part_name', 'part_number', 'sku', 'article', 'brand', 'car_brand', 'car_model'].some(key => name.includes(key))) {
                field.setAttribute('maxlength', '150');
            }
            if (['part_name', 'name', 'part_number', 'sku', 'article'].includes(name)) {
                field.required = true;
            }
        });
    }

    function applyStandardAttributes() {
        [['db', 'Подключение к БД'], ['salesDb', 'Подключение к БД'], ['purchaseDb', 'Подключение к БД'], ['supDb', 'Подключение к БД'], ['catDb', 'Подключение к БД'], ['financeDb', 'Подключение к БД']]
            .forEach(([prefix]) => {
                setAttributes(`${prefix}Server`, { required: true, maxlength: 100 });
                setAttributes(`${prefix}Port`, { required: true, type: 'number', min: 1, max: 65535, step: 1, inputmode: 'numeric' });
                setAttributes(`${prefix}Name`, { required: true, maxlength: 100 });
                setAttributes(`${prefix}User`, { required: true, maxlength: 100 });
                setAttributes(`${prefix}Pass`, { maxlength: 100 });
            });

        setAttributes('supplierNameForm', { required: true, maxlength: 150 });
        setAttributes('supplierPhoneForm', { type: 'tel', pattern: phonePattern, maxlength: 25 });
        setAttributes('supplierEmailForm', { type: 'email', maxlength: 150 });
        setAttributes('supplierContactForm', { maxlength: 100 });
        setAttributes('supplierAddressForm', { maxlength: 200 });
        setAttributes('supplierNoteForm', { maxlength: 500 });
        setAttributes('categoryNameForm', { required: true, maxlength: 100 });
        setAttributes('categoryDescriptionForm', { maxlength: 500 });
        setAttributes('customerName', { required: true, maxlength: 150 });
        setAttributes('customerPhone', { type: 'tel', pattern: phonePattern, maxlength: 25 });
        setAttributes('customerEmail', { type: 'email', maxlength: 150 });
        setAttributes('saleNote', { maxlength: 500 });
        setAttributes('supplierName', { required: true, maxlength: 150 });
        setAttributes('supplierPhone', { type: 'tel', pattern: phonePattern, maxlength: 25 });
        setAttributes('purchaseDocumentNumber', { maxlength: 100 });
        setAttributes('purchaseNote', { maxlength: 500 });
        setAttributes('minPriceFilter', { type: 'number', min: 0, step: '0.01' });
        setAttributes('maxPriceFilter', { type: 'number', min: 0, step: '0.01' });

        [
            ['supplierNameForm', 'Название поставщика'],
            ['supplierPhoneForm', 'Телефон поставщика'],
            ['supplierEmailForm', 'Email поставщика'],
            ['categoryNameForm', 'Название категории'],
            ['customerName', 'Имя клиента'],
            ['customerPhone', 'Телефон клиента'],
            ['customerEmail', 'Email клиента'],
            ['supplierName', 'Поставщик'],
            ['supplierPhone', 'Телефон поставщика'],
            ['purchaseDocumentNumber', 'Номер документа'],
            ['dbServer', 'Сервер'], ['dbPort', 'Порт'], ['dbName', 'База данных'], ['dbUser', 'Логин'], ['dbPass', 'Пароль'],
            ['salesDbServer', 'Сервер'], ['salesDbPort', 'Порт'], ['salesDbName', 'База данных'], ['salesDbUser', 'Логин'], ['salesDbPass', 'Пароль'],
            ['purchaseDbServer', 'Сервер'], ['purchaseDbPort', 'Порт'], ['purchaseDbName', 'База данных'], ['purchaseDbUser', 'Логин'], ['purchaseDbPass', 'Пароль'],
            ['supDbServer', 'Сервер'], ['supDbPort', 'Порт'], ['supDbName', 'База данных'], ['supDbUser', 'Логин'], ['supDbPass', 'Пароль'],
            ['catDbServer', 'Сервер'], ['catDbPort', 'Порт'], ['catDbName', 'База данных'], ['catDbUser', 'Логин'], ['catDbPass', 'Пароль'],
            ['financeDbServer', 'Сервер'], ['financeDbPort', 'Порт'], ['financeDbName', 'База данных'], ['financeDbUser', 'Логин'], ['financeDbPass', 'Пароль']
        ].forEach(([id, label]) => {
            const element = byId(id);
            if (element) element.dataset.label = label;
        });

        document.querySelectorAll('input[data-return-input]').forEach((input) => {
            input.dataset.label = 'Количество возврата';
            input.setAttribute('min', input.getAttribute('min') || '1');
            input.dataset.validateNumber = 'true';
        });

        applyDynamicEditValidation();
    }

    document.addEventListener('input', (event) => {
        const field = event.target.closest('input, select, textarea');
        if (field) clearInvalid(field);
    });

    document.addEventListener('click', (event) => {
        const button = event.target.closest('button, [role="button"]');
        if (!button) return;

        let valid = true;
        switch (button.id) {
            case 'btnDoConnect': valid = validateDbSettings('db'); break;
            case 'btnSalesConnect': valid = validateDbSettings('salesDb'); break;
            case 'btnPurchaseConnect': valid = validateDbSettings('purchaseDb'); break;
            case 'btnSuppliersConnect': valid = validateDbSettings('supDb'); break;
            case 'btnCategoriesConnect': valid = validateDbSettings('catDb'); break;
            case 'btnFinanceConnect': valid = validateDbSettings('financeDb'); break;
            case 'saveSupplierBtn': valid = validateElements(['supplierNameForm', 'supplierPhoneForm', 'supplierEmailForm', 'supplierContactForm', 'supplierAddressForm', 'supplierNoteForm']); break;
            case 'saveCategoryBtn': valid = validateElements(['categoryNameForm', 'categoryDescriptionForm']); break;
            case 'submitSalesOrderBtn': valid = validateSalesOrderForm(); break;
            case 'submitPurchaseBtn': valid = validatePurchaseForm(); break;
            case 'saveEditBtn': valid = validateEditForm(); break;
            default:
                if (button.dataset.action === 'return-item') {
                    const input = button.parentElement?.querySelector('input[data-return-input]');
                    valid = validateElements([input]);
                }
                break;
        }

        if (!valid) {
            event.preventDefault();
            event.stopImmediatePropagation();
        }
    }, true);

    return {
        validateElements,
        validateDbSettings,
        validateSalesOrderForm,
        validatePurchaseForm,
        validateEditForm,
        applyStandardAttributes,
        applyDynamicEditValidation,
        clearInvalid
    };
})();

window.AppValidation = AppValidation;

function getSettingsFromForm() {
    return {
        server: document.getElementById('dbServer').value,
        port: document.getElementById('dbPort').value,
        database: document.getElementById('dbName').value,
        username: document.getElementById('dbUser').value,
        password: document.getElementById('dbPass').value
    };
}

function fillSettingsForm(settings) {
    if (!settings) return;
    const map = {
        dbServer: settings.server,
        dbPort: settings.port,
        dbName: settings.database,
        dbUser: settings.username,
        dbPass: settings.password
    };
    Object.entries(map).forEach(([id, value]) => {
        const el = document.getElementById(id);
        if (el) el.value = value || '';
    });
}

function getRowValue(row, ...candidates) {
    for (const candidate of candidates) {
        const key = Object.keys(row).find(k => k.toLowerCase() === String(candidate).toLowerCase());
        if (key) return row[key] ?? '';
    }
    return '';
}

function getResolvedQuantity(row) {
    return parseNumber(getRowValue(row, '__resolvedQuantity', 'quantity', 'stock'));
}

function getResolvedPrice(row) {
    const direct = getRowValue(row, '__resolvedPrice', 'sale_price', 'retail_price', 'selling_price', 'unit_price', 'price');
    if (direct !== '') return parseNumber(direct);
    const keys = Object.keys(row);
    const matchedKey = keys.find(key => {
        const normalized = key.toLowerCase();
        if (normalized.includes('purchase') || normalized.includes('cost')) return false;
        return normalized.includes('price') || normalized.includes('цена') || normalized.includes('стоимость');
    });
    return matchedKey ? parseNumber(row[matchedKey]) : 0;
}

function getResolvedMinQuantity(row) {
    return parseNumber(getRowValue(row, 'min_quantity', 'min_stock', 'reorder_level', 'минимальный_остаток')) || 5;
}

function getRowIdentifier(row) {
    return getRowValue(row, 'id', 'SKU', 'sku', '__resolvedPartNumber', 'part_number');
}

function getResolvedCategory(row) {
    const direct = getRowValue(row, '__resolvedCategory', 'category', 'category_name', 'категория');
    if (direct) return direct;
    const categoryId = parseNumber(getRowValue(row, '__resolvedCategoryId', 'category_id'));
    return lookupState.categories.find(x => Number(x.id) === categoryId)?.name || '';
}

function getResolvedSupplier(row) {
    const direct = getRowValue(row, '__resolvedSupplier', 'supplier', 'supplier_name', 'vendor', 'поставщик');
    if (direct) return direct;
    const supplierId = parseNumber(getRowValue(row, '__resolvedSupplierId', 'supplier_id'));
    return lookupState.suppliers.find(x => Number(x.id) === supplierId)?.name || '';
}

function getResolvedBrand(row) {
    return getRowValue(row, '__resolvedBrand', 'brand', 'manufacturer', 'бренд');
}

function getResolvedModel(row) {
    return getRowValue(row, '__resolvedCarModel', 'car_model', 'model', 'модель');
}

function getResolvedCarBrand(row) {
    return getRowValue(row, '__resolvedCarBrand', 'car_brand', 'make', 'маркаавто');
}

function updateStats(stats) {
    const total = document.getElementById('totalPositionsCount');
    const inStock = document.getElementById('inStockCount');
    const outOfStock = document.getElementById('outOfStockCount');
    const totalValue = document.getElementById('totalInventoryValue');
    const partsCount = document.getElementById('partsCountText');

    if (total) total.innerText = stats.total ?? 0;
    if (inStock) inStock.innerText = stats.inStock ?? 0;
    if (outOfStock) outOfStock.innerText = stats.outOfStock ?? 0;
    if (totalValue) totalValue.innerText = `${stats.totalValue ?? 0} BYN`;
    if (partsCount) partsCount.innerText = stats.total ?? 0;
}

function renderLowStockPanel(rows) {
    const tbody = document.getElementById('lowStockTableBody');
    const badge = document.getElementById('lowStockBadge');
    const count = document.getElementById('lowStockCount');
    if (!tbody) return;

    const lowStockRows = (rows || [])
        .map(row => {
            const quantity = getResolvedQuantity(row);
            const minQuantity = getResolvedMinQuantity(row);
            return { row, quantity, minQuantity, isLowStock: quantity <= minQuantity };
        })
        .filter(item => item.isLowStock)
        .sort((a, b) => a.quantity - b.quantity)
        .slice(0, 10);

    if (badge) badge.innerText = `${lowStockRows.length} уведомлений`;
    if (count) count.innerText = lowStockRows.length;

    if (!lowStockRows.length) {
        tbody.innerHTML = '<tr><td colspan="5" class="text-center text-muted py-4">Все позиции в безопасном остатке.</td></tr>';
        return;
    }

    tbody.innerHTML = lowStockRows.map(({ row, quantity, minQuantity }) => {
        const status = quantity === 0 ? '<span class="badge-outofstock">Нет на складе</span>' : '<span class="badge-lowstock">Нужно пополнить</span>';
        return `
            <tr>
                <td class="fw-semibold">${getRowValue(row, '__resolvedPartNumber', 'part_number', 'SKU') || '—'}</td>
                <td>${getRowValue(row, '__resolvedPartName', 'part_name', 'name') || '—'}</td>
                <td>${quantity} шт.</td>
                <td>${minQuantity} шт.</td>
                <td>${status}</td>
            </tr>`;
    }).join('');
}

function renderDynamicTable(rows) {
    const tbody = document.getElementById('partsTableBody');
    if (!tbody) return;

    if (!rows || !rows.length) {
        tbody.innerHTML = '<tr><td colspan="11" class="text-center text-muted py-4">По текущему запросу ничего не найдено.</td></tr>';
        const partsCount = document.getElementById('partsCountText');
        if (partsCount) partsCount.innerText = 0;
        return;
    }

    tbody.innerHTML = rows.map(row => {
        const id = getRowIdentifier(row);
        const partNumber = getRowValue(row, '__resolvedPartNumber', 'part_number', 'SKU');
        const partName = getRowValue(row, '__resolvedPartName', 'part_name', 'name');
        const carBrand = getResolvedCarBrand(row);
        const carModel = getResolvedModel(row);
        const brand = getResolvedBrand(row);
        const category = getResolvedCategory(row);
        const supplier = getResolvedSupplier(row);
        const quantity = getResolvedQuantity(row);
        const price = getResolvedPrice(row);
        const minQuantity = getResolvedMinQuantity(row);

        let status = '<span class="badge-instock">В наличии</span>';
        if (quantity <= 0) status = '<span class="badge-outofstock">Нет</span>';
        else if (quantity <= minQuantity) status = '<span class="badge-lowstock">Низкий остаток</span>';

        return `
            <tr>
                <td class="fw-semibold">${partNumber || '—'}</td>
                <td>${partName || '—'}</td>
                <td>${carBrand || '—'}</td>
                <td>${carModel || '—'}</td>
                <td>${brand || '—'}</td>
                <td>${category || '—'}</td>
                <td>${supplier || '—'}</td>
                <td>${formatMoney(price)}</td>
                <td>${quantity} шт.</td>
                <td>${status}</td>
                <td class="text-end">
                    <button class="btn btn-link btn-sm text-dark p-0 me-2" onclick="openProductCard('${String(id).replace(/'/g, "\\'")}')"><i class="bi bi-eye"></i></button>
                    <button class="btn btn-link btn-sm text-warning p-0" onclick="openEditModal('${String(id).replace(/'/g, "\\'")}')"><i class="bi bi-pencil"></i></button>
                </td>
            </tr>`;
    }).join('');

    const partsCount = document.getElementById('partsCountText');
    if (partsCount) partsCount.innerText = rows.length;
}

function buildSelectOptions(items, placeholder) {
    return [`<option value="">${placeholder}</option>`].concat(items.map(item => `<option value="${item}">${item}</option>`)).join('');
}

function populateFilters(rows) {
    const brands = [...new Set(rows.map(getResolvedBrand).filter(Boolean))].sort((a, b) => a.localeCompare(b, 'ru'));
    const models = [...new Set(rows.map(getResolvedModel).filter(Boolean))].sort((a, b) => a.localeCompare(b, 'ru'));
    const categories = [...new Set(rows.map(getResolvedCategory).filter(Boolean))].sort((a, b) => a.localeCompare(b, 'ru'));
    const suppliers = [...new Set(rows.map(getResolvedSupplier).filter(Boolean))].sort((a, b) => a.localeCompare(b, 'ru'));

    document.getElementById('brandFilter').innerHTML = buildSelectOptions(brands, 'Все');
    document.getElementById('modelFilter').innerHTML = buildSelectOptions(models, 'Все');
    document.getElementById('categoryFilter').innerHTML = buildSelectOptions(categories, 'Все');
    document.getElementById('supplierFilter').innerHTML = buildSelectOptions(suppliers, 'Все');
}

function applyFilters() {
    const query = document.getElementById('tableSearchInput')?.value.trim().toLowerCase() || '';
    const brandFilter = document.getElementById('brandFilter')?.value || '';
    const modelFilter = document.getElementById('modelFilter')?.value || '';
    const categoryFilter = document.getElementById('categoryFilter')?.value || '';
    const supplierFilter = document.getElementById('supplierFilter')?.value || '';
    const stockFilter = document.getElementById('stockFilter')?.value || '';
    const sortFilter = document.getElementById('sortFilter')?.value || 'name_asc';
    const minPrice = parseNumber(document.getElementById('minPriceFilter')?.value || '');
    const maxPriceRaw = document.getElementById('maxPriceFilter')?.value;
    const maxPrice = maxPriceRaw ? parseNumber(maxPriceRaw) : null;

    let filtered = loadedRows.filter(row => {
        const quantity = getResolvedQuantity(row);
        const minQuantity = getResolvedMinQuantity(row);
        const price = getResolvedPrice(row);
        const textMatch = !query || Object.values(row).some(val => String(val).toLowerCase().includes(query)) || getResolvedCategory(row).toLowerCase().includes(query) || getResolvedSupplier(row).toLowerCase().includes(query);
        const brandMatch = !brandFilter || getResolvedBrand(row) === brandFilter;
        const modelMatch = !modelFilter || getResolvedModel(row) === modelFilter;
        const categoryMatch = !categoryFilter || getResolvedCategory(row) === categoryFilter;
        const supplierMatch = !supplierFilter || getResolvedSupplier(row) === supplierFilter;
        const minPriceMatch = !minPrice || price >= minPrice;
        const maxPriceMatch = maxPrice === null || price <= maxPrice;

        let stockMatch = true;
        if (stockFilter === 'instock') stockMatch = quantity > minQuantity;
        if (stockFilter === 'low') stockMatch = quantity > 0 && quantity <= minQuantity;
        if (stockFilter === 'out') stockMatch = quantity <= 0;

        return textMatch && brandMatch && modelMatch && categoryMatch && supplierMatch && minPriceMatch && maxPriceMatch && stockMatch;
    });

    const sorters = {
        name_asc: (a, b) => (getRowValue(a, '__resolvedPartName', 'part_name', 'name') || '').localeCompare(getRowValue(b, '__resolvedPartName', 'part_name', 'name') || '', 'ru'),
        name_desc: (a, b) => (getRowValue(b, '__resolvedPartName', 'part_name', 'name') || '').localeCompare(getRowValue(a, '__resolvedPartName', 'part_name', 'name') || '', 'ru'),
        price_asc: (a, b) => getResolvedPrice(a) - getResolvedPrice(b),
        price_desc: (a, b) => getResolvedPrice(b) - getResolvedPrice(a),
        quantity_asc: (a, b) => getResolvedQuantity(a) - getResolvedQuantity(b),
        quantity_desc: (a, b) => getResolvedQuantity(b) - getResolvedQuantity(a)
    };
    filtered.sort(sorters[sortFilter] || sorters.name_asc);
    renderDynamicTable(filtered);
}

function resetFilters() {
    ['tableSearchInput', 'minPriceFilter', 'maxPriceFilter'].forEach(id => {
        const el = document.getElementById(id);
        if (el) el.value = '';
    });
    ['brandFilter', 'modelFilter', 'categoryFilter', 'supplierFilter', 'stockFilter'].forEach(id => {
        const el = document.getElementById(id);
        if (el) el.value = '';
    });
    const sort = document.getElementById('sortFilter');
    if (sort) sort.value = 'name_asc';
    applyFilters();
}

function openEditModal(id) {
    const row = loadedRows.find(r => String(getRowIdentifier(r)) === String(id));
    if (!row) return;

    const container = document.getElementById('dynamicFormFields');
    container.innerHTML = '';

    Object.keys(row).forEach(key => {
        if (key.startsWith('__resolved')) return;
        const lowerKey = key.toLowerCase();
        const readonly = ['sku', 'id', 'created_at', 'updated_at'].includes(lowerKey) ? 'readonly' : '';
        const compact = ['price', 'quantity', 'stock', 'sale_price', 'purchase_price', 'min_quantity', 'category_id', 'supplier_id'].includes(lowerKey) ? 'col-md-6' : 'col-12';

        if (key.toLowerCase() === 'category_id') {
            const options = ['<option value="">Без категории</option>']
                .concat(lookupState.categories.map(item => `<option value="${item.id}" ${String(row[key] || '') === String(item.id) ? 'selected' : ''}>${item.name}</option>`))
                .join('');
            container.insertAdjacentHTML('beforeend', `<div class="${compact}"><label class="form-label text-muted small fw-bold mb-1">${key}</label><select class="form-select" name="${key}" ${readonly}>${options}</select></div>`);
            return;
        }

        if (key.toLowerCase() === 'supplier_id') {
            const options = ['<option value="">Без поставщика</option>']
                .concat(lookupState.suppliers.map(item => `<option value="${item.id}" ${String(row[key] || '') === String(item.id) ? 'selected' : ''}>${item.name}</option>`))
                .join('');
            container.insertAdjacentHTML('beforeend', `<div class="${compact}"><label class="form-label text-muted small fw-bold mb-1">${key}</label><select class="form-select" name="${key}" ${readonly}>${options}</select></div>`);
            return;
        }

        const inputType = ['quantity', 'stock', 'min_quantity', 'category_id', 'supplier_id'].includes(lowerKey)
            ? 'number'
            : ['sale_price', 'purchase_price', 'price'].includes(lowerKey)
                ? 'number'
                : 'text';
        const inputStep = ['sale_price', 'purchase_price', 'price'].includes(lowerKey) ? 'step="0.01"' : '';
        container.insertAdjacentHTML('beforeend', `
            <div class="${compact}">
                <label class="form-label text-muted small fw-bold mb-1">${key}</label>
                <input type="${inputType}" class="form-control" name="${key}" value="${row[key] ?? ''}" ${inputStep} ${readonly}>
            </div>`);
    });

    AppValidation.applyDynamicEditValidation(container);
    bootstrap.Modal.getOrCreateInstance(document.getElementById('editModal')).show();
}

async function openProductCard(id) {
    const tableName = document.getElementById('tableComboBox')?.value;
    const row = loadedRows.find(r => String(getRowIdentifier(r)) === String(id));
    if (!tableName || !window.currentDbSettings || !row) return;

    const productId = getRowValue(row, 'id');
    const partNumber = getRowValue(row, '__resolvedPartNumber', 'part_number', 'SKU');

    const result = await apiRequest('/Inventory/GetProductCard', {
        settings: window.currentDbSettings,
        inventoryTableName: tableName,
        partId: productId ? String(productId) : null,
        partNumber: partNumber || null,
        movementsLimit: 20
    });

    if (!result.success) {
        alert('Ошибка: ' + result.message);
        return;
    }

    fillProductCard(result);
    bootstrap.Modal.getOrCreateInstance(document.getElementById('productCardModal')).show();
}

function fillProductCard(data) {
    const product = data.product || {};
    const fields = data.fields || {};
    const movements = data.movements || [];
    const totals = data.totals || {};

    document.getElementById('productCardTitle').innerText = product.partNumber ? `· ${product.partNumber}` : '';
    document.getElementById('productCardSubtitle').innerText = product.partName || 'Подробности о позиции, остатках и движении';
    document.getElementById('cardQuantity').innerText = `${parseNumber(product.quantity)} шт.`;
    document.getElementById('cardMinQuantity').innerText = `${parseNumber(product.minQuantity)} шт.`;
    document.getElementById('cardSalePrice').innerText = formatMoney(product.salePrice || 0);
    document.getElementById('cardLowStockStatus').innerText = product.isLowStock ? 'Низкий остаток' : 'В норме';
    document.getElementById('cardStockValue').innerText = `Стоимость остатка ${formatMoney(product.stockValue || 0)}`;
    document.getElementById('cardSoldUnits').innerText = `${parseNumber(totals.soldUnits)} шт.`;
    document.getElementById('cardSoldAmount').innerText = formatMoney(totals.totalSold || 0);
    document.getElementById('cardPurchasedUnits').innerText = `${parseNumber(totals.purchasedUnits)} шт.`;
    document.getElementById('cardPurchasedAmount').innerText = formatMoney(totals.totalPurchased || 0);

    const fieldsContainer = document.getElementById('productCardFields');
    fieldsContainer.innerHTML = Object.entries(fields)
        .filter(([key]) => !key.startsWith('__resolved'))
        .map(([key, value]) => `
            <div class="col-md-6">
                <div class="product-info-field">
                    <div class="product-info-label">${key}</div>
                    <div class="product-info-value">${value || '—'}</div>
                </div>
            </div>`).join('');

    const movementsBody = document.getElementById('productMovementsBody');
    if (!movements.length) {
        movementsBody.innerHTML = '<tr><td colspan="5" class="text-center text-muted py-4">Нет данных по движению товара.</td></tr>';
        return;
    }

    movementsBody.innerHTML = movements.map(item => {
        const qtyText = parseNumber(item.quantityChange) > 0 ? `+${parseNumber(item.quantityChange)} шт.` : `${parseNumber(item.quantityChange)} шт.`;
        return `
            <tr>
                <td>${item.movementDate || '—'}</td>
                <td>${item.movementType || '—'}</td>
                <td>${qtyText}</td>
                <td>${formatMoney(item.unitPrice || 0)}</td>
                <td>${item.comment || '—'}</td>
            </tr>`;
    }).join('');
}

async function loadLookups() {
    const tableName = document.getElementById('tableComboBox')?.value;
    if (!tableName || !window.currentDbSettings) return;
    const result = await apiRequest('/MasterData/GetLookups', {
        settings: window.currentDbSettings,
        inventoryTableName: tableName,
        suppliersTableName: 'suppliers',
        categoriesTableName: 'categories'
    });
    if (result.success) {
        lookupState.suppliers = result.suppliers || [];
        lookupState.categories = result.categories || [];
    }
}

async function refreshData() {
    const tableName = document.getElementById('tableComboBox')?.value;
    if (!tableName || !window.currentDbSettings) return;

    await loadLookups();
    const result = await apiRequest(`/Database/GetTableData?tableName=${encodeURIComponent(tableName)}`, window.currentDbSettings);
    if (!result.success) {
        alert('Ошибка: ' + result.message);
        return;
    }

    loadedRows = result.rows;
    loadedColumns = result.columns;
    updateStats(result.stats);
    renderLowStockPanel(loadedRows);
    populateFilters(loadedRows);
    applyFilters();
}

async function saveChanges() {
    if (window.AppValidation && !window.AppValidation.validateEditForm()) return;

    const form = document.getElementById('editForm');
    const data = Object.fromEntries(new FormData(form).entries());
    const tableName = document.getElementById('tableComboBox').value;

    for (const key of Object.keys(data)) {
        if (typeof data[key] === 'string' && /^\d+,\d+$/.test(data[key])) {
            data[key] = data[key].replace(',', '.');
        }
    }

    const response = await fetch(`/Database/UpdateRecord?tableName=${encodeURIComponent(tableName)}`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ settings: window.currentDbSettings, data })
    });

    const result = await response.json();
    if (!result.success) {
        alert('Ошибка: ' + result.message);
        return;
    }

    bootstrap.Modal.getInstance(document.getElementById('editModal'))?.hide();
    await refreshData();
}

async function exportParts() {
    const tableName = document.getElementById('tableComboBox')?.value;
    if (!tableName || !window.currentDbSettings) return alert('Сначала подключи базу данных.');

    const response = await apiRequestRaw('/Export/PartsCsv', {
        settings: window.currentDbSettings,
        inventoryTableName: tableName,
        suppliersTableName: 'suppliers',
        categoriesTableName: 'categories'
    });

    if (!response.ok) {
        alert(await response.text());
        return;
    }

    const blob = await response.blob();
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = 'parts_export.csv';
    document.body.appendChild(a);
    a.click();
    a.remove();
    URL.revokeObjectURL(url);
}

async function connectAndLoadTables() {
    const settings = getSettingsFromForm();
    const result = await apiRequest('/Database/GetTables', settings);
    if (!result.success) {
        alert('Ошибка: ' + result.message);
        return;
    }

    window.currentDbSettings = settings;
    localStorage.setItem('autoPartsDbSettings', JSON.stringify(settings));

    const combo = document.getElementById('tableComboBox');
    combo.innerHTML = '<option value="">Выберите таблицу</option>';
    result.tables.forEach(name => combo.insertAdjacentHTML('beforeend', `<option value="${name}">${name}</option>`));
    combo.value = result.tables.includes('parts') ? 'parts' : (result.tables[0] || '');

    bootstrap.Modal.getInstance(document.getElementById('connectModal'))?.hide();

    if (combo.value) {
        await refreshData();
    }
}

document.addEventListener('DOMContentLoaded', () => {
    AppValidation.applyStandardAttributes();
    if (!hasPartsPage()) return;

    document.getElementById('btnDoConnect')?.addEventListener('click', connectAndLoadTables);
    document.getElementById('tableComboBox')?.addEventListener('change', refreshData);
    ['tableSearchInput', 'brandFilter', 'modelFilter', 'categoryFilter', 'supplierFilter', 'stockFilter', 'sortFilter', 'minPriceFilter', 'maxPriceFilter']
        .forEach(id => document.getElementById(id)?.addEventListener(id.includes('Price') ? 'input' : 'change', applyFilters));
    document.getElementById('tableSearchInput')?.addEventListener('input', applyFilters);
    document.getElementById('resetFiltersBtn')?.addEventListener('click', resetFilters);
    document.getElementById('exportPartsBtn')?.addEventListener('click', exportParts);

    const saved = localStorage.getItem('autoPartsDbSettings');
    if (saved) {
        try {
            const settings = JSON.parse(saved);
            fillSettingsForm(settings);
            window.currentDbSettings = settings;
            connectAndLoadTables();
        } catch {}
    }
});

const themeSwitcher = document.getElementById('themeSwitcher');
const body = document.documentElement; // используем html тег для data-theme

// Проверяем сохраненную тему
const savedTheme = localStorage.getItem('theme') || 'light';
body.setAttribute('data-theme', savedTheme);
updateIcon(savedTheme);

themeSwitcher.addEventListener('click', () => {
    const currentTheme = body.getAttribute('data-theme');
    const newTheme = currentTheme === 'light' ? 'dark' : 'light';

    body.setAttribute('data-theme', newTheme);
    localStorage.setItem('theme', newTheme);
    updateIcon(newTheme);
});

function updateIcon(theme) {
    const icon = themeSwitcher.querySelector('i');
    if (theme === 'dark') {
        icon.className = 'bi bi-sun-fill';
        themeSwitcher.style.color = '#ffc107'; // золотистый для солнца
    } else {
        icon.className = 'bi bi-moon-stars-fill';
        themeSwitcher.style.color = '#000';
    }
}


