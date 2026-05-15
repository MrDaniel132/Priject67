
using System.Collections;
using System.ComponentModel.DataAnnotations;
using System.Reflection;

namespace MyFinanceApp.Services;

public static class RequestValidationHelper
{
    public static List<string> ValidateObjectGraph(object? model)
    {
        if (model is null)
            return ["Не переданы данные запроса."];

        var results = new List<ValidationResult>();
        var visited = new HashSet<object>(ReferenceEqualityComparer.Instance);
        ValidateRecursive(model, results, visited);

        return results
            .Select(r => r.ErrorMessage)
            .Where(message => !string.IsNullOrWhiteSpace(message))
            .Distinct(StringComparer.Ordinal)
            .Cast<string>()
            .ToList();
    }

    private static void ValidateRecursive(object model, ICollection<ValidationResult> results, ISet<object> visited)
    {
        if (!ShouldValidateType(model.GetType()) || !visited.Add(model))
            return;

        Validator.TryValidateObject(model, new ValidationContext(model), results, validateAllProperties: true);

        foreach (var property in model.GetType().GetProperties(BindingFlags.Instance | BindingFlags.Public))
        {
            if (!property.CanRead || property.GetIndexParameters().Length > 0)
                continue;

            var value = property.GetValue(model);
            if (value is null || value is string)
                continue;

            if (value is IEnumerable enumerable)
            {
                foreach (var item in enumerable)
                {
                    if (item is not null)
                        ValidateRecursive(item, results, visited);
                }
                continue;
            }

            if (ShouldValidateType(property.PropertyType))
                ValidateRecursive(value, results, visited);
        }
    }

    private static bool ShouldValidateType(Type type)
    {
        type = Nullable.GetUnderlyingType(type) ?? type;
        return type.IsClass
               && type != typeof(string)
               && type != typeof(DateTime)
               && type != typeof(DateTimeOffset)
               && type != typeof(TimeSpan)
               && type != typeof(Guid)
               && type != typeof(decimal)
               && !typeof(IEnumerable).IsAssignableFrom(type);
    }
}
