using System.Security.Claims;

namespace MyFinanceApp.Services;

public sealed record AppUser(string Username, string Password, string Role, string DisplayName);

public static class AppUserStore
{
    private static readonly List<AppUser> Users =
    [
        new("admin", "12345", "Администратор", "Администратор"),
        new("manager", "12345", "Менеджер", "Менеджер"),
        new("storekeeper", "12345", "Кладовщик", "Кладовщик"),
        new("cashier", "12345", "Кассир", "Кассир")
    ];

    public static bool DeleteUser(string username)
    {
        var user = Users.FirstOrDefault(u =>
            u.Username.Equals(username, StringComparison.OrdinalIgnoreCase));

        if (user == null)
            return false;

        if (user.Username.Equals("admin", StringComparison.OrdinalIgnoreCase))
            return false;

        Users.Remove(user);
        return true;
    }

    public static AppUser? Validate(string? username, string? password)
    {
        return Users.FirstOrDefault(u =>
            string.Equals(u.Username, username?.Trim(), StringComparison.OrdinalIgnoreCase) &&
            u.Password == password?.Trim());
    }

    public static IReadOnlyList<AppUser> GetAll() => Users;

    public static bool AddUser(AppUser user)
    {
        if (Users.Any(u => u.Username.Equals(user.Username, StringComparison.OrdinalIgnoreCase)))
            return false;

        Users.Add(user);
        return true;
    }
}

public static class ClaimsPrincipalExtensions
{
    public static string GetUserRole(this ClaimsPrincipal user) =>
        user.FindFirst(ClaimTypes.Role)?.Value ?? string.Empty;

    public static string GetDisplayName(this ClaimsPrincipal user) =>
        user.FindFirst("display_name")?.Value ?? user.Identity?.Name ?? "Неизвестный пользователь";
}