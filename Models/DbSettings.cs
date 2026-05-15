using System.ComponentModel.DataAnnotations;

namespace MyFinanceApp.Models;


public class DbSettings : IValidatableObject
{
    [Required(ErrorMessage = "Укажи адрес сервера.")]
    [StringLength(100, ErrorMessage = "Сервер не должен быть длиннее 100 символов.")]
    public string Server { get; set; } = "localhost";

    [Required(ErrorMessage = "Укажи порт.")]
    [RegularExpression(@"^\d{1,5}$", ErrorMessage = "Порт должен содержать только цифры.")]
    public string Port { get; set; } = "3306";

    [Required(ErrorMessage = "Укажи имя базы данных.")]
    [StringLength(100, ErrorMessage = "Имя базы данных не должно быть длиннее 100 символов.")]
    public string Database { get; set; } = "myfinanceapp_db";

    [Required(ErrorMessage = "Укажи логин для подключения.")]
    [StringLength(100, ErrorMessage = "Логин не должен быть длиннее 100 символов.")]
    public string Username { get; set; } = "root";

    [StringLength(100, ErrorMessage = "Пароль не должен быть длиннее 100 символов.")]
    public string Password { get; set; } = "root";

    public IEnumerable<ValidationResult> Validate(ValidationContext validationContext)
    {
        if (!int.TryParse(Port, out var port) || port is < 1 or > 65535)
            yield return new ValidationResult("Порт должен быть в диапазоне от 1 до 65535.", [nameof(Port)]);
    }

    public string GetConnectionString() =>
        $"Server={Server};Port={Port};Database={Database};Uid={Username};Pwd={Password};SslMode=None;";
}