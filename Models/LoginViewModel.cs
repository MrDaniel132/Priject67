using System.ComponentModel.DataAnnotations;

namespace MyFinanceApp.Models;

public class LoginViewModel
{
    [Required(ErrorMessage = "Введите логин")]
    [StringLength(50, ErrorMessage = "Логин не должен быть длиннее 50 символов")]
    public string Username { get; set; } = "";

    [Required(ErrorMessage = "Введите пароль")]
    [DataType(DataType.Password)]
    [StringLength(100, ErrorMessage = "Пароль не должен быть длиннее 100 символов")]
    public string Password { get; set; } = "";
}