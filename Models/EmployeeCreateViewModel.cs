using System.ComponentModel.DataAnnotations;

namespace MyFinanceApp.Models;

public class EmployeeCreateViewModel
{
    [Required(ErrorMessage = "Введите ФИО")]
    public string FullName { get; set; } = "";

    [Required(ErrorMessage = "Введите логин")]
    public string Username { get; set; } = "";

    [Required(ErrorMessage = "Введите пароль")]
    [DataType(DataType.Password)]
    public string Password { get; set; } = "";

    [Required(ErrorMessage = "Выберите роль")]
    public string Role { get; set; } = "";
}