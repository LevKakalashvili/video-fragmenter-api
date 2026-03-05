def _env_name_from_error_loc(loc: tuple[str | int, ...]) -> str:
    if not loc:
        return "UNKNOWN_ENV_FIELD"

    env_prefixes = {
        "app": "APP",
        "kafka": "KAFKA",
        "minio": "MINIO",
    }

    root = str(loc[0])
    if root in env_prefixes:
        tail = "_".join(str(item) for item in loc[1:])
        return f"{env_prefixes[root]}_{tail}".upper() if tail else env_prefixes[root]

    return "_".join(str(item) for item in loc).upper()


def _translate_error_type(error_type: str) -> str:
    translations = {
        "missing": "Отсутствует",
        "validation_error": "Ошибка валидации",
        "string_type": "Некорректный тип строки",
        "int_type": "Некорректный тип целого числа",
        "bool_type": "Некорректный тип булевого значения",
    }
    return translations.get(error_type, f"Ошибка валидации ({error_type})")


def _translate_error_message(message: str) -> str:
    translations = {
        "Field required": "Поле обязательно",
    }
    return translations.get(message, message)
