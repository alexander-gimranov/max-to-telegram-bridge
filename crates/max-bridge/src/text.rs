#[cfg(not(any(language = "en", language = "ru")))]
compile_error!("Set LANGUAGE=en or LANGUAGE=ru when you build.");

#[cfg(all(language = "en", language = "ru"))]
compile_error!("Set LANGUAGE to either en or ru, not both.");

#[cfg(language = "en")]
mod lang {
    pub const HTML_LANG: &str = "en";
    pub const YOU: &str = "you";
    pub const FAVORITES: &str = "Favorites";
    pub const NO_TEXT: &str = "(no text)";
    pub const DELETED: &str = "(deleted)";
    pub const NOT_FORWARDED: &str = "(attachment is not forwarded)";
    pub const FORWARDED_BY: &str = "Forwarded by ";

    pub fn chat_created(title: &str) -> String {
        format!("You created the chat \"{title}\"")
    }
    pub const SETUP: &str = "Set up";
    pub const LOAD_HISTORY: &str = "Load history";
    pub const RELOAD_HISTORY: &str = "Reload history";
    pub const SIGN_OUT: &str = "Sign out";
    pub const YES: &str = "Yes";
    pub const NO: &str = "No";
    pub const PANEL_OFF: &str = "MAX is not signed in. Open the page on this computer.";
    pub const PANEL_CONNECTING: &str = "Connecting to MAX.";
    pub const PANEL_READY: &str =
        "\"Load history\" appends chats. \"Reload history\" deletes topics and loads them again.";
    pub const PANEL_UPDATED: &str = "Bridge panel updated.";
    pub const ERROR_BUDGET: &str =
        "The bridge recorded more than 100 errors. Details are in errors.log.";
    pub const SESSION_REFRESH_FAILED: &str =
        "The MAX session could not be refreshed. Sign in again.";
    pub const LOGOUT_ASK: &str = "Delete the loaded messages?";
    pub const HISTORY_NEEDS_THREAD: &str =
        "History for every contact is written into topics. Set MODE to Thread.";
    pub const HISTORY_LOADING: &str = "Loading history for every contact.";
    pub const HISTORY_DONE: &str = "History loaded.";
    pub const HISTORY_LOADING_TOPIC: &str = "Loading history for this chat.";
    pub const HISTORY_DONE_TOPIC: &str = "History for this chat is loaded.";
    pub const HISTORY_REREAD_TOPIC: &str = "Reloading history for this chat.";
    pub const HISTORY_REREAD_DONE_TOPIC: &str = "History for this chat is reloaded.";
    pub const AUTH_TITLE: &str = "Sign in to MAX";
    pub const PHONE_LABEL: &str = "Phone number";
    pub const NEXT: &str = "Next";
    pub const CODE_LABEL: &str = "Code";
    pub const PASSWORD_LABEL: &str = "Cloud password";
    pub const SIGN_IN: &str = "Sign in";
    pub const PAGE_ERROR: &str = "The sign-in page did not respond.";
    pub const ENTER_PHONE: &str = "Enter a phone number";
    pub const ENTER_CODE: &str = "Enter the code";
    pub const PHONE_FIRST: &str = "Enter a phone number first";
    pub const UNREGISTERED: &str = "This number is not registered";
    pub const ENTER_PASSWORD: &str = "Enter the cloud password";
    pub const CODE_FIRST: &str = "Enter the code first";
    pub const SIGNED_IN: &str = "Signed in.";
    pub const ADD_CONTACT: &str = "Add contact";
    pub const CONTACTS_TITLE: &str = "Add a MAX contact";
    pub const NAME_LABEL: &str = "Name";
    pub const ADD: &str = "Add";
    pub const ENTER_NAME: &str = "Enter a name";
    pub const CONTACT_ADDED: &str = "Contact added.";
    pub const CONTACT_EXISTS: &str = "This contact is already saved.";
    pub const CONTACT_MISSING: &str = "This number is not in MAX.";
    pub const CONTACT_PRIVACY: &str = "This person accepts messages only from their contacts.";
    pub const CONTACTS_NEED_SESSION: &str = "MAX is not signed in.";
    pub const CONTACTS_PAGE_ERROR: &str = "The contacts page did not respond.";
    pub const CONTACT_CARD: &str = "Contact";
    pub const FILE_CARD: &str = "File";
    pub const STICKER_CARD: &str = "Sticker";
    pub const LINK_CARD: &str = "Link";
    pub const CALL_CARD: &str = "Call";
    pub const CALL_AUDIO: &str = "Audio call";
    pub const CALL_VIDEO: &str = "Video call";
    pub const CALL_MISSED: &str = "missed";
    pub const CALL_REJECTED: &str = "rejected";
    pub const CALL_CANCELED: &str = "canceled";
    pub const CALL_HUNGUP: &str = "ended";
    pub const POLL_CARD: &str = "Poll";
    pub const BUTTONS_CARD: &str = "Buttons";
    pub const SYSTEM_EVENT: &str = "System message";

    pub fn code_call(digits: i64, wait_secs: i64) -> String {
        format!(
            "MAX is calling. The code has {digits} digits. Wait up to {wait_secs} seconds for the call."
        )
    }

    pub fn password_hint(hint: &str) -> String {
        format!("Hint: {hint}.")
    }
}

#[cfg(language = "ru")]
mod lang {
    pub const HTML_LANG: &str = "ru";
    pub const YOU: &str = "ты";
    pub const FAVORITES: &str = "Избранное";
    pub const NO_TEXT: &str = "(нет текста)";
    pub const DELETED: &str = "(удалено)";
    pub const NOT_FORWARDED: &str = "(вложение не пересылается)";
    pub const FORWARDED_BY: &str = "Переслано от ";

    pub fn chat_created(title: &str) -> String {
        format!("Вы создали чат «{title}»")
    }
    pub const SETUP: &str = "Настроить";
    pub const LOAD_HISTORY: &str = "Загрузить историю";
    pub const RELOAD_HISTORY: &str = "Перечитать историю";
    pub const SIGN_OUT: &str = "Выйти";
    pub const YES: &str = "Да";
    pub const NO: &str = "Нет";
    pub const PANEL_OFF: &str = "Сессия MAX не настроена. Откройте страницу на этом компьютере.";
    pub const PANEL_CONNECTING: &str = "Подключаю MAX.";
    pub const PANEL_READY: &str = "«Загрузить историю» дописывает чаты. «Перечитать историю» удаляет темы и загружает их заново.";
    pub const PANEL_UPDATED: &str = "Панель моста обновлена.";
    pub const ERROR_BUDGET: &str = "Ошибок моста больше 100. Журнал: errors.log.";
    pub const SESSION_REFRESH_FAILED: &str =
        "Не получилось обновить сессию MAX. Выполните вход повторно.";
    pub const LOGOUT_ASK: &str = "Удалить загруженные сообщения?";
    pub const HISTORY_NEEDS_THREAD: &str =
        "История всех контактов пишется в темы. Включите режим Thread.";
    pub const HISTORY_LOADING: &str = "Загружаю историю всех контактов.";
    pub const HISTORY_DONE: &str = "История загружена.";
    pub const HISTORY_LOADING_TOPIC: &str = "Загружаю историю этого чата.";
    pub const HISTORY_DONE_TOPIC: &str = "История этого чата загружена.";
    pub const HISTORY_REREAD_TOPIC: &str = "Перечитываю историю этого чата.";
    pub const HISTORY_REREAD_DONE_TOPIC: &str = "История этого чата перечитана.";
    pub const AUTH_TITLE: &str = "Вход в MAX";
    pub const PHONE_LABEL: &str = "Номер телефона";
    pub const NEXT: &str = "Дальше";
    pub const CODE_LABEL: &str = "Код";
    pub const PASSWORD_LABEL: &str = "Облачный пароль";
    pub const SIGN_IN: &str = "Войти";
    pub const PAGE_ERROR: &str = "Страница входа не ответила.";
    pub const ENTER_PHONE: &str = "введите номер";
    pub const ENTER_CODE: &str = "введите код";
    pub const PHONE_FIRST: &str = "сначала введите номер";
    pub const UNREGISTERED: &str = "этот номер не зарегистрирован";
    pub const ENTER_PASSWORD: &str = "введите облачный пароль";
    pub const CODE_FIRST: &str = "сначала введите код";
    pub const SIGNED_IN: &str = "Вход выполнен.";
    pub const ADD_CONTACT: &str = "Добавить контакт";
    pub const CONTACTS_TITLE: &str = "Контакт MAX";
    pub const NAME_LABEL: &str = "Имя";
    pub const ADD: &str = "Добавить";
    pub const ENTER_NAME: &str = "введите имя";
    pub const CONTACT_ADDED: &str = "Контакт добавлен.";
    pub const CONTACT_EXISTS: &str = "Контакт уже в книге.";
    pub const CONTACT_MISSING: &str = "Этого номера нет в MAX.";
    pub const CONTACT_PRIVACY: &str = "Этот человек принимает сообщения только от своих контактов.";
    pub const CONTACTS_NEED_SESSION: &str = "Вход в MAX не выполнен.";
    pub const CONTACTS_PAGE_ERROR: &str = "Страница контактов не ответила.";
    pub const CONTACT_CARD: &str = "Контакт";
    pub const FILE_CARD: &str = "Файл";
    pub const STICKER_CARD: &str = "Стикер";
    pub const LINK_CARD: &str = "Ссылка";
    pub const CALL_CARD: &str = "Звонок";
    pub const CALL_AUDIO: &str = "Аудиозвонок";
    pub const CALL_VIDEO: &str = "Видеозвонок";
    pub const CALL_MISSED: &str = "пропущен";
    pub const CALL_REJECTED: &str = "отклонён";
    pub const CALL_CANCELED: &str = "отменён";
    pub const CALL_HUNGUP: &str = "завершён";
    pub const POLL_CARD: &str = "Опрос";
    pub const BUTTONS_CARD: &str = "Кнопки";
    pub const SYSTEM_EVENT: &str = "Системное сообщение";

    pub fn code_call(digits: i64, wait_secs: i64) -> String {
        format!("MAX звонит. Код из {digits} цифр. Ждите звонок до {wait_secs} с.")
    }

    pub fn password_hint(hint: &str) -> String {
        format!("Подсказка: {hint}.")
    }
}

pub use lang::*;
