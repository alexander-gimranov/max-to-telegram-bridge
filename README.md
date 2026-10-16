[Русский](README.ru.md)

# MAX to Telegram bridge

The bridge copies messages between a MAX account and a Telegram bot. It runs on your computer. The sign-in page stays on that computer.

## Requirements

- Rust and Cargo, on the stable channel
- A Telegram bot token
- The Telegram user id of each person who can use the bot

## Build

Bot labels and the sign-in page use one language, chosen when you build. Set `LANGUAGE` to `en` or `ru`. The build stops when `LANGUAGE` is missing or has another value.

PowerShell:

```powershell
$env:LANGUAGE = "en"
cargo build -p max-bridge --release
```

bash:

```bash
LANGUAGE=ru cargo build -p max-bridge --release
```

The program file is `target/release/max-bridge`. On Windows it is `target\release\max-bridge.exe`.

`LANGUAGE=en` selects English labels. `LANGUAGE=ru` selects Russian labels. This covers bot buttons, command descriptions, status text, and the sign-in page. Log lines stay in English.

Set the same value before `cargo test`. After you change `LANGUAGE`, Cargo rebuilds the bridge.

## Configuration

Before you start the program, create `var/bridge.env`. Git ignores `var/`. Do not publish that directory. It holds the MAX session.

```
TELEGRAM_BOT_TOKEN=123456:replace-me
TELEGRAM_ALLOWED_USER_IDS=123456789
MODE=Thread
```

The bridge reads these variables from the environment or from `var/bridge.env`. When both are set, the environment value is used.

| Variable | Required | Description |
| --- | --- | --- |
| `TELEGRAM_BOT_TOKEN` | Yes | Token from BotFather. |
| `TELEGRAM_ALLOWED_USER_IDS` | Yes | Comma-separated Telegram user ids that can use the bot. |
| `MODE` | No | `Chat` or `Thread`. The default is `Chat`. |

Optional path variables: `MAX_VAR`, `MAX_ENV_PATH`, `MAX_SESSION_PATH`, `MAX_MAP_PATH`, `MAX_LINKS_PATH`, `MAX_TOPICS_PATH`, `MAX_OFFSET_PATH`, `MAX_SEEN_PATH`.

## Sign in

1. Start the program.
2. Open the bot in Telegram.
3. When MAX is not signed in, select **Set up**, or send `/setup`.
4. On this computer, open `http://127.0.0.1:8787`.
5. Enter the phone number, then the code from the MAX call.
6. When MAX asks for a cloud password, enter it.

The sign-in page accepts connections only from this computer. It listens on port `8787`. The labels in this section match `LANGUAGE=en`.

PowerShell:

```powershell
$env:LANGUAGE = "en"
cargo run -p max-bridge --release
```

bash:

```bash
LANGUAGE=ru cargo run -p max-bridge --release
```

`cargo run` passes `LANGUAGE` to the compiler. A binary you already built keeps the language from that build.

## Modes

`Chat` copies every MAX chat into one Telegram chat. The contact name is placed above the message.

`Thread` creates a Telegram topic for each MAX chat. Replies stay in that topic. History commands work only in this mode.

In `Thread` mode the bot shows these controls after MAX is connected:

- **Load history** (`/history`) writes chats that are not loaded yet.
- **Reload history** (`/reread`) deletes the topics and loads them again.
- **Sign out** (`/logout`) drops the MAX session, then asks whether to delete the loaded messages.

The MAX favorites chat is copied under the label **Favorites** or **Избранное**, matching `LANGUAGE`. Live messages that you send from another MAX client are copied only in that chat. A history load includes your own messages and starts them with **you** or **ты**.

When both services provide the file, photos, voice messages, and videos are copied in both directions.

## Project layout

- `crates/max-client` is the MAX client.
- `crates/max-bridge` is the Telegram bot and the sign-in page.

## License

Non-commercial use is allowed. You can change the source code only in this repository, by opening a pull request. The English text is in [LICENSE](LICENSE). The Russian text is in [LICENSE.ru.md](LICENSE.ru.md).
