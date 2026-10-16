fn main() {
    println!("cargo:rerun-if-env-changed=LANGUAGE");
    println!("cargo::rustc-check-cfg=cfg(language, values(\"en\", \"ru\"))");
    let language = std::env::var("LANGUAGE").unwrap_or_default();
    match language.as_str() {
        "en" | "ru" => println!("cargo:rustc-cfg=language=\"{language}\""),
        "" => panic!(
            "LANGUAGE is not set. Set LANGUAGE=en or LANGUAGE=ru.\n\
             PowerShell: $env:LANGUAGE = \"en\"; cargo build --release\n\
             bash: LANGUAGE=en cargo build --release"
        ),
        other => panic!("LANGUAGE must be en or ru, got {other}"),
    }
}
