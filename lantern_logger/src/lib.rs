#[derive(PartialEq, Clone)]
pub enum LogLevel {
    Error,
    Info,
    Warn,
    Debug,
}

pub struct Logger {
    pub label: String,
    pub level: LogLevel,
}

impl Logger {
    pub fn new(label: &str, level: LogLevel) -> Logger {
        Logger {
            label: label.to_owned(),
            level,
        }
    }

    pub fn print_raw(&self, msg: &str) {
        println!("{}", msg);
    }

    pub fn info(&self, msg: &str) {
        if self.level == LogLevel::Error {
            return;
        }

        println!("[*] [{}] {}", &self.label, msg);
    }

    pub fn debug(&self, msg: &str) {
        if self.level != LogLevel::Debug {
            return;
        }

        println!("[+] [{}] {}", &self.label, msg);
    }

    pub fn warn(&self, msg: &str) {
        if self.level != LogLevel::Warn && self.level != LogLevel::Debug {
            return;
        }

        println!("[!] [{}] {}", &self.label, msg);
    }

    pub fn error(&self, msg: &str) {
        eprintln!("[X] [{}] {}", &self.label, msg);
    }
}

impl Clone for Logger {
    fn clone(&self) -> Self {
        Logger::new(&self.label, self.level.clone())
    }
}
