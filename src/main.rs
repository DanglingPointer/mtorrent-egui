// Prevents additional console window on Windows in release, DO NOT REMOVE!!
#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

fn main() {
    if let Err(e) = mtorrent_egui::run() {
        eprintln!("Error: {}", e);
        std::process::exit(1);
    }
}
