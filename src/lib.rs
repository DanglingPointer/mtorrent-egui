mod listener;
mod logging;

use crate::listener::{Canceller, listener_with_canceller};
use crate::logging::{Config, setup_log_rotation};
use eframe::egui;
use mtorrent::utils::re_exports::mtorrent_dht as dht;
use mtorrent::utils::re_exports::mtorrent_utils::{peer_id::PeerId, worker};
use mtorrent::{app, utils};
use parking_lot::Mutex;
use std::path::PathBuf;
use std::sync::Arc;
use std::{env, io};

const UPNP_ENABLED: bool = true;

#[derive(Clone, Debug)]
struct PeerInfo {
    addr: String,
    client: String,
    origin: String,
    downloaded: u64,
    uploaded: u64,
}

#[derive(Clone, Debug)]
struct DownloadProgress {
    total_bytes: u64,
    downloaded_bytes: u64,
    peers: Vec<PeerInfo>,
    status: String,
}

impl Default for DownloadProgress {
    fn default() -> Self {
        Self {
            total_bytes: 0,
            downloaded_bytes: 0,
            peers: Vec::new(),
            status: "Idle".to_string(),
        }
    }
}

struct DownloadTask {
    id: usize,
    uri: String,
    name: String,
    output_dir: PathBuf,
    canceller: Option<Canceller>,
    progress: Arc<Mutex<DownloadProgress>>,
    new_uri_input: String,
}

struct MtorrentApp {
    peer_id: PeerId,
    local_data_dir: PathBuf,
    active_downloads: Vec<DownloadTask>,
    next_id: usize,
    main_runtime_handle: tokio::runtime::Handle,
    pwp_runtime_handle: tokio::runtime::Handle,
    storage_runtime_handle: tokio::runtime::Handle,
    dht_cmd_sender: dht::CommandSink,
    cli_arg: Option<String>,
    net_if: Option<String>,
}

impl MtorrentApp {
    fn new(
        local_data_dir: PathBuf,
        main_runtime_handle: tokio::runtime::Handle,
        pwp_runtime_handle: tokio::runtime::Handle,
        storage_runtime_handle: tokio::runtime::Handle,
        dht_cmd_sender: dht::CommandSink,
        cli_arg: Option<String>,
        net_if: Option<String>,
    ) -> Self {
        let mut app = Self {
            peer_id: PeerId::generate_new(),
            local_data_dir,
            active_downloads: Vec::new(),
            next_id: 1,
            main_runtime_handle,
            pwp_runtime_handle,
            storage_runtime_handle,
            dht_cmd_sender,
            cli_arg,
            net_if,
        };

        // Handle CLI arg if provided
        if let Some(arg) = app.cli_arg.take() {
            app.add_new_task();
            if let Some(task) = app.active_downloads.first_mut() {
                task.new_uri_input = arg;
            }
        }

        app
    }

    fn add_new_task(&mut self) {
        let id = self.next_id;
        self.next_id += 1;

        let task = DownloadTask {
            id,
            uri: String::new(),
            name: format!("Task {}", id),
            output_dir: PathBuf::new(),
            canceller: None,
            progress: Arc::new(Mutex::new(DownloadProgress::default())),
            new_uri_input: String::new(),
        };

        self.active_downloads.push(task);
    }

    fn remove_task(&mut self, index: usize) {
        if index < self.active_downloads.len() {
            // Stop download if active
            self.stop_download(index);
            
            self.active_downloads.remove(index);
        }
    }

    fn stop_download(&mut self, index: usize) {
        if let Some(task) = self.active_downloads.get_mut(index) {
            task.canceller = None;
            task.progress.lock().status = "Stopped".to_string();
        }
    }

    fn start_download(&mut self, index: usize, uri: String, output_dir: PathBuf) {
        if index >= self.active_downloads.len() {
            return;
        }

        let task = &mut self.active_downloads[index];
        task.uri = uri.clone();
        task.output_dir = output_dir.clone();
        task.progress.lock().status = "Loading...".to_string();

        // Try to get torrent name
        let name = utils::startup::get_torrent_name(&uri).unwrap_or_else(|| format!("Download {}", task.id));
        task.name = name.clone();

        let progress = Arc::clone(&task.progress);
        let metainfo_uri = uri.clone();
        let local_data_dir = self.local_data_dir.clone();
        let peer_id = self.peer_id;
        let pwp_runtime_handle = self.pwp_runtime_handle.clone();
        let storage_runtime_handle = self.storage_runtime_handle.clone();
        let dht_cmd_sender = self.dht_cmd_sender.clone();

        // Store canceller first
        let (listener, canceller) = listener_with_canceller(
            move |json_value| {
                if let Ok(snapshot) = serde_json::from_value::<serde_json::Value>(json_value.clone()) {
                    let mut prog = progress.lock();
                    
                    // Parse snapshot
                    if let Some(bytes) = snapshot.get("bytes") {
                        prog.total_bytes = bytes.get("total").and_then(|v| v.as_u64()).unwrap_or(0);
                        prog.downloaded_bytes = bytes.get("downloaded").and_then(|v| v.as_u64()).unwrap_or(0);
                    }
                    
                    if let Some(peers) = snapshot.get("peers").and_then(|v| v.as_object()) {
                        prog.peers.clear();
                        for (addr, peer_data) in peers {
                            if let Some(peer_obj) = peer_data.as_object() {
                                let peer_info = PeerInfo {
                                    addr: addr.clone(),
                                    client: peer_obj.get("client").and_then(|v| v.as_str()).unwrap_or("n/a").to_string(),
                                    origin: peer_obj.get("origin").and_then(|v| v.as_str()).unwrap_or("").to_string(),
                                    downloaded: peer_obj.get("download")
                                        .and_then(|v| v.get("bytesReceived"))
                                        .and_then(|v| v.as_u64())
                                        .unwrap_or(0),
                                    uploaded: peer_obj.get("upload")
                                        .and_then(|v| v.get("bytesSent"))
                                        .and_then(|v| v.as_u64())
                                        .unwrap_or(0),
                                };
                                prog.peers.push(peer_info);
                            }
                        }
                    }
                    
                    prog.status = "Downloading".to_string();
                }
            },
            log::Level::Debug,
        );

        task.canceller = Some(canceller);

        // Spawn download task on the main runtime's thread
        let net_if = self.net_if.clone();
        self.main_runtime_handle.spawn(async move {
            tokio::task::spawn_local(async move {
                let result = app::main::single_torrent(
                    metainfo_uri.clone(),
                    listener,
                    app::main::Config {
                        local_peer_id: peer_id,
                        output_dir,
                        config_dir: local_data_dir,
                        use_upnp: UPNP_ENABLED,
                        pwp_port: None,
                        bind_interface: net_if,
                    },
                    app::main::Context {
                        dht_handle: Some(dht_cmd_sender),
                        pwp_runtime: pwp_runtime_handle,
                        storage_runtime: storage_runtime_handle,
                    },
                )
                .await;

                match result {
                    Ok(()) => log::info!("Download completed: {}", metainfo_uri),
                    Err(e) => log::error!("Download failed: {} - {}", metainfo_uri, e),
                }
            });
        });
    }

    fn format_bytes(bytes: u64) -> String {
        if bytes == 0 {
            return "0 B".to_string();
        }
        const UNITS: &[&str] = &["B", "KB", "MB", "GB", "TB"];
        let k = 1024f64;
        let i = (bytes as f64).log(k) as usize;
        let i = i.min(UNITS.len() - 1);
        let value = bytes as f64 / k.powi(i as i32);
        if value < 10.0 {
            format!("{:.1} {}", value, UNITS[i])
        } else {
            format!("{:.0} {}", value, UNITS[i])
        }
    }

    fn format_bytes_kib(bytes: u64) -> String {
        let kib = bytes / 1024;
        format!("{} KiB", kib.to_string().as_bytes().rchunks(3).rev().map(|chunk| std::str::from_utf8(chunk).unwrap()).collect::<Vec<_>>().join(" "))
    }
}

impl eframe::App for MtorrentApp {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        egui::CentralPanel::default().show(ctx, |ui| {
            // Header
            ui.horizontal(|ui| {
                ui.heading("mtorrent");
                ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                    ui.hyperlink_to("crates.io", "https://crates.io/crates/mtorrent");
                });
            });
            
            ui.separator();

            // Add new task button
            if ui.button("➕ Add New Download Task").clicked() {
                self.add_new_task();
            }

            ui.separator();

            // Scroll area for task list
            egui::ScrollArea::vertical().show(ui, |ui| {
                let mut task_to_remove: Option<usize> = None;
                let mut tasks_to_start: Vec<(usize, String, PathBuf)> = Vec::new();
                let mut tasks_to_stop: Vec<usize> = Vec::new();

                for (idx, task) in self.active_downloads.iter_mut().enumerate() {
                    let progress = task.progress.lock();
                    let is_downloading = task.canceller.is_some();
                    let task_name = task.name.clone();
                    drop(progress);

                    ui.group(|ui| {
                        ui.horizontal(|ui| {
                            ui.heading(&task_name);
                            ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                                if ui.button("🗑 Remove").clicked() {
                                    task_to_remove = Some(idx);
                                }
                            });
                        });
                        
                        ui.horizontal(|ui| {
                            ui.label("Magnet link or file path:");
                            ui.text_edit_singleline(&mut task.new_uri_input);
                            
                            if ui.button("Select...").clicked() && !is_downloading {
                                if let Some(path) = rfd::FileDialog::new()
                                    .add_filter("Torrent Files", &["torrent"])
                                    .pick_file()
                                {
                                    task.new_uri_input = path.to_string_lossy().to_string();
                                }
                            }
                        });

                        if !is_downloading {
                            if ui.button("▶ Start Download").clicked() && !task.new_uri_input.is_empty() {
                                if let Some(output_dir) = rfd::FileDialog::new().pick_folder() {
                                    tasks_to_start.push((idx, task.new_uri_input.clone(), output_dir));
                                }
                            }
                        } else {
                            if ui.button("⏹ Stop Download").clicked() {
                                tasks_to_stop.push(idx);
                            }
                        }

                        // Progress
                        let progress = task.progress.lock();
                        let pct = if progress.total_bytes > 0 {
                            (progress.downloaded_bytes as f64 / progress.total_bytes as f64 * 100.0).min(100.0)
                        } else {
                            0.0
                        };

                        ui.add_space(10.0);
                        
                        // Progress bar
                        let progress_bar = egui::ProgressBar::new(pct as f32 / 100.0)
                            .text(format!("{:.1}%", pct));
                        ui.add(progress_bar);

                        // Status
                        ui.label(format!(
                            "Status: {} | Downloaded: {} / {} ({})",
                            progress.status,
                            Self::format_bytes(progress.downloaded_bytes),
                            Self::format_bytes(progress.total_bytes),
                            Self::format_bytes_kib(progress.downloaded_bytes)
                        ));

                        // Peers table
                        if !progress.peers.is_empty() {
                            ui.add_space(10.0);
                            ui.label("Connected Peers:");
                            
                            egui::ScrollArea::vertical().max_height(200.0).show(ui, |ui| {
                                egui::Grid::new(format!("peers_table_{}", idx))
                                    .num_columns(5)
                                    .spacing([10.0, 4.0])
                                    .striped(true)
                                    .show(ui, |ui| {
                                        // Header
                                        ui.label("Address");
                                        ui.label("Client");
                                        ui.label("Origin");
                                        ui.label("Downloaded");
                                        ui.label("Uploaded");
                                        ui.end_row();

                                        // Rows
                                        for peer in &progress.peers {
                                            ui.label(&peer.addr);
                                            ui.label(&peer.client);
                                            ui.label(&peer.origin);
                                            ui.label(Self::format_bytes(peer.downloaded));
                                            ui.label(Self::format_bytes(peer.uploaded));
                                            ui.end_row();
                                        }
                                    });
                            });
                        }
                    });

                    ui.add_space(10.0);
                }

                // Handle actions after iteration
                if let Some(idx) = task_to_remove {
                    self.remove_task(idx);
                }
                for (idx, uri, output_dir) in tasks_to_start {
                    self.start_download(idx, uri, output_dir);
                }
                for idx in tasks_to_stop {
                    self.stop_download(idx);
                }
            });
        });
    }

    fn on_exit(&mut self, _ctx: Option<&eframe::glow::Context>) {
        // Stop all downloads
        for idx in 0..self.active_downloads.len() {
            self.stop_download(idx);
        }
        
        // Shutdown DHT
        let _ = self.dht_cmd_sender.try_send(dht::Command::Shutdown);
    }
}

fn setup_logging(local_data_dir: &PathBuf) -> io::Result<()> {
    let (log_sink, mut log_writer) = setup_log_rotation(Config {
        file_path: local_data_dir.join("mtorrent.log"),
        max_files: 3,
        max_file_size: 10 * 1024 * 1024, // 10 MiB
        buffer_capacity: 32 * 1024,      // 32 KiB
    });

    std::thread::Builder::new()
        .name("logger".to_owned())
        .stack_size(128 * 1024)
        .spawn(move || {
            log_writer.write_logs().inspect_err(|e| eprintln!("Failed to write logs: {e}"))
        })?;

    env_logger::Builder::from_default_env()
        .filter(None, log::LevelFilter::Debug)
        .target(env_logger::Target::Pipe(Box::new(log_sink)))
        .init();

    Ok(())
}

pub fn run() -> io::Result<()> {
    // Get local data directory
    let local_data_dir = match dirs_next::data_local_dir()
        .or_else(dirs_next::data_dir)
        .or_else(dirs_next::config_dir)
    {
        Some(dir) => dir,
        None => env::current_dir()?,
    };
    let local_data_dir = local_data_dir.join(env!("CARGO_PKG_NAME"));
    
    // Create directory if it doesn't exist
    std::fs::create_dir_all(&local_data_dir)?;
    
    println!("Log directory: {}", local_data_dir.display());

    // Setup logging
    setup_logging(&local_data_dir)?;

    // Get CLI arg
    let cli_arg = env::args().nth(1);

    // Create workers
    let main_worker = worker::with_local_runtime(worker::rt::Config {
        name: "app".to_owned(),
        io_enabled: true,
        time_enabled: true,
        ..Default::default()
    })?;

    let storage_worker = worker::with_runtime(worker::rt::Config {
        name: "storage".to_owned(),
        io_enabled: false,
        time_enabled: false,
        ..Default::default()
    })?;

    let pwp_worker = worker::with_local_runtime(worker::rt::Config {
        name: "pwp".to_owned(),
        io_enabled: true,
        time_enabled: true,
        ..Default::default()
    })?;

    let net_if = env::var("MTORRENT_NET_IF").ok();

    let (_dht_worker, dht_cmds) = app::dht::launch_dht_node_runtime(app::dht::Config {
        local_port: 6881,
        max_concurrent_queries: None,
        config_dir: local_data_dir.clone(),
        use_upnp: UPNP_ENABLED,
        bootstrap_nodes_override: None,
        bind_interface: net_if.clone(),
        query_timeout: None,
    })?;

    // Create app
    let app = MtorrentApp::new(
        local_data_dir,
        main_worker.runtime_handle().clone(),
        pwp_worker.runtime_handle().clone(),
        storage_worker.runtime_handle().clone(),
        dht_cmds,
        cli_arg,
        net_if,
    );

    // Run egui app
    let icon_data = include_bytes!("../icon.png");
    let icon_image = image::load_from_memory(icon_data)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?
        .to_rgba8();
    let (icon_width, icon_height) = icon_image.dimensions();
    
    let options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default()
            .with_inner_size([900.0, 700.0])
            .with_min_inner_size([600.0, 400.0])
            .with_icon(egui::IconData {
                rgba: icon_image.into_raw(),
                width: icon_width,
                height: icon_height,
            }),
        ..Default::default()
    };

    eframe::run_native(
        "mtorrent",
        options,
        Box::new(|_cc| Ok(Box::new(app))),
    )
    .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;

    Ok(())
}
