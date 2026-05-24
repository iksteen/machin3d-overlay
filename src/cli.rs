use std::{path::PathBuf, time::Duration};

use anyhow::{bail, Context, Result};
use chrono::{DateTime, Utc};
use clap::{Args, Parser, Subcommand};

use crate::{
    bambu::{
        auth::{default_token_path, load_token, save_token},
        cloud::CloudSession,
        local::BambuLocalEndpointConfig,
        BambuClient, LoginResponse, API_BASE, MQTT_HOST,
    },
    endpoint::{Endpoint, MqttEndpoint},
    monitor::{monitor_mqtt, MonitorConfig},
    secret::Secret,
    server::{serve, ServerConfig, DEFAULT_HOST, DEFAULT_PORT},
    snapmaker::{
        default_snap_token_path, fresh_clientid, load_snap_tokens, pair, upsert_snap_token,
        PairConfig, SnapmakerDeviceConfig,
    },
    video::VideoEndpoint,
};

const SNAP_BOOTSTRAP_PORT: u16 = 1884;

#[derive(Parser)]
#[command(name = "bambu-overlay", version, about = "3D printer OBS overlay")]
pub struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    #[command(about = "Log in and store a Bambu Cloud access token")]
    Login(LoginArgs),
    #[command(about = "List Bambu printers in the token account")]
    Devices(DevicesArgs),
    #[command(about = "Monitor Bambu MQTT events for one printer")]
    Mqtt(MqttArgs),
    #[command(about = "Serve an OBS browser overlay page")]
    Serve(ServeArgs),
    #[command(about = "Pair this overlay with a Snapmaker U1 (tap Approve on the printer screen)")]
    SnapPair(SnapPairArgs),
}

#[derive(Args, Clone)]
struct HttpArgs {
    #[arg(long, default_value = API_BASE)]
    api_base: String,
    #[arg(long, default_value_t = 30.0, value_parser = positive_f64)]
    timeout: f64,
}

#[derive(Args, Clone)]
struct TokenFileArgs {
    #[arg(
        long = "bbl-token-file",
        value_name = "PATH",
        default_value_os_t = default_token_path().to_path_buf(),
        help = "Bambu Cloud token JSON path"
    )]
    token_file: PathBuf,
}

#[derive(Args, Clone)]
struct ServeTokenFileArgs {
    #[arg(
        long = "bbl-token-file",
        value_name = "PATH",
        default_value_os_t = default_token_path().to_path_buf(),
        help = "Bambu Cloud token JSON path",
        help_heading = "Bambu Cloud"
    )]
    token_file: PathBuf,
}

#[derive(Args)]
struct LoginArgs {
    #[command(flatten)]
    http: HttpArgs,
    #[command(flatten)]
    token: TokenFileArgs,
    #[arg(long)]
    account: Option<String>,
    #[arg(long)]
    password: Option<String>,
    #[arg(long)]
    code: Option<String>,
}

#[derive(Args)]
struct DevicesArgs {
    #[command(flatten)]
    token: TokenFileArgs,
    #[arg(long, default_value_t = 30.0, value_parser = positive_f64)]
    timeout: f64,
}

#[derive(Args)]
struct ServeArgs {
    #[arg(
        long = "bind",
        value_name = "HOST[:PORT]",
        default_value = DEFAULT_HOST,
        value_parser = parse_bind_endpoint,
        help = "HTTP bind address. Port defaults to 8765",
        help_heading = "Server"
    )]
    bind: Endpoint,
    #[command(flatten)]
    token: ServeTokenFileArgs,
    #[arg(
        long,
        default_value_t = 30.0,
        value_parser = positive_f64,
        help = "Bambu Cloud API timeout in seconds",
        help_heading = "Bambu Cloud"
    )]
    timeout: f64,
    #[command(flatten)]
    devices: DeviceSelectionArgs,
    #[arg(
        long = "bbl-video-device",
        value_name = "HOST[:PORT][,ACCESS_CODE]",
        help = "Bambu LAN video endpoint; repeat for multiple printers. Port defaults to 6000. The device ID is inferred from the video certificate and must match a configured cloud or local device. ACCESS_CODE can be provided here or looked up from /bind when needed",
        help_heading = "Bambu LAN"
    )]
    video_devices: Vec<VideoEndpoint>,
    #[arg(
        long = "snap-device",
        value_name = "HOST[:PORT]",
        help = "Snapmaker printer reachable over Moonraker; repeat for multiple printers. Port defaults to 80. Startup probes machine/system_info to discover the serial number used as the device id",
        help_heading = "Snapmaker"
    )]
    snapmaker_devices: Vec<SnapmakerDeviceConfig>,
    #[arg(
        long = "snap-token-file",
        value_name = "PATH",
        default_value_os_t = default_snap_token_path().to_path_buf(),
        help = "Snapmaker pairing token JSON path. Loaded at startup to enable reliable camera wake-up over mTLS; HOST in --snap-device must match the host passed to `snap-pair`",
        help_heading = "Snapmaker"
    )]
    snap_token_file: PathBuf,
}

#[derive(Args)]
struct SnapPairArgs {
    #[arg(
        value_name = "HOST",
        help = "Snapmaker printer host or IP; must match the value passed to `serve --snap-device`"
    )]
    host: String,
    #[arg(
        long = "port",
        default_value_t = SNAP_BOOTSTRAP_PORT,
        help = "Cleartext MQTT bootstrap port. The U1 listens on 1884 by default"
    )]
    port: u16,
    #[arg(
        long = "snap-token-file",
        value_name = "PATH",
        default_value_os_t = default_snap_token_path().to_path_buf(),
        help = "Snapmaker pairing token JSON path"
    )]
    token_file: PathBuf,
    #[arg(
        long = "timeout",
        default_value_t = 180.0,
        value_parser = positive_f64,
        help = "Seconds to wait for the approval tap on the printer screen"
    )]
    timeout: f64,
    #[arg(
        long = "clientid",
        help = "Reuse a stable client identifier instead of generating a new one (allows re-pairing without re-tapping if the printer remembers us)"
    )]
    clientid: Option<String>,
}

#[derive(Args)]
struct MqttArgs {
    #[command(flatten)]
    token: ServeTokenFileArgs,
    #[arg(
        long,
        default_value_t = 30.0,
        value_parser = positive_f64,
        help = "Bambu Cloud API timeout in seconds",
        help_heading = "Bambu Cloud"
    )]
    timeout: f64,
    #[command(flatten)]
    devices: DeviceSelectionArgs,
    #[arg(
        long = "device",
        value_name = "DEVICE_ID",
        help = "Device ID to monitor. Defaults to the first resolved device",
        help_heading = "Selection"
    )]
    device: Option<String>,
}

#[derive(Args, Clone)]
struct DeviceSelectionArgs {
    #[arg(
        long = "bbl-cloud-mqtt",
        value_name = "HOST[:PORT]",
        default_value = MQTT_HOST,
        help = "Bambu Cloud MQTT endpoint. Port defaults to 8883",
        help_heading = "Bambu Cloud"
    )]
    cloud_mqtt: MqttEndpoint,
    #[arg(
        long = "bbl-cloud-device",
        value_name = "DEVICE_ID",
        value_parser = parse_cloud_device_id,
        help = "Explicit Bambu Cloud MQTT device ID; repeat to add devices. When set, /bind enumeration is skipped",
        help_heading = "Bambu Cloud"
    )]
    cloud_devices: Vec<String>,
    #[arg(
        long = "bbl-local-device",
        value_name = "HOST[:PORT][,ACCESS_CODE[,NAME]]",
        help = "Bambu LAN MQTT device; repeat for multiple printers. Port defaults to 8883. The device ID is inferred from the MQTT certificate. ACCESS_CODE can be provided here or looked up from /bind when needed",
        help_heading = "Bambu LAN"
    )]
    local_devices: Vec<BambuLocalEndpointConfig>,
}

pub async fn run(cli: Cli) -> Result<()> {
    match cli.command {
        Command::Login(args) => login(args).await,
        Command::Devices(args) => devices_cmd(args).await,
        Command::Mqtt(args) => mqtt_cmd(args).await,
        Command::Serve(args) => serve_cmd(args).await,
        Command::SnapPair(args) => snap_pair_cmd(args).await,
    }
}

async fn login(args: LoginArgs) -> Result<()> {
    let client = client(&args.http)?;
    let account = match args.account {
        Some(account) => account,
        None => prompt("Bambu account email/username: ")?,
    };

    if args.password.is_some() && args.code.is_some() {
        bail!("set only one of --password or --code");
    }

    let mut login_response = if let Some(code) = args.code.as_deref() {
        client.login(&account, None, Some(code)).await?
    } else {
        let password = match args.password {
            Some(password) => password,
            None => rpassword::prompt_password("Bambu password: ")
                .context("failed to read Bambu password")?,
        };
        client.login(&account, Some(&password), None).await?
    };

    if requires_verification_code(&login_response) {
        let code = prompt("Bambu verification code: ")?;
        login_response = client.login(&account, None, Some(&code)).await?;
    }

    let access_token = login_response
        .access_token
        .as_ref()
        .map(|token| token.expose().as_str())
        .filter(|token| !token.trim().is_empty())
        .context("login response did not include accessToken")?;
    let uid = client
        .user_preference(access_token)
        .await
        .context("could not fetch MQTT user id from user preference")?
        .mqtt_user_id()
        .context("could not derive MQTT user id from user preference")?;

    let token_path = save_token(
        &login_response,
        Some(args.token.token_file),
        &args.http.api_base,
        &uid,
    )?;
    println!("Saved Bambu access token to {}", token_path.display());
    println!("Run `bambu-overlay serve` to start the overlay.");
    Ok(())
}

async fn devices_cmd(args: DevicesArgs) -> Result<()> {
    let cloud = token_client(Some(args.token.token_file), args.timeout)?;
    let bound_devices = cloud
        .client
        .bound_devices(cloud.access_token.expose())
        .await?;

    println!(
        "{:<24}  {:<32}  {:<8}  {:<12}",
        "ID", "NAME", "ONLINE", "ACCESS CODE"
    );
    for device in bound_devices.devices {
        let id = device.id.unwrap_or_else(|| "--".to_owned());
        let name = device.name.unwrap_or_else(|| "--".to_owned());
        let access_code = device
            .access_code
            .map(|code| code.into_inner())
            .unwrap_or_else(|| "--".to_owned());
        let online = match device.online {
            Some(true) => "yes",
            Some(false) => "no",
            None => "--",
        };
        println!("{id:<24}  {name:<32}  {online:<8}  {access_code:<12}");
    }
    Ok(())
}

async fn snap_pair_cmd(args: SnapPairArgs) -> Result<()> {
    let host = args.host.trim();
    if host.is_empty() {
        bail!("snap-pair host must not be empty");
    }

    let existing = load_snap_tokens(&args.token_file)?;
    let clientid = args
        .clientid
        .or_else(|| {
            existing
                .iter()
                .find(|entry| entry.host == host)
                .map(|entry| entry.clientid.clone())
        })
        .unwrap_or_else(fresh_clientid);

    let config = PairConfig {
        host: host.to_owned(),
        clear_port: args.port,
        approval_timeout: Duration::from_secs_f64(args.timeout),
        clientid,
    };

    println!(
        "Connecting to {host}:{} (cleartext MQTT) with clientid `{}`...",
        args.port, config.clientid
    );

    let token = pair(config, |message| println!("{message}")).await?;

    println!(
        "Approved. Printer SN: {}, mTLS port: {}",
        token.sn, token.mqtt_port
    );

    upsert_snap_token(&args.token_file, token)?;

    println!(
        "Saved pairing to {}. Run `bambu-overlay serve --snap-device {host}` to use it.",
        args.token_file.display()
    );
    Ok(())
}

async fn serve_cmd(args: ServeArgs) -> Result<()> {
    let config = ServerConfig::from(&args);
    let cloud = optional_token_client(args.token.token_file.clone(), args.timeout)?;
    serve(cloud, config).await
}

async fn mqtt_cmd(args: MqttArgs) -> Result<()> {
    let cloud = optional_token_client(args.token.token_file.clone(), args.timeout)?;
    monitor_mqtt(cloud, MonitorConfig::from(&args)).await
}

fn optional_token_client(token_file: PathBuf, timeout: f64) -> Result<Option<CloudSession>> {
    if !token_file.exists() {
        return Ok(None);
    }

    token_client(Some(token_file), timeout).map(Some)
}

fn token_client(token_file: Option<PathBuf>, timeout: f64) -> Result<CloudSession> {
    let token_data = load_token(token_file)?;
    validate_token_freshness(&token_data)?;
    let access_token = token_data.access_token.expose().trim();
    if access_token.is_empty() {
        bail!("cached token file does not include accessToken");
    }
    let user_id = token_data.uid.trim();
    if user_id.is_empty() {
        bail!("cached token file does not include uid");
    }
    let api_base = token_data
        .api_base
        .as_deref()
        .filter(|api_base| !api_base.is_empty())
        .unwrap_or(API_BASE);
    let client = BambuClient::new(api_base, Duration::from_secs_f64(timeout))?;
    Ok(CloudSession {
        client,
        access_token: Secret::new(access_token.to_owned()),
        user_id: user_id.to_owned(),
    })
}

fn validate_token_freshness(token_data: &crate::bambu::auth::TokenData) -> Result<()> {
    let Some(expires_at) = token_data
        .expires_at
        .as_deref()
        .map(str::trim)
        .filter(|expires_at| !expires_at.is_empty())
    else {
        return Ok(());
    };
    let expires_at = DateTime::parse_from_rfc3339(expires_at)
        .context("cached token expiresAt is not a valid RFC3339 timestamp")?
        .with_timezone(&Utc);
    if expires_at <= Utc::now() {
        bail!(
            "cached Bambu token expired at {}; run `bambu-overlay login` again",
            expires_at.to_rfc3339()
        );
    }
    Ok(())
}

fn client(args: &HttpArgs) -> Result<BambuClient> {
    BambuClient::new(&args.api_base, Duration::from_secs_f64(args.timeout))
}

fn requires_verification_code(login_response: &LoginResponse) -> bool {
    login_response
        .login_type
        .as_deref()
        .map(|login_type| login_type.eq_ignore_ascii_case("verifycode"))
        .unwrap_or(false)
}

impl From<&ServeArgs> for ServerConfig {
    fn from(args: &ServeArgs) -> Self {
        Self {
            bind: args.bind.clone(),
            cloud_mqtt: args.devices.cloud_mqtt.clone(),
            local_devices: args.devices.local_devices.clone(),
            cloud_devices: args.devices.cloud_devices.clone(),
            video_endpoints: args.video_devices.clone(),
            snapmaker_devices: args.snapmaker_devices.clone(),
            snap_token_file: Some(args.snap_token_file.clone()),
        }
    }
}

impl From<&MqttArgs> for MonitorConfig {
    fn from(args: &MqttArgs) -> Self {
        Self {
            cloud_mqtt: args.devices.cloud_mqtt.clone(),
            local_devices: args.devices.local_devices.clone(),
            cloud_devices: args.devices.cloud_devices.clone(),
            device: args.device.clone(),
        }
    }
}

fn prompt(label: &str) -> Result<String> {
    use std::io::{self, Write};

    print!("{label}");
    io::stdout().flush().context("failed to flush stdout")?;
    let mut value = String::new();
    io::stdin()
        .read_line(&mut value)
        .context("failed to read stdin")?;
    Ok(value.trim().to_owned())
}

fn positive_f64(value: &str) -> std::result::Result<f64, String> {
    let parsed = value
        .parse::<f64>()
        .map_err(|_| format!("expected a number, got `{value}`"))?;
    if parsed.is_finite() && parsed > 0.0 {
        Ok(parsed)
    } else {
        Err(format!("expected a positive finite number, got `{value}`"))
    }
}

fn parse_bind_endpoint(value: &str) -> std::result::Result<Endpoint, String> {
    Endpoint::parse(value, DEFAULT_PORT)
        .map_err(|error| format!("invalid bind address `{value}`: {error}"))
}

fn parse_cloud_device_id(value: &str) -> std::result::Result<String, String> {
    let value = value.trim();
    if value.is_empty() {
        return Err("cloud device id must not be empty".to_owned());
    }
    if value.contains(',') {
        return Err(format!(
            "invalid cloud device `{value}`: expected only DEVICE_ID"
        ));
    }
    Ok(value.to_owned())
}

#[cfg(test)]
mod tests {
    use crate::bambu::auth::TokenData;

    use super::{parse_cloud_device_id, validate_token_freshness};

    #[test]
    fn cloud_device_parser_accepts_id_only() {
        assert_eq!(parse_cloud_device_id(" printer-a ").unwrap(), "printer-a");
    }

    #[test]
    fn cloud_device_parser_rejects_metadata() {
        let error = parse_cloud_device_id("printer-a,12345678").unwrap_err();
        assert!(error.contains("expected only DEVICE_ID"));
    }

    #[test]
    fn token_freshness_rejects_expired_tokens() {
        let token =
            token_with_expiry((chrono::Utc::now() - chrono::Duration::minutes(1)).to_rfc3339());

        let error = validate_token_freshness(&token).unwrap_err();

        assert!(error.to_string().contains("expired"));
        assert!(error.to_string().contains("bambu-overlay login"));
    }

    #[test]
    fn token_freshness_accepts_unexpired_tokens() {
        let token =
            token_with_expiry((chrono::Utc::now() + chrono::Duration::minutes(1)).to_rfc3339());

        validate_token_freshness(&token).unwrap();
    }

    fn token_with_expiry(expires_at: String) -> TokenData {
        TokenData {
            access_token: super::Secret::new("token".to_owned()),
            api_base: None,
            uid: "123".to_owned(),
            expires_at: Some(expires_at),
        }
    }
}
