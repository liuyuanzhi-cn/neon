//!
//! `neon_local` is an executable that can be used to create a local
//! Neon environment, for testing purposes. The local environment is
//! quite different from the cloud environment with Kubernetes, but it
//! easier to work with locally. The python tests in `test_runner`
//! rely on `neon_local` to set up the environment for each test.
//!
use anyhow::{anyhow, bail, Context, Result};
use clap::{value_parser, Arg, ArgAction, ArgMatches, Command};
use compute_api::spec::ComputeMode;
use control_plane::attachment_service::AttachmentService;
use control_plane::endpoint::ComputeControlPlane;
use control_plane::local_env::LocalEnv;
use control_plane::pageserver::PageServerNode;
use control_plane::safekeeper::SafekeeperNode;
use control_plane::{broker, local_env};
use pageserver_api::models::TimelineInfo;
use pageserver_api::{
    DEFAULT_HTTP_LISTEN_ADDR as DEFAULT_PAGESERVER_HTTP_ADDR,
    DEFAULT_PG_LISTEN_ADDR as DEFAULT_PAGESERVER_PG_ADDR,
};
use postgres_backend::AuthType;
use safekeeper_api::{
    DEFAULT_HTTP_LISTEN_PORT as DEFAULT_SAFEKEEPER_HTTP_PORT,
    DEFAULT_PG_LISTEN_PORT as DEFAULT_SAFEKEEPER_PG_PORT,
};
use std::collections::{BTreeSet, HashMap};
use std::path::PathBuf;
use std::process::exit;
use std::str::FromStr;
use storage_broker::DEFAULT_LISTEN_ADDR as DEFAULT_BROKER_ADDR;
use utils::{
    auth::{Claims, Scope},
    id::{NodeId, TenantId, TenantTimelineId, TimelineId},
    lsn::Lsn,
    project_git_version,
};

// Default id of a safekeeper node, if not specified on the command line.
const DEFAULT_SAFEKEEPER_ID: NodeId = NodeId(1);
const DEFAULT_PAGESERVER_ID: NodeId = NodeId(1);
const DEFAULT_BRANCH_NAME: &str = "main";
project_git_version!(GIT_VERSION);

const DEFAULT_PG_VERSION: &str = "15";

const DEFAULT_PAGESERVER_CONTROL_PLANE_API: &str = "http://127.0.0.1:1234/";

fn default_conf() -> String {
    format!(
        r#"
# Default built-in configuration, defined in main.rs
control_plane_api = '{DEFAULT_PAGESERVER_CONTROL_PLANE_API}'

[broker]
listen_addr = '{DEFAULT_BROKER_ADDR}'

[[pageservers]]
id = {DEFAULT_PAGESERVER_ID}
listen_pg_addr = '{DEFAULT_PAGESERVER_PG_ADDR}'
listen_http_addr = '{DEFAULT_PAGESERVER_HTTP_ADDR}'
pg_auth_type = '{trust_auth}'
http_auth_type = '{trust_auth}'

[[safekeepers]]
id = {DEFAULT_SAFEKEEPER_ID}
pg_port = {DEFAULT_SAFEKEEPER_PG_PORT}
http_port = {DEFAULT_SAFEKEEPER_HTTP_PORT}

"#,
        trust_auth = AuthType::Trust,
    )
}

///
/// Timelines tree element used as a value in the HashMap.
///
struct TimelineTreeEl {
    /// `TimelineInfo` received from the `pageserver` via the `timeline_list` http API call.
    pub info: TimelineInfo,
    /// Name, recovered from neon config mappings
    pub name: Option<String>,
    /// Holds all direct children of this timeline referenced using `timeline_id`.
    pub children: BTreeSet<TimelineId>,
}

// Main entry point for the 'neon_local' CLI utility
//
// This utility helps to manage neon installation. That includes following:
//   * Management of local postgres installations running on top of the
//     pageserver.
//   * Providing CLI api to the pageserver
//   * TODO: export/import to/from usual postgres
fn main() -> Result<()> {
    let matches = cli().get_matches();

    let (sub_name, sub_args) = match matches.subcommand() {
        Some(subcommand_data) => subcommand_data,
        None => bail!("no subcommand provided"),
    };

    // Check for 'neon init' command first.
    let subcommand_result = if sub_name == "init" {
        handle_init(sub_args).map(Some)
    } else {
        // all other commands need an existing config
        let mut env = LocalEnv::load_config().context("Error loading config")?;
        let original_env = env.clone();

        let subcommand_result = match sub_name {
            "tenant" => handle_tenant(sub_args, &mut env),
            "timeline" => handle_timeline(sub_args, &mut env),
            "start" => handle_start_all(sub_args, &env),
            "stop" => handle_stop_all(sub_args, &env),
            "pageserver" => handle_pageserver(sub_args, &env),
            "attachment_service" => handle_attachment_service(sub_args, &env),
            "safekeeper" => handle_safekeeper(sub_args, &env),
            "endpoint" => handle_endpoint(sub_args, &env),
            "pg" => bail!("'pg' subcommand has been renamed to 'endpoint'"),
            _ => bail!("unexpected subcommand {sub_name}"),
        };

        if original_env != env {
            subcommand_result.map(|()| Some(env))
        } else {
            subcommand_result.map(|()| None)
        }
    };

    match subcommand_result {
        Ok(Some(updated_env)) => updated_env.persist_config(&updated_env.base_data_dir)?,
        Ok(None) => (),
        Err(e) => {
            eprintln!("command failed: {e:?}");
            exit(1);
        }
    }
    Ok(())
}

///
/// Prints timelines list as a tree-like structure.
///
fn print_timelines_tree(
    timelines: Vec<TimelineInfo>,
    mut timeline_name_mappings: HashMap<TenantTimelineId, String>,
) -> Result<()> {
    let mut timelines_hash = timelines
        .iter()
        .map(|t| {
            (
                t.timeline_id,
                TimelineTreeEl {
                    info: t.clone(),
                    children: BTreeSet::new(),
                    name: timeline_name_mappings
                        .remove(&TenantTimelineId::new(t.tenant_id, t.timeline_id)),
                },
            )
        })
        .collect::<HashMap<_, _>>();

    // Memorize all direct children of each timeline.
    for timeline in timelines.iter() {
        if let Some(ancestor_timeline_id) = timeline.ancestor_timeline_id {
            timelines_hash
                .get_mut(&ancestor_timeline_id)
                .context("missing timeline info in the HashMap")?
                .children
                .insert(timeline.timeline_id);
        }
    }

    for timeline in timelines_hash.values() {
        // Start with root local timelines (no ancestors) first.
        if timeline.info.ancestor_timeline_id.is_none() {
            print_timeline(0, &Vec::from([true]), timeline, &timelines_hash)?;
        }
    }

    Ok(())
}

///
/// Recursively prints timeline info with all its children.
///
fn print_timeline(
    nesting_level: usize,
    is_last: &[bool],
    timeline: &TimelineTreeEl,
    timelines: &HashMap<TimelineId, TimelineTreeEl>,
) -> Result<()> {
    if nesting_level > 0 {
        let ancestor_lsn = match timeline.info.ancestor_lsn {
            Some(lsn) => lsn.to_string(),
            None => "Unknown Lsn".to_string(),
        };

        let mut br_sym = "┣━";

        // Draw each nesting padding with proper style
        // depending on whether its timeline ended or not.
        if nesting_level > 1 {
            for l in &is_last[1..is_last.len() - 1] {
                if *l {
                    print!("   ");
                } else {
                    print!("┃  ");
                }
            }
        }

        // We are the last in this sub-timeline
        if *is_last.last().unwrap() {
            br_sym = "┗━";
        }

        print!("{} @{}: ", br_sym, ancestor_lsn);
    }

    // Finally print a timeline id and name with new line
    println!(
        "{} [{}]",
        timeline.name.as_deref().unwrap_or("_no_name_"),
        timeline.info.timeline_id
    );

    let len = timeline.children.len();
    let mut i: usize = 0;
    let mut is_last_new = Vec::from(is_last);
    is_last_new.push(false);

    for child in &timeline.children {
        i += 1;

        // Mark that the last padding is the end of the timeline
        if i == len {
            if let Some(last) = is_last_new.last_mut() {
                *last = true;
            }
        }

        print_timeline(
            nesting_level + 1,
            &is_last_new,
            timelines
                .get(child)
                .context("missing timeline info in the HashMap")?,
            timelines,
        )?;
    }

    Ok(())
}

/// Returns a map of timeline IDs to timeline_id@lsn strings.
/// Connects to the pageserver to query this information.
fn get_timeline_infos(
    env: &local_env::LocalEnv,
    tenant_id: &TenantId,
) -> Result<HashMap<TimelineId, TimelineInfo>> {
    Ok(get_default_pageserver(env)
        .timeline_list(tenant_id)?
        .into_iter()
        .map(|timeline_info| (timeline_info.timeline_id, timeline_info))
        .collect())
}

// Helper function to parse --tenant_id option, or get the default from config file
fn get_tenant_id(sub_match: &ArgMatches, env: &local_env::LocalEnv) -> anyhow::Result<TenantId> {
    if let Some(tenant_id_from_arguments) = parse_tenant_id(sub_match).transpose() {
        tenant_id_from_arguments
    } else if let Some(default_id) = env.default_tenant_id {
        Ok(default_id)
    } else {
        anyhow::bail!("No tenant id. Use --tenant-id, or set a default tenant");
    }
}

fn parse_tenant_id(sub_match: &ArgMatches) -> anyhow::Result<Option<TenantId>> {
    sub_match
        .get_one::<String>("tenant-id")
        .map(|tenant_id| TenantId::from_str(tenant_id))
        .transpose()
        .context("Failed to parse tenant id from the argument string")
}

fn parse_timeline_id(sub_match: &ArgMatches) -> anyhow::Result<Option<TimelineId>> {
    sub_match
        .get_one::<String>("timeline-id")
        .map(|timeline_id| TimelineId::from_str(timeline_id))
        .transpose()
        .context("Failed to parse timeline id from the argument string")
}

fn handle_init(init_match: &ArgMatches) -> anyhow::Result<LocalEnv> {
    // Create config file
    let toml_file: String = if let Some(config_path) = init_match.get_one::<PathBuf>("config") {
        // load and parse the file
        std::fs::read_to_string(config_path).with_context(|| {
            format!(
                "Could not read configuration file '{}'",
                config_path.display()
            )
        })?
    } else {
        // Built-in default config
        default_conf()
    };

    let pg_version = init_match
        .get_one::<u32>("pg-version")
        .copied()
        .context("Failed to parse postgres version from the argument string")?;

    let mut env =
        LocalEnv::parse_config(&toml_file).context("Failed to create neon configuration")?;
    let force = init_match.get_flag("force");
    env.init(pg_version, force)
        .context("Failed to initialize neon repository")?;

    // Initialize pageserver, create initial tenant and timeline.
    for ps_conf in &env.pageservers {
        PageServerNode::from_env(&env, ps_conf)
            .initialize(&pageserver_config_overrides(init_match))
            .unwrap_or_else(|e| {
                eprintln!("pageserver init failed: {e:?}");
                exit(1);
            });
    }

    Ok(env)
}

/// The default pageserver is the one where CLI tenant/timeline operations are sent by default.
/// For typical interactive use, one would just run with a single pageserver.  Scenarios with
/// tenant/timeline placement across multiple pageservers are managed by python test code rather
/// than this CLI.
fn get_default_pageserver(env: &local_env::LocalEnv) -> PageServerNode {
    let ps_conf = env
        .pageservers
        .first()
        .expect("Config is validated to contain at least one pageserver");
    PageServerNode::from_env(env, ps_conf)
}

fn pageserver_config_overrides(init_match: &ArgMatches) -> Vec<&str> {
    init_match
        .get_many::<String>("pageserver-config-override")
        .into_iter()
        .flatten()
        .map(String::as_str)
        .collect()
}

fn handle_tenant(tenant_match: &ArgMatches, env: &mut local_env::LocalEnv) -> anyhow::Result<()> {
    let pageserver = get_default_pageserver(env);
    match tenant_match.subcommand() {
        Some(("list", _)) => {
            for t in pageserver.tenant_list()? {
                println!("{} {:?}", t.id, t.state);
            }
        }
        Some(("create", create_match)) => {
            let tenant_conf: HashMap<_, _> = create_match
                .get_many::<String>("config")
                .map(|vals| vals.flat_map(|c| c.split_once(':')).collect())
                .unwrap_or_default();

            // If tenant ID was not specified, generate one
            let tenant_id = parse_tenant_id(create_match)?.unwrap_or_else(TenantId::generate);

            let generation = if env.control_plane_api.is_some() {
                // We must register the tenant with the attachment service, so
                // that when the pageserver restarts, it will be re-attached.
                let attachment_service = AttachmentService::from_env(env);
                attachment_service.attach_hook(tenant_id, pageserver.conf.id)?
            } else {
                None
            };

            pageserver.tenant_create(tenant_id, generation, tenant_conf)?;
            println!("tenant {tenant_id} successfully created on the pageserver");

            // Create an initial timeline for the new tenant
            let new_timeline_id = parse_timeline_id(create_match)?;
            let pg_version = create_match
                .get_one::<u32>("pg-version")
                .copied()
                .context("Failed to parse postgres version from the argument string")?;

            let timeline_info = pageserver.timeline_create(
                tenant_id,
                new_timeline_id,
                None,
                None,
                Some(pg_version),
            )?;
            let new_timeline_id = timeline_info.timeline_id;
            let last_record_lsn = timeline_info.last_record_lsn;

            env.register_branch_mapping(
                DEFAULT_BRANCH_NAME.to_string(),
                tenant_id,
                new_timeline_id,
            )?;

            println!(
                "Created an initial timeline '{new_timeline_id}' at Lsn {last_record_lsn} for tenant: {tenant_id}",
            );

            if create_match.get_flag("set-default") {
                println!("Setting tenant {tenant_id} as a default one");
                env.default_tenant_id = Some(tenant_id);
            }
        }
        Some(("set-default", set_default_match)) => {
            let tenant_id =
                parse_tenant_id(set_default_match)?.context("No tenant id specified")?;
            println!("Setting tenant {tenant_id} as a default one");
            env.default_tenant_id = Some(tenant_id);
        }
        Some(("config", create_match)) => {
            let tenant_id = get_tenant_id(create_match, env)?;
            let tenant_conf: HashMap<_, _> = create_match
                .get_many::<String>("config")
                .map(|vals| vals.flat_map(|c| c.split_once(':')).collect())
                .unwrap_or_default();

            pageserver
                .tenant_config(tenant_id, tenant_conf)
                .with_context(|| format!("Tenant config failed for tenant with id {tenant_id}"))?;
            println!("tenant {tenant_id} successfully configured on the pageserver");
        }
        Some((sub_name, _)) => bail!("Unexpected tenant subcommand '{}'", sub_name),
        None => bail!("no tenant subcommand provided"),
    }
    Ok(())
}

fn handle_timeline(timeline_match: &ArgMatches, env: &mut local_env::LocalEnv) -> Result<()> {
    let pageserver = get_default_pageserver(env);

    match timeline_match.subcommand() {
        Some(("list", list_match)) => {
            let tenant_id = get_tenant_id(list_match, env)?;
            let timelines = pageserver.timeline_list(&tenant_id)?;
            print_timelines_tree(timelines, env.timeline_name_mappings())?;
        }
        Some(("create", create_match)) => {
            let tenant_id = get_tenant_id(create_match, env)?;
            let new_branch_name = create_match
                .get_one::<String>("branch-name")
                .ok_or_else(|| anyhow!("No branch name provided"))?;

            let pg_version = create_match
                .get_one::<u32>("pg-version")
                .copied()
                .context("Failed to parse postgres version from the argument string")?;

            let timeline_info =
                pageserver.timeline_create(tenant_id, None, None, None, Some(pg_version))?;
            let new_timeline_id = timeline_info.timeline_id;

            let last_record_lsn = timeline_info.last_record_lsn;
            env.register_branch_mapping(new_branch_name.to_string(), tenant_id, new_timeline_id)?;

            println!(
                "Created timeline '{}' at Lsn {last_record_lsn} for tenant: {tenant_id}",
                timeline_info.timeline_id
            );
        }
        Some(("import", import_match)) => {
            let tenant_id = get_tenant_id(import_match, env)?;
            let timeline_id = parse_timeline_id(import_match)?.expect("No timeline id provided");
            let name = import_match
                .get_one::<String>("node-name")
                .ok_or_else(|| anyhow!("No node name provided"))?;

            // Parse base inputs
            let base_tarfile = import_match
                .get_one::<PathBuf>("base-tarfile")
                .ok_or_else(|| anyhow!("No base-tarfile provided"))?
                .to_owned();
            let base_lsn = Lsn::from_str(
                import_match
                    .get_one::<String>("base-lsn")
                    .ok_or_else(|| anyhow!("No base-lsn provided"))?,
            )?;
            let base = (base_lsn, base_tarfile);

            // Parse pg_wal inputs
            let wal_tarfile = import_match.get_one::<PathBuf>("wal-tarfile").cloned();
            let end_lsn = import_match
                .get_one::<String>("end-lsn")
                .map(|s| Lsn::from_str(s).unwrap());
            // TODO validate both or none are provided
            let pg_wal = end_lsn.zip(wal_tarfile);

            let pg_version = import_match
                .get_one::<u32>("pg-version")
                .copied()
                .context("Failed to parse postgres version from the argument string")?;

            let mut cplane = ComputeControlPlane::load(env.clone())?;
            println!("Importing timeline into pageserver ...");
            pageserver.timeline_import(tenant_id, timeline_id, base, pg_wal, pg_version)?;
            env.register_branch_mapping(name.to_string(), tenant_id, timeline_id)?;

            println!("Creating endpoint for imported timeline ...");
            cplane.new_endpoint(
                name,
                tenant_id,
                timeline_id,
                None,
                None,
                pg_version,
                ComputeMode::Primary,
                DEFAULT_PAGESERVER_ID,
            )?;
            println!("Done");
        }
        Some(("branch", branch_match)) => {
            let tenant_id = get_tenant_id(branch_match, env)?;
            let new_branch_name = branch_match
                .get_one::<String>("branch-name")
                .ok_or_else(|| anyhow!("No branch name provided"))?;
            let ancestor_branch_name = branch_match
                .get_one::<String>("ancestor-branch-name")
                .map(|s| s.as_str())
                .unwrap_or(DEFAULT_BRANCH_NAME);
            let ancestor_timeline_id = env
                .get_branch_timeline_id(ancestor_branch_name, tenant_id)
                .ok_or_else(|| {
                    anyhow!("Found no timeline id for branch name '{ancestor_branch_name}'")
                })?;

            let start_lsn = branch_match
                .get_one::<String>("ancestor-start-lsn")
                .map(|lsn_str| Lsn::from_str(lsn_str))
                .transpose()
                .context("Failed to parse ancestor start Lsn from the request")?;
            let timeline_info = pageserver.timeline_create(
                tenant_id,
                None,
                start_lsn,
                Some(ancestor_timeline_id),
                None,
            )?;
            let new_timeline_id = timeline_info.timeline_id;

            let last_record_lsn = timeline_info.last_record_lsn;

            env.register_branch_mapping(new_branch_name.to_string(), tenant_id, new_timeline_id)?;

            println!(
                "Created timeline '{}' at Lsn {last_record_lsn} for tenant: {tenant_id}. Ancestor timeline: '{ancestor_branch_name}'",
                timeline_info.timeline_id
            );
        }
        Some((sub_name, _)) => bail!("Unexpected tenant subcommand '{sub_name}'"),
        None => bail!("no tenant subcommand provided"),
    }

    Ok(())
}

fn handle_endpoint(ep_match: &ArgMatches, env: &local_env::LocalEnv) -> Result<()> {
    let (sub_name, sub_args) = match ep_match.subcommand() {
        Some(ep_subcommand_data) => ep_subcommand_data,
        None => bail!("no endpoint subcommand provided"),
    };
    let mut cplane = ComputeControlPlane::load(env.clone())?;

    // All subcommands take an optional --tenant-id option
    let tenant_id = get_tenant_id(sub_args, env)?;

    match sub_name {
        "list" => {
            let timeline_infos = get_timeline_infos(env, &tenant_id).unwrap_or_else(|e| {
                eprintln!("Failed to load timeline info: {}", e);
                HashMap::new()
            });

            let timeline_name_mappings = env.timeline_name_mappings();

            let mut table = comfy_table::Table::new();

            table.load_preset(comfy_table::presets::NOTHING);

            table.set_header([
                "ENDPOINT",
                "ADDRESS",
                "TIMELINE",
                "BRANCH NAME",
                "LSN",
                "STATUS",
            ]);

            for (endpoint_id, endpoint) in cplane
                .endpoints
                .iter()
                .filter(|(_, endpoint)| endpoint.tenant_id == tenant_id)
            {
                let lsn_str = match endpoint.mode {
                    ComputeMode::Static(lsn) => {
                        // -> read-only endpoint
                        // Use the node's LSN.
                        lsn.to_string()
                    }
                    _ => {
                        // -> primary endpoint or hot replica
                        // Use the LSN at the end of the timeline.
                        timeline_infos
                            .get(&endpoint.timeline_id)
                            .map(|bi| bi.last_record_lsn.to_string())
                            .unwrap_or_else(|| "?".to_string())
                    }
                };

                let branch_name = timeline_name_mappings
                    .get(&TenantTimelineId::new(tenant_id, endpoint.timeline_id))
                    .map(|name| name.as_str())
                    .unwrap_or("?");

                table.add_row([
                    endpoint_id.as_str(),
                    &endpoint.pg_address.to_string(),
                    &endpoint.timeline_id.to_string(),
                    branch_name,
                    lsn_str.as_str(),
                    endpoint.status(),
                ]);
            }

            println!("{table}");
        }
        "create" => {
            let branch_name = sub_args
                .get_one::<String>("branch-name")
                .map(|s| s.as_str())
                .unwrap_or(DEFAULT_BRANCH_NAME);
            let endpoint_id = sub_args
                .get_one::<String>("endpoint_id")
                .map(String::to_string)
                .unwrap_or_else(|| format!("ep-{branch_name}"));

            let lsn = sub_args
                .get_one::<String>("lsn")
                .map(|lsn_str| Lsn::from_str(lsn_str))
                .transpose()
                .context("Failed to parse Lsn from the request")?;
            let timeline_id = env
                .get_branch_timeline_id(branch_name, tenant_id)
                .ok_or_else(|| anyhow!("Found no timeline id for branch name '{branch_name}'"))?;

            let pg_port: Option<u16> = sub_args.get_one::<u16>("pg-port").copied();
            let http_port: Option<u16> = sub_args.get_one::<u16>("http-port").copied();
            let pg_version = sub_args
                .get_one::<u32>("pg-version")
                .copied()
                .context("Failed to parse postgres version from the argument string")?;

            let hot_standby = sub_args
                .get_one::<bool>("hot-standby")
                .copied()
                .unwrap_or(false);

            let pageserver_id =
                if let Some(id_str) = sub_args.get_one::<String>("endpoint-pageserver-id") {
                    NodeId(id_str.parse().context("while parsing pageserver id")?)
                } else {
                    DEFAULT_PAGESERVER_ID
                };

            let mode = match (lsn, hot_standby) {
                (Some(lsn), false) => ComputeMode::Static(lsn),
                (None, true) => ComputeMode::Replica,
                (None, false) => ComputeMode::Primary,
                (Some(_), true) => anyhow::bail!("cannot specify both lsn and hot-standby"),
            };

            cplane.new_endpoint(
                &endpoint_id,
                tenant_id,
                timeline_id,
                pg_port,
                http_port,
                pg_version,
                mode,
                pageserver_id,
            )?;
        }
        "start" => {
            let pg_port: Option<u16> = sub_args.get_one::<u16>("pg-port").copied();
            let http_port: Option<u16> = sub_args.get_one::<u16>("http-port").copied();
            let endpoint_id = sub_args
                .get_one::<String>("endpoint_id")
                .ok_or_else(|| anyhow!("No endpoint ID was provided to start"))?;

            let pageserver_id =
                if let Some(id_str) = sub_args.get_one::<String>("endpoint-pageserver-id") {
                    NodeId(id_str.parse().context("while parsing pageserver id")?)
                } else {
                    DEFAULT_PAGESERVER_ID
                };

            let remote_ext_config = sub_args.get_one::<String>("remote-ext-config");

            // If --safekeepers argument is given, use only the listed safekeeper nodes.
            let safekeepers =
                if let Some(safekeepers_str) = sub_args.get_one::<String>("safekeepers") {
                    let mut safekeepers: Vec<NodeId> = Vec::new();
                    for sk_id in safekeepers_str.split(',').map(str::trim) {
                        let sk_id = NodeId(u64::from_str(sk_id).map_err(|_| {
                            anyhow!("invalid node ID \"{sk_id}\" in --safekeepers list")
                        })?);
                        safekeepers.push(sk_id);
                    }
                    safekeepers
                } else {
                    env.safekeepers.iter().map(|sk| sk.id).collect()
                };

            let endpoint = cplane.endpoints.get(endpoint_id.as_str());

            let ps_conf = env.get_pageserver_conf(pageserver_id)?;
            let auth_token = if matches!(ps_conf.pg_auth_type, AuthType::NeonJWT) {
                let claims = Claims::new(Some(tenant_id), Scope::Tenant);

                Some(env.generate_auth_token(&claims)?)
            } else {
                None
            };

            let hot_standby = sub_args
                .get_one::<bool>("hot-standby")
                .copied()
                .unwrap_or(false);

            if let Some(endpoint) = endpoint {
                match (&endpoint.mode, hot_standby) {
                    (ComputeMode::Static(_), true) => {
                        bail!("Cannot start a node in hot standby mode when it is already configured as a static replica")
                    }
                    (ComputeMode::Primary, true) => {
                        bail!("Cannot start a node as a hot standby replica, it is already configured as primary node")
                    }
                    _ => {}
                }
                println!("Starting existing endpoint {endpoint_id}...");
                endpoint.start(&auth_token, safekeepers, remote_ext_config)?;
            } else {
                let branch_name = sub_args
                    .get_one::<String>("branch-name")
                    .map(|s| s.as_str())
                    .unwrap_or(DEFAULT_BRANCH_NAME);
                let timeline_id = env
                    .get_branch_timeline_id(branch_name, tenant_id)
                    .ok_or_else(|| {
                        anyhow!("Found no timeline id for branch name '{branch_name}'")
                    })?;
                let lsn = sub_args
                    .get_one::<String>("lsn")
                    .map(|lsn_str| Lsn::from_str(lsn_str))
                    .transpose()
                    .context("Failed to parse Lsn from the request")?;
                let pg_version = sub_args
                    .get_one::<u32>("pg-version")
                    .copied()
                    .context("Failed to `pg-version` from the argument string")?;

                let mode = match (lsn, hot_standby) {
                    (Some(lsn), false) => ComputeMode::Static(lsn),
                    (None, true) => ComputeMode::Replica,
                    (None, false) => ComputeMode::Primary,
                    (Some(_), true) => anyhow::bail!("cannot specify both lsn and hot-standby"),
                };

                // when used with custom port this results in non obvious behaviour
                // port is remembered from first start command, i e
                // start --port X
                // stop
                // start <-- will also use port X even without explicit port argument
                println!("Starting new endpoint {endpoint_id} (PostgreSQL v{pg_version}) on timeline {timeline_id} ...");

                let ep = cplane.new_endpoint(
                    endpoint_id,
                    tenant_id,
                    timeline_id,
                    pg_port,
                    http_port,
                    pg_version,
                    mode,
                    pageserver_id,
                )?;
                ep.start(&auth_token, safekeepers, remote_ext_config)?;
            }
        }
        "stop" => {
            let endpoint_id = sub_args
                .get_one::<String>("endpoint_id")
                .ok_or_else(|| anyhow!("No endpoint ID was provided to stop"))?;
            let destroy = sub_args.get_flag("destroy");

            let endpoint = cplane
                .endpoints
                .get(endpoint_id.as_str())
                .with_context(|| format!("postgres endpoint {endpoint_id} is not found"))?;
            endpoint.stop(destroy)?;
        }

        _ => bail!("Unexpected endpoint subcommand '{sub_name}'"),
    }

    Ok(())
}

fn handle_pageserver(sub_match: &ArgMatches, env: &local_env::LocalEnv) -> Result<()> {
    fn get_pageserver(env: &local_env::LocalEnv, args: &ArgMatches) -> Result<PageServerNode> {
        let node_id = if let Some(id_str) = args.get_one::<String>("pageserver-id") {
            NodeId(id_str.parse().context("while parsing pageserver id")?)
        } else {
            DEFAULT_PAGESERVER_ID
        };

        Ok(PageServerNode::from_env(
            env,
            env.get_pageserver_conf(node_id)?,
        ))
    }

    match sub_match.subcommand() {
        Some(("start", subcommand_args)) => {
            if let Err(e) = get_pageserver(env, subcommand_args)?
                .start(&pageserver_config_overrides(subcommand_args))
            {
                eprintln!("pageserver start failed: {e}");
                exit(1);
            }
        }

        Some(("stop", subcommand_args)) => {
            let immediate = subcommand_args
                .get_one::<String>("stop-mode")
                .map(|s| s.as_str())
                == Some("immediate");

            if let Err(e) = get_pageserver(env, subcommand_args)?.stop(immediate) {
                eprintln!("pageserver stop failed: {}", e);
                exit(1);
            }
        }

        Some(("restart", subcommand_args)) => {
            let pageserver = get_pageserver(env, subcommand_args)?;
            //TODO what shutdown strategy should we use here?
            if let Err(e) = pageserver.stop(false) {
                eprintln!("pageserver stop failed: {}", e);
                exit(1);
            }

            if let Err(e) = pageserver.start(&pageserver_config_overrides(subcommand_args)) {
                eprintln!("pageserver start failed: {e}");
                exit(1);
            }
        }

        Some(("status", subcommand_args)) => {
            match get_pageserver(env, subcommand_args)?.check_status() {
                Ok(_) => println!("Page server is up and running"),
                Err(err) => {
                    eprintln!("Page server is not available: {}", err);
                    exit(1);
                }
            }
        }

        Some((sub_name, _)) => bail!("Unexpected pageserver subcommand '{}'", sub_name),
        None => bail!("no pageserver subcommand provided"),
    }
    Ok(())
}

fn handle_attachment_service(sub_match: &ArgMatches, env: &local_env::LocalEnv) -> Result<()> {
    let svc = AttachmentService::from_env(env);
    match sub_match.subcommand() {
        Some(("start", _start_match)) => {
            if let Err(e) = svc.start() {
                eprintln!("start failed: {e}");
                exit(1);
            }
        }

        Some(("stop", stop_match)) => {
            let immediate = stop_match
                .get_one::<String>("stop-mode")
                .map(|s| s.as_str())
                == Some("immediate");

            if let Err(e) = svc.stop(immediate) {
                eprintln!("stop failed: {}", e);
                exit(1);
            }
        }
        Some((sub_name, _)) => bail!("Unexpected attachment_service subcommand '{}'", sub_name),
        None => bail!("no attachment_service subcommand provided"),
    }
    Ok(())
}

fn get_safekeeper(env: &local_env::LocalEnv, id: NodeId) -> Result<SafekeeperNode> {
    if let Some(node) = env.safekeepers.iter().find(|node| node.id == id) {
        Ok(SafekeeperNode::from_env(env, node))
    } else {
        bail!("could not find safekeeper {id}")
    }
}

// Get list of options to append to safekeeper command invocation.
fn safekeeper_extra_opts(init_match: &ArgMatches) -> Vec<String> {
    init_match
        .get_many::<String>("safekeeper-extra-opt")
        .into_iter()
        .flatten()
        .map(|s| s.to_owned())
        .collect()
}

fn handle_safekeeper(sub_match: &ArgMatches, env: &local_env::LocalEnv) -> Result<()> {
    let (sub_name, sub_args) = match sub_match.subcommand() {
        Some(safekeeper_command_data) => safekeeper_command_data,
        None => bail!("no safekeeper subcommand provided"),
    };

    // All the commands take an optional safekeeper name argument
    let sk_id = if let Some(id_str) = sub_args.get_one::<String>("id") {
        NodeId(id_str.parse().context("while parsing safekeeper id")?)
    } else {
        DEFAULT_SAFEKEEPER_ID
    };
    let safekeeper = get_safekeeper(env, sk_id)?;

    match sub_name {
        "start" => {
            let extra_opts = safekeeper_extra_opts(sub_args);

            if let Err(e) = safekeeper.start(extra_opts) {
                eprintln!("safekeeper start failed: {}", e);
                exit(1);
            }
        }

        "stop" => {
            let immediate =
                sub_args.get_one::<String>("stop-mode").map(|s| s.as_str()) == Some("immediate");

            if let Err(e) = safekeeper.stop(immediate) {
                eprintln!("safekeeper stop failed: {}", e);
                exit(1);
            }
        }

        "restart" => {
            let immediate =
                sub_args.get_one::<String>("stop-mode").map(|s| s.as_str()) == Some("immediate");

            if let Err(e) = safekeeper.stop(immediate) {
                eprintln!("safekeeper stop failed: {}", e);
                exit(1);
            }

            let extra_opts = safekeeper_extra_opts(sub_args);
            if let Err(e) = safekeeper.start(extra_opts) {
                eprintln!("safekeeper start failed: {}", e);
                exit(1);
            }
        }

        _ => {
            bail!("Unexpected safekeeper subcommand '{}'", sub_name)
        }
    }
    Ok(())
}

fn handle_start_all(sub_match: &ArgMatches, env: &local_env::LocalEnv) -> anyhow::Result<()> {
    // Endpoints are not started automatically

    broker::start_broker_process(env)?;

    // Only start the attachment service if the pageserver is configured to need it
    if env.control_plane_api.is_some() {
        let attachment_service = AttachmentService::from_env(env);
        if let Err(e) = attachment_service.start() {
            eprintln!("attachment_service start failed: {:#}", e);
            try_stop_all(env, true);
            exit(1);
        }
    }

    for ps_conf in &env.pageservers {
        let pageserver = PageServerNode::from_env(env, ps_conf);
        if let Err(e) = pageserver.start(&pageserver_config_overrides(sub_match)) {
            eprintln!("pageserver {} start failed: {:#}", ps_conf.id, e);
            try_stop_all(env, true);
            exit(1);
        }
    }

    for node in env.safekeepers.iter() {
        let safekeeper = SafekeeperNode::from_env(env, node);
        if let Err(e) = safekeeper.start(vec![]) {
            eprintln!("safekeeper {} start failed: {:#}", safekeeper.id, e);
            try_stop_all(env, false);
            exit(1);
        }
    }
    Ok(())
}

fn handle_stop_all(sub_match: &ArgMatches, env: &local_env::LocalEnv) -> Result<()> {
    let immediate =
        sub_match.get_one::<String>("stop-mode").map(|s| s.as_str()) == Some("immediate");

    try_stop_all(env, immediate);

    Ok(())
}

fn try_stop_all(env: &local_env::LocalEnv, immediate: bool) {
    // Stop all endpoints
    match ComputeControlPlane::load(env.clone()) {
        Ok(cplane) => {
            for (_k, node) in cplane.endpoints {
                if let Err(e) = node.stop(false) {
                    eprintln!("postgres stop failed: {e:#}");
                }
            }
        }
        Err(e) => {
            eprintln!("postgres stop failed, could not restore control plane data from env: {e:#}")
        }
    }

    for ps_conf in &env.pageservers {
        let pageserver = PageServerNode::from_env(env, ps_conf);
        if let Err(e) = pageserver.stop(immediate) {
            eprintln!("pageserver {} stop failed: {:#}", ps_conf.id, e);
        }
    }

    for node in env.safekeepers.iter() {
        let safekeeper = SafekeeperNode::from_env(env, node);
        if let Err(e) = safekeeper.stop(immediate) {
            eprintln!("safekeeper {} stop failed: {:#}", safekeeper.id, e);
        }
    }

    if let Err(e) = broker::stop_broker_process(env) {
        eprintln!("neon broker stop failed: {e:#}");
    }

    if env.control_plane_api.is_some() {
        let attachment_service = AttachmentService::from_env(env);
        if let Err(e) = attachment_service.stop(immediate) {
            eprintln!("attachment service stop failed: {e:#}");
        }
    }
}

fn cli() -> Command {
    let branch_name_arg = Arg::new("branch-name")
        .long("branch-name")
        .help("Name of the branch to be created or used as an alias for other services")
        .required(false);

    let endpoint_id_arg = Arg::new("endpoint_id")
        .help("Postgres endpoint id")
        .required(false);

    let safekeeper_id_arg = Arg::new("id").help("safekeeper id").required(false);

    // --id, when using a pageserver command
    let pageserver_id_arg = Arg::new("pageserver-id")
        .long("id")
        .help("pageserver id")
        .required(false);
    // --pageserver-id when using a non-pageserver command
    let endpoint_pageserver_id_arg = Arg::new("endpoint-pageserver-id")
        .long("pageserver-id")
        .required(false);

    let safekeeper_extra_opt_arg = Arg::new("safekeeper-extra-opt")
        .short('e')
        .long("safekeeper-extra-opt")
        .num_args(1)
        .action(ArgAction::Append)
        .help("Additional safekeeper invocation options, e.g. -e=--http-auth-public-key-path=foo")
        .required(false);

    let tenant_id_arg = Arg::new("tenant-id")
        .long("tenant-id")
        .help("Tenant id. Represented as a hexadecimal string 32 symbols length")
        .required(false);

    let timeline_id_arg = Arg::new("timeline-id")
        .long("timeline-id")
        .help("Timeline id. Represented as a hexadecimal string 32 symbols length")
        .required(false);

    let pg_version_arg = Arg::new("pg-version")
        .long("pg-version")
        .help("Postgres version to use for the initial tenant")
        .required(false)
        .value_parser(value_parser!(u32))
        .default_value(DEFAULT_PG_VERSION);

    let pg_port_arg = Arg::new("pg-port")
        .long("pg-port")
        .required(false)
        .value_parser(value_parser!(u16))
        .value_name("pg-port");

    let http_port_arg = Arg::new("http-port")
        .long("http-port")
        .required(false)
        .value_parser(value_parser!(u16))
        .value_name("http-port");

    let safekeepers_arg = Arg::new("safekeepers")
        .long("safekeepers")
        .required(false)
        .value_name("safekeepers");

    let stop_mode_arg = Arg::new("stop-mode")
        .short('m')
        .value_parser(["fast", "immediate"])
        .default_value("fast")
        .help("If 'immediate', don't flush repository data at shutdown")
        .required(false)
        .value_name("stop-mode");

    let pageserver_config_args = Arg::new("pageserver-config-override")
        .long("pageserver-config-override")
        .num_args(1)
        .action(ArgAction::Append)
        .help("Additional pageserver's configuration options or overrides, refer to pageserver's 'config-override' CLI parameter docs for more")
        .required(false);

    let remote_ext_config_args = Arg::new("remote-ext-config")
        .long("remote-ext-config")
        .num_args(1)
        .help("Configure the S3 bucket that we search for extensions in.")
        .required(false);

    let lsn_arg = Arg::new("lsn")
        .long("lsn")
        .help("Specify Lsn on the timeline to start from. By default, end of the timeline would be used.")
        .required(false);

    let hot_standby_arg = Arg::new("hot-standby")
        .value_parser(value_parser!(bool))
        .long("hot-standby")
        .help("If set, the node will be a hot replica on the specified timeline")
        .required(false);

    let force_arg = Arg::new("force")
        .value_parser(value_parser!(bool))
        .long("force")
        .action(ArgAction::SetTrue)
        .help("Force initialization even if the repository is not empty")
        .required(false);

    Command::new("Neon CLI")
        .arg_required_else_help(true)
        .version(GIT_VERSION)
        .subcommand(
            Command::new("init")
                .about("Initialize a new Neon repository, preparing configs for services to start with")
                .arg(pageserver_config_args.clone())
                .arg(
                    Arg::new("config")
                        .long("config")
                        .required(false)
                        .value_parser(value_parser!(PathBuf))
                        .value_name("config"),
                )
                .arg(pg_version_arg.clone())
                .arg(force_arg)
        )
        .subcommand(
            Command::new("timeline")
            .about("Manage timelines")
            .subcommand(Command::new("list")
                .about("List all timelines, available to this pageserver")
                .arg(tenant_id_arg.clone()))
            .subcommand(Command::new("branch")
                .about("Create a new timeline, using another timeline as a base, copying its data")
                .arg(tenant_id_arg.clone())
                .arg(branch_name_arg.clone())
                .arg(Arg::new("ancestor-branch-name").long("ancestor-branch-name")
                    .help("Use last Lsn of another timeline (and its data) as base when creating the new timeline. The timeline gets resolved by its branch name.").required(false))
                .arg(Arg::new("ancestor-start-lsn").long("ancestor-start-lsn")
                    .help("When using another timeline as base, use a specific Lsn in it instead of the latest one").required(false)))
            .subcommand(Command::new("create")
                .about("Create a new blank timeline")
                .arg(tenant_id_arg.clone())
                .arg(branch_name_arg.clone())
                .arg(pg_version_arg.clone())
            )
            .subcommand(Command::new("import")
                .about("Import timeline from basebackup directory")
                .arg(tenant_id_arg.clone())
                .arg(timeline_id_arg.clone())
                .arg(Arg::new("node-name").long("node-name")
                    .help("Name to assign to the imported timeline"))
                .arg(Arg::new("base-tarfile")
                    .long("base-tarfile")
                    .value_parser(value_parser!(PathBuf))
                    .help("Basebackup tarfile to import")
                )
                .arg(Arg::new("base-lsn").long("base-lsn")
                    .help("Lsn the basebackup starts at"))
                .arg(Arg::new("wal-tarfile")
                    .long("wal-tarfile")
                    .value_parser(value_parser!(PathBuf))
                    .help("Wal to add after base")
                )
                .arg(Arg::new("end-lsn").long("end-lsn")
                    .help("Lsn the basebackup ends at"))
                .arg(pg_version_arg.clone())
            )
        ).subcommand(
            Command::new("tenant")
            .arg_required_else_help(true)
            .about("Manage tenants")
            .subcommand(Command::new("list"))
            .subcommand(Command::new("create")
                .arg(tenant_id_arg.clone())
                .arg(timeline_id_arg.clone().help("Use a specific timeline id when creating a tenant and its initial timeline"))
                .arg(Arg::new("config").short('c').num_args(1).action(ArgAction::Append).required(false))
                .arg(pg_version_arg.clone())
                .arg(Arg::new("set-default").long("set-default").action(ArgAction::SetTrue).required(false)
                    .help("Use this tenant in future CLI commands where tenant_id is needed, but not specified"))
                )
            .subcommand(Command::new("set-default").arg(tenant_id_arg.clone().required(true))
                .about("Set a particular tenant as default in future CLI commands where tenant_id is needed, but not specified"))
            .subcommand(Command::new("config")
                .arg(tenant_id_arg.clone())
                .arg(Arg::new("config").short('c').num_args(1).action(ArgAction::Append).required(false)))
        )
        .subcommand(
            Command::new("pageserver")
                .arg_required_else_help(true)
                .about("Manage pageserver")
                .subcommand(Command::new("status"))
                .arg(pageserver_id_arg.clone())
                .subcommand(Command::new("start").about("Start local pageserver")
                .arg(pageserver_id_arg.clone())
                .arg(pageserver_config_args.clone()))
                .subcommand(Command::new("stop").about("Stop local pageserver")
                .arg(pageserver_id_arg.clone())
                            .arg(stop_mode_arg.clone()))
                .subcommand(Command::new("restart").about("Restart local pageserver")
                .arg(pageserver_id_arg.clone())
                .arg(pageserver_config_args.clone()))
        )
        .subcommand(
            Command::new("attachment_service")
                .arg_required_else_help(true)
                .about("Manage attachment_service")
                .subcommand(Command::new("start").about("Start local pageserver").arg(pageserver_config_args.clone()))
                .subcommand(Command::new("stop").about("Stop local pageserver")
                            .arg(stop_mode_arg.clone()))
        )
        .subcommand(
            Command::new("safekeeper")
                .arg_required_else_help(true)
                .about("Manage safekeepers")
                .subcommand(Command::new("start")
                            .about("Start local safekeeper")
                            .arg(safekeeper_id_arg.clone())
                            .arg(safekeeper_extra_opt_arg.clone())
                )
                .subcommand(Command::new("stop")
                            .about("Stop local safekeeper")
                            .arg(safekeeper_id_arg.clone())
                            .arg(stop_mode_arg.clone())
                )
                .subcommand(Command::new("restart")
                            .about("Restart local safekeeper")
                            .arg(safekeeper_id_arg)
                            .arg(stop_mode_arg.clone())
                            .arg(safekeeper_extra_opt_arg)
                )
        )
        .subcommand(
            Command::new("endpoint")
                .arg_required_else_help(true)
                .about("Manage postgres instances")
                .subcommand(Command::new("list").arg(tenant_id_arg.clone()))
                .subcommand(Command::new("create")
                    .about("Create a compute endpoint")
                    .arg(endpoint_id_arg.clone())
                    .arg(branch_name_arg.clone())
                    .arg(tenant_id_arg.clone())
                    .arg(lsn_arg.clone())
                    .arg(pg_port_arg.clone())
                    .arg(http_port_arg.clone())
                    .arg(endpoint_pageserver_id_arg.clone())
                    .arg(
                        Arg::new("config-only")
                            .help("Don't do basebackup, create endpoint directory with only config files")
                            .long("config-only")
                            .required(false))
                    .arg(pg_version_arg.clone())
                    .arg(hot_standby_arg.clone())
                )
                .subcommand(Command::new("start")
                    .about("Start postgres.\n If the endpoint doesn't exist yet, it is created.")
                    .arg(endpoint_id_arg.clone())
                    .arg(tenant_id_arg.clone())
                    .arg(branch_name_arg)
                    .arg(timeline_id_arg)
                    .arg(lsn_arg)
                    .arg(pg_port_arg)
                    .arg(http_port_arg)
                    .arg(endpoint_pageserver_id_arg.clone())
                    .arg(pg_version_arg)
                    .arg(hot_standby_arg)
                    .arg(safekeepers_arg)
                    .arg(remote_ext_config_args)
                )
                .subcommand(
                    Command::new("stop")
                    .arg(endpoint_id_arg)
                    .arg(tenant_id_arg)
                    .arg(
                        Arg::new("destroy")
                            .help("Also delete data directory (now optional, should be default in future)")
                            .long("destroy")
                            .action(ArgAction::SetTrue)
                            .required(false)
                        )
                )

        )
        // Obsolete old name for 'endpoint'. We now just print an error if it's used.
        .subcommand(
            Command::new("pg")
                .hide(true)
                .arg(Arg::new("ignore-rest").allow_hyphen_values(true).num_args(0..).required(false))
                .trailing_var_arg(true)
        )
        .subcommand(
            Command::new("start")
                .about("Start page server and safekeepers")
                .arg(pageserver_config_args)
        )
        .subcommand(
            Command::new("stop")
                .about("Stop page server and safekeepers")
                .arg(stop_mode_arg)
        )
}

#[test]
fn verify_cli() {
    cli().debug_assert();
}
