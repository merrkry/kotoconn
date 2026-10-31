use kotoconn_config::ResolveHandlerId;
use kotoconn_script::Script;
use std::{collections::HashMap, time::Duration};

const TIMEOUT: Duration = Duration::from_secs(3);

async fn load(source: &str) -> (Script, ResolveHandlerId) {
    let script = Script::load(
        "main.ts",
        HashMap::from([("main.ts".into(), source.into())]),
    )
    .await
    .unwrap();
    let config = script.config().await.unwrap();
    let id = *config.resolve_handlers.iter().next().unwrap();

    (script, id)
}

// Policies record events in `log`; their resolve handler fails unless `log` matches `name`.
const CHECK_LOG: &str = "if (log.join('|') !== name) throw Error(`log: ${log.join('|')}`);";

#[tokio::test(start_paused = true)]
async fn sleep_waits_for_its_timeout_and_rejects_once_shutdown_starts() {
    let (script, id) = load(&format!(
        r#"
        import {{ kotoconn as k }} from '@kotoconn/bindings';
        const log = [];
        k.sleep(k.timeout(60000)).then(() => log.push('slept'));
        k.sleep(k.timeout(3600000)).then(() => log.push('woke'), () => log.push('pending rejected'));
        k.resolve_handler(async name => {{
            if (name === 'late') {{
                await k.sleep(k.timeout(60000)).then(() => log.push('late slept'), () => log.push('late rejected'));
                return [];
            }}
            {CHECK_LOG}
            return [];
        }});
    "#
    ))
    .await;

    // Paused time advances only when nothing else can run, so the driver handles the first wake-up.
    tokio::select! {
        _ = script.drive() => unreachable!("driver lives while its runtime exists"),
        _ = tokio::time::sleep(Duration::from_secs(61)) => {}
    }
    script.resolve(id, "slept").await.unwrap();

    script.shutdown().await.unwrap();
    script.idle().await;
    script.resolve(id, "late").await.unwrap();
    script
        .resolve(id, "slept|pending rejected|late rejected")
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_hooks_run_newest_first_one_at_a_time_despite_failures() {
    let (script, id) = load(&format!(
        r#"
        import {{ kotoconn as k }} from '@kotoconn/bindings';
        const log = [];
        k.on_shutdown(() => {{ log.push('first'); }});
        k.on_shutdown(async () => {{ throw Error('rejected hook'); }});
        k.on_shutdown(() => {{ throw Error('thrown hook'); }});
        k.on_shutdown(async () => {{
            const result = await k.exec(['sh', '-c', 'cat; echo diagnostic >&2; exit 3'], {{ input: 'input' }});
            await k.sleep(k.timeout(0)).catch(() => log.push('sleep rejected'));
            log.push(`${{result.status}} ${{result.signal}} ${{result.stdout}} ${{result.stderr.trim()}}`);
        }});
        k.resolve_handler(name => {{
            if (name === 'register') k.on_shutdown(() => {{ log.push('registered later'); }});
            else if (name === 'register again') k.on_shutdown(() => {{}});
            else {CHECK_LOG}
            return [];
        }});
    "#
    ))
    .await;

    script.resolve(id, "register").await.unwrap();
    tokio::time::timeout(TIMEOUT, script.shutdown())
        .await
        .unwrap()
        .unwrap();

    script
        .resolve(
            id,
            "registered later|sleep rejected|3 undefined input diagnostic|first",
        )
        .await
        .unwrap();
    let error = script.resolve(id, "register again").await.unwrap_err();
    assert!(
        error
            .to_string()
            .contains("shutdown hooks have already run"),
        "{error}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn exec_reports_results_and_rejects_failures() {
    let directory = tempfile::tempdir().unwrap();
    let pid_file = directory.path().join("pid");
    let (script, id) = load(&format!(
        r#"
        import {{ kotoconn as k }} from '@kotoconn/bindings';
        const log = [];
        const rejects = async (promise, message) => {{
            try {{ await promise; }} catch (error) {{
                if (String(error).includes(message)) return log.push(message);
                throw error;
            }}
            throw Error(`expected rejection: ${{message}}`);
        }};
        k.resolve_handler(async name => {{
            if (name !== 'run') {{ {CHECK_LOG} return []; }}

            const empty = await k.exec(['cat']);
            log.push(`${{empty.status}} ${{empty.signal}} [${{empty.stdout}}]`);
            const killed = await k.exec(['sh', '-c', 'kill -9 $$']);
            log.push(`${{killed.status}} ${{killed.signal}}`);
            const lossy = await k.exec(['printf', '\\377']);
            log.push(lossy.stdout === '\uFFFD' ? 'lossy' : `bytes ${{lossy.stdout}}`);

            await rejects(k.exec([]), 'needs a program');
            await rejects(k.exec(['/nonexistent/kotoconn-program']), 'spawn');
            await rejects(k.exec(
                ['sh', '-c', 'sleep 1000 & echo $! > "$1"; wait', 'sh', {pid_file:?}],
                {{ timeout: k.timeout(200) }},
            ), 'timed out');
            return [];
        }});
    "#,
        pid_file = pid_file.to_str().unwrap(),
    ))
    .await;

    tokio::time::timeout(TIMEOUT, script.resolve(id, "run"))
        .await
        .unwrap()
        .unwrap();
    script
        .resolve(
            id,
            "0 undefined []|undefined 9|lossy|needs a program|spawn|timed out",
        )
        .await
        .unwrap();

    // The timeout kills the shell's background child too. It can fire before the shell
    // records the child's PID; then nothing is left to observe.
    if let Some(pid) = std::fs::read_to_string(&pid_file)
        .ok()
        .and_then(|pid| pid.trim().parse::<u32>().ok())
    {
        tokio::time::timeout(TIMEOUT, exited(pid)).await.unwrap();
    }
}

// SIGKILL delivery is asynchronous, and the child may stay a zombie until it is reaped.
async fn exited(pid: u32) {
    loop {
        match std::fs::read_to_string(format!("/proc/{pid}/stat")) {
            Err(_) => return,
            Ok(stat)
                if stat
                    .rsplit_once(") ")
                    .is_some_and(|(_, rest)| rest.starts_with('Z')) =>
            {
                return;
            }
            Ok(_) => tokio::time::sleep(Duration::from_millis(10)).await,
        }
    }
}
