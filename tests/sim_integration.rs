use std::time::Duration;

use dst::sim::Sim;
use dst::sim::history::HistoryEvent;
use dst::{Builder, ClosureFilter, Error, FilterDecision, UdpSocket};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

#[test]
fn client_completes_immediately() {
    let mut sim = Builder::new()
        .rng_seed(1)
        .simulation_duration(Duration::from_secs(5))
        .build();

    sim.client("c1", async { Ok(()) }).unwrap();
    sim.run().unwrap();
    assert!(sim.steps() <= 2);
}

#[test]
fn client_sleeps_then_completes() {
    let mut sim = Builder::new()
        .rng_seed(7)
        .tick_duration(Duration::from_millis(1))
        .simulation_duration(Duration::from_secs(5))
        .build();

    sim.client("c1", async {
        tokio::time::sleep(Duration::from_millis(50)).await;
        Ok(())
    })
    .unwrap();

    sim.run().unwrap();
    assert!(sim.steps() >= 48);
    assert!(sim.steps() <= 55);
}

#[test]
fn multiple_clients_all_must_finish() {
    let mut sim = Builder::new()
        .rng_seed(7)
        .simulation_duration(Duration::from_secs(5))
        .build();

    sim.client("fast", async {
        tokio::time::sleep(Duration::from_millis(10)).await;
        Ok(())
    })
    .unwrap();

    sim.client("slow", async {
        tokio::time::sleep(Duration::from_millis(100)).await;
        Ok(())
    })
    .unwrap();

    sim.run().unwrap();
    assert!(sim.elapsed() >= Duration::from_millis(100));
}

#[test]
fn host_runs_indefinitely_client_drives_completion() {
    let mut sim = Builder::new()
        .rng_seed(11)
        .simulation_duration(Duration::from_secs(5))
        .build();

    sim.host("server", || async {
        loop {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        #[allow(unreachable_code)]
        Ok(())
    })
    .unwrap();

    sim.client("client", async {
        tokio::time::sleep(Duration::from_millis(50)).await;
        Ok(())
    })
    .unwrap();

    sim.run().unwrap();
}

#[test]
fn crash_stops_host() {
    let mut sim = Builder::new()
        .rng_seed(22)
        .simulation_duration(Duration::from_secs(5))
        .build();

    sim.host("server", || async {
        loop {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        #[allow(unreachable_code)]
        Ok(())
    })
    .unwrap();

    sim.client("client", async {
        tokio::time::sleep(Duration::from_millis(20)).await;
        Ok(())
    })
    .unwrap();

    for _ in 0..10 {
        sim.step().unwrap();
    }
    sim.crash("server");

    sim.run().unwrap();
}

#[test]
fn bounce_restarts_host() {
    let mut sim = Builder::new()
        .rng_seed(33)
        .simulation_duration(Duration::from_secs(5))
        .build();

    sim.host("server", || async {
        loop {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        #[allow(unreachable_code)]
        Ok(())
    })
    .unwrap();

    sim.client("client", async {
        tokio::time::sleep(Duration::from_millis(100)).await;
        Ok(())
    })
    .unwrap();

    for _ in 0..10 {
        sim.step().unwrap();
    }
    sim.crash("server");
    sim.bounce("server").unwrap();

    sim.run().unwrap();
}

#[test]
fn partition_and_repair() {
    let mut sim = Builder::new()
        .rng_seed(44)
        .simulation_duration(Duration::from_secs(5))
        .build();

    sim.host("a", || async {
        loop {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        #[allow(unreachable_code)]
        Ok(())
    })
    .unwrap();

    sim.host("b", || async {
        loop {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        #[allow(unreachable_code)]
        Ok(())
    })
    .unwrap();

    sim.client("client", async {
        tokio::time::sleep(Duration::from_millis(50)).await;
        Ok(())
    })
    .unwrap();

    sim.partition("a", "b");
    for _ in 0..20 {
        sim.step().unwrap();
    }
    sim.repair("a", "b");

    sim.run().unwrap();
}

#[test]
fn oneway_partition() {
    let mut sim = Builder::new()
        .rng_seed(55)
        .simulation_duration(Duration::from_secs(5))
        .build();

    sim.host("a", || async {
        loop {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        #[allow(unreachable_code)]
        Ok(())
    })
    .unwrap();

    sim.host("b", || async {
        loop {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        #[allow(unreachable_code)]
        Ok(())
    })
    .unwrap();

    sim.client("client", async {
        tokio::time::sleep(Duration::from_millis(20)).await;
        Ok(())
    })
    .unwrap();

    sim.partition_oneway("a", "b");
    assert!(
        !sim.network()
            .topology()
            .can_deliver(&"a".into(), &"b".into())
    );
    assert!(
        sim.network()
            .topology()
            .can_deliver(&"b".into(), &"a".into())
    );

    sim.repair_oneway("a", "b");
    assert!(
        sim.network()
            .topology()
            .can_deliver(&"a".into(), &"b".into())
    );

    sim.run().unwrap();
}

#[test]
fn hold_and_release() {
    let mut sim = Builder::new()
        .rng_seed(66)
        .simulation_duration(Duration::from_secs(5))
        .build();

    sim.host("a", || async {
        loop {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        #[allow(unreachable_code)]
        Ok(())
    })
    .unwrap();

    sim.host("b", || async {
        loop {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        #[allow(unreachable_code)]
        Ok(())
    })
    .unwrap();

    sim.client("client", async {
        tokio::time::sleep(Duration::from_millis(50)).await;
        Ok(())
    })
    .unwrap();

    sim.hold("a", "b");
    assert!(sim.network().topology().is_held(&"a".into(), &"b".into()));

    for _ in 0..10 {
        sim.step().unwrap();
    }

    sim.release("a", "b");
    assert!(!sim.network().topology().is_held(&"a".into(), &"b".into()));

    sim.run().unwrap();
}

#[test]
fn duplicate_node_rejected() {
    let mut sim = Builder::new().rng_seed(77).build();
    sim.host("server", || async { Ok(()) }).unwrap();
    let result = sim.host("server", || async { Ok(()) });
    assert!(result.is_err());
    match result.unwrap_err() {
        Error::DuplicateNode { name } => assert_eq!(name, "server"),
        other => panic!("expected DuplicateNode, got: {other}"),
    }
}

#[test]
fn duration_exceeded_error() {
    let mut sim = Builder::new()
        .rng_seed(88)
        .simulation_duration(Duration::from_millis(10))
        .tick_duration(Duration::from_millis(1))
        .build();

    sim.client("client", async {
        tokio::time::sleep(Duration::from_secs(1)).await;
        Ok(())
    })
    .unwrap();

    let result = sim.run();
    assert!(result.is_err());
    match result.unwrap_err() {
        Error::DurationExceeded { .. } => {}
        other => panic!("expected DurationExceeded, got: {other}"),
    }
}

#[test]
fn deterministic_same_seed() {
    use dst::harness::determinism::assert_same_seed_twice;

    fn run_sim(seed: u64) -> dst::sim::history::RunSummary {
        let mut sim = Builder::new()
            .rng_seed(seed)
            .simulation_duration(Duration::from_secs(5))
            .build();

        sim.client("c", async {
            tokio::time::sleep(Duration::from_millis(50)).await;
            Ok(())
        })
        .unwrap();

        let ok = sim.run().is_ok();
        sim.run_summary(ok)
    }

    assert_same_seed_twice(&run_sim(7), &run_sim(7));
}

/// F2: the seed contract proven empirically on a chaotic run — packet loss +
/// latency jitter + multiple peers + crash/bounce/partition faults — asserting
/// bit-for-bit identical `history_hash`, not just steps/elapsed.
#[test]
fn two_run_hash_equality_full_chaos() {
    use dst::UdpSocket;
    use dst::harness::determinism::assert_same_seed_twice;
    use std::net::SocketAddr;

    fn chaos(seed: u64) -> dst::sim::history::RunSummary {
        let mut sim = Builder::new()
            .rng_seed(seed)
            .tick_duration(Duration::from_millis(1))
            .min_message_latency(Duration::from_millis(5))
            .max_message_latency(Duration::from_millis(50))
            .message_loss_rate(0.1)
            .simulation_duration(Duration::from_secs(30))
            .build();

        sim.host("a", || async {
            let sock = UdpSocket::bind("0.0.0.0:7000".parse::<SocketAddr>().unwrap()).await?;
            let mut n: u32 = 0;
            loop {
                tokio::time::sleep(Duration::from_millis(10)).await;
                let _ = sock
                    .send_to(
                        &n.to_le_bytes(),
                        "192.168.0.2:7001".parse::<SocketAddr>().unwrap(),
                    )
                    .await;
                let _ = sock
                    .send_to(
                        &n.to_le_bytes(),
                        "192.168.0.3:7002".parse::<SocketAddr>().unwrap(),
                    )
                    .await;
                n = n.wrapping_add(1);
            }
        })
        .unwrap();

        for (name, port) in [("b", 7001u16), ("c", 7002u16)] {
            sim.host(name, move || async move {
                let sock =
                    UdpSocket::bind(format!("0.0.0.0:{port}").parse::<SocketAddr>().unwrap())
                        .await?;
                let mut buf = [0u8; 64];
                loop {
                    let _ = sock.recv_from(&mut buf).await;
                }
            })
            .unwrap();
        }

        sim.client("driver", async {
            tokio::time::sleep(Duration::from_millis(800)).await;
            Ok(())
        })
        .unwrap();

        for i in 0..2000u64 {
            if sim.step().unwrap() {
                break;
            }
            match i {
                200 => sim.partition("a", "b"),
                400 => sim.crash("c"),
                500 => {
                    let _ = sim.bounce("c");
                }
                700 => sim.repair("a", "b"),
                _ => {}
            }
        }
        let _ = sim.run();
        sim.run_summary(true)
    }

    assert_same_seed_twice(&chaos(7), &chaos(7));
}

/// F2: `verify_same_seed_twice` wired and exercised, env-gated by
/// `DST_CHECK_DETERMINISM` (skipped by default, runs the double-check when set).
#[test]
fn determinism_check_env_gated() {
    use dst::harness::determinism::{check_determinism_enabled, verify_same_seed_twice};

    if !check_determinism_enabled() {
        return;
    }
    verify_same_seed_twice(|| {
        let mut sim = Builder::new()
            .rng_seed(99)
            .simulation_duration(Duration::from_secs(5))
            .build();
        sim.client("c", async {
            tokio::time::sleep(Duration::from_millis(20)).await;
            Ok(())
        })
        .unwrap();
        let ok = sim.run().is_ok();
        sim.run_summary(ok)
    });
}

/// H1/C5: a fault pattern driven with a sim-seed-bound RNG (`sim.derive_rng`,
/// bound ONCE before the loop) is reproducible across same-seed runs; distinct
/// salts yield independent streams.
#[test]
fn rolling_restart_deterministic() {
    use dst::NodeName;
    use dst::harness::determinism::assert_same_seed_twice;
    use dst::patterns::RollingRestart;

    fn run(seed: u64, salt: &[u8]) -> dst::sim::history::RunSummary {
        let mut sim = Builder::new()
            .rng_seed(seed)
            .simulation_duration(Duration::from_secs(20))
            .build();
        for n in ["n1", "n2", "n3", "n4", "n5"] {
            sim.host(n, || async {
                loop {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .unwrap();
        }
        sim.client("driver", async {
            tokio::time::sleep(Duration::from_millis(800)).await;
            Ok(())
        })
        .unwrap();

        let nodes: Vec<NodeName> = ["n1", "n2", "n3", "n4", "n5"]
            .iter()
            .map(|s| (*s).into())
            .collect();
        let mut pat = RollingRestart::new(nodes, 3, 3);
        // C5: bind the derived RNG ONCE before the driver loop.
        let mut prng = sim.derive_rng(salt);
        for i in 0..4000u64 {
            if sim.step().unwrap() {
                break;
            }
            pat.tick(&mut sim, i, &mut prng).unwrap();
        }
        let _ = sim.run();
        sim.run_summary(true)
    }

    assert_same_seed_twice(&run(7, b"rolling_restart"), &run(7, b"rolling_restart"));

    // Independence: distinct salts derive distinct streams.
    use rand::RngCore;
    let s = Builder::new().rng_seed(7).build();
    let mut ra = s.derive_rng(b"salt-a");
    let mut rb = s.derive_rng(b"salt-b");
    assert_ne!(ra.next_u64(), rb.next_u64());
}

/// H1/C5: same for the swizzle-clog pattern (hold/release driven by a
/// sim-seed-bound RNG bound once before the loop).
#[test]
fn swizzle_clog_deterministic() {
    use dst::NodeName;
    use dst::harness::determinism::assert_same_seed_twice;
    use dst::patterns::RollingNetworkClog;

    fn run(seed: u64) -> dst::sim::history::RunSummary {
        let mut sim = Builder::new()
            .rng_seed(seed)
            .simulation_duration(Duration::from_secs(20))
            .build();
        for n in ["n1", "n2", "n3", "n4"] {
            sim.host(n, || async {
                loop {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .unwrap();
        }
        sim.client("driver", async {
            tokio::time::sleep(Duration::from_millis(800)).await;
            Ok(())
        })
        .unwrap();

        let nodes: Vec<NodeName> = ["n1", "n2", "n3", "n4"]
            .iter()
            .map(|s| (*s).into())
            .collect();
        let mut pat = RollingNetworkClog::new(nodes, 2, 3);
        let mut prng = sim.derive_rng(b"swizzle_clog");
        for i in 0..4000u64 {
            if sim.step().unwrap() {
                break;
            }
            pat.tick(&mut sim, i, &mut prng);
        }
        let _ = sim.run();
        sim.run_summary(true)
    }

    assert_same_seed_twice(&run(11), &run(11));
}

#[test]
fn banner_format() {
    use dst::harness::banner::format_banner;
    use dst::harness::scenario::Scenario;

    let scenario = Scenario::new(12345).with_label("test-run");
    let banner = format_banner(&scenario);
    assert!(banner.contains("12345"));
    assert!(banner.contains("test-run"));
}

#[test]
fn seed_sweep_basic() {
    use dst::harness::seed_sweep::run_seed_sweep;

    let table = run_seed_sweep(0..5, |seed| {
        let mut sim = Builder::new()
            .rng_seed(seed)
            .simulation_duration(Duration::from_secs(5))
            .build();

        sim.client("c", async {
            tokio::time::sleep(Duration::from_millis(10)).await;
            Ok(())
        })
        .unwrap();

        sim.run().map_err(|e| e.to_string())?;

        Ok(sim.run_summary(true))
    });

    assert_eq!(table.total(), 5);
    assert_eq!(table.passed(), 5);
    assert_eq!(table.failed(), 0);
}

#[test]
fn udp_send_recv() {
    use dst::UdpSocket;
    use std::net::SocketAddr;
    use std::sync::{Arc, Mutex};

    let mut sim = Builder::new()
        .rng_seed(100)
        .tick_duration(Duration::from_millis(1))
        .min_message_latency(Duration::from_millis(1))
        .max_message_latency(Duration::from_millis(5))
        .simulation_duration(Duration::from_secs(5))
        .build();

    let received: Arc<Mutex<Option<Vec<u8>>>> = Arc::new(Mutex::new(None));
    let received_server = Arc::clone(&received);

    sim.host("sender", || async {
        tokio::time::sleep(Duration::from_millis(5)).await;
        let sock = UdpSocket::bind("0.0.0.0:9001".parse::<SocketAddr>().unwrap()).await?;
        sock.send_to(
            b"hello-dst",
            "192.168.0.2:9002".parse::<SocketAddr>().unwrap(),
        )
        .await?;
        Ok(())
    })
    .unwrap();

    sim.host("receiver", move || {
        let rx = Arc::clone(&received_server);
        async move {
            let sock = UdpSocket::bind("0.0.0.0:9002".parse::<SocketAddr>().unwrap()).await?;
            let mut buf = [0u8; 64];
            let (len, _from) = sock.recv_from(&mut buf).await?;
            *rx.lock().unwrap() = Some(buf[..len].to_vec());
            Ok(())
        }
    })
    .unwrap();

    sim.client("done", async {
        tokio::time::sleep(Duration::from_millis(100)).await;
        Ok(())
    })
    .unwrap();

    sim.run().unwrap();
    assert_eq!(
        received.lock().unwrap().as_deref(),
        Some(b"hello-dst" as &[u8])
    );
}

#[test]
fn packet_filter_drops_by_content() {
    use dst::{ClosureFilter, FilterDecision, UdpSocket};
    use std::net::SocketAddr;
    use std::sync::{Arc, Mutex};

    let mut sim = Builder::new()
        .rng_seed(200)
        .tick_duration(Duration::from_millis(1))
        .min_message_latency(Duration::from_millis(1))
        .max_message_latency(Duration::from_millis(2))
        .simulation_duration(Duration::from_secs(5))
        .build();

    let received: Arc<Mutex<Vec<Vec<u8>>>> = Arc::new(Mutex::new(Vec::new()));
    let received_server = Arc::clone(&received);

    sim.host("sender", || async {
        tokio::time::sleep(Duration::from_millis(5)).await;
        let sock = UdpSocket::bind("0.0.0.0:9001".parse::<SocketAddr>().unwrap()).await?;
        let target: SocketAddr = "192.168.0.2:9002".parse().unwrap();
        sock.send_to(b"blocked", target).await?;
        sock.send_to(b"allowed", target).await?;
        Ok(())
    })
    .unwrap();

    sim.host("receiver", move || {
        let rx = Arc::clone(&received_server);
        async move {
            let sock = UdpSocket::bind("0.0.0.0:9002".parse::<SocketAddr>().unwrap()).await?;
            let mut buf = [0u8; 64];
            let (len, _) = sock.recv_from(&mut buf).await?;
            rx.lock().unwrap().push(buf[..len].to_vec());
            Ok(())
        }
    })
    .unwrap();

    sim.client("done", async {
        tokio::time::sleep(Duration::from_millis(200)).await;
        Ok(())
    })
    .unwrap();

    sim.add_packet_filter(Box::new(ClosureFilter::new("drop-blocked", |meta| {
        if meta.payload == b"blocked" {
            FilterDecision::Drop
        } else {
            FilterDecision::Pass
        }
    })));

    sim.run().unwrap();

    let msgs = received.lock().unwrap();
    assert_eq!(msgs.len(), 1, "expected exactly one message to arrive");
    assert_eq!(msgs[0], b"allowed");
}

#[test]
fn packet_filter_delay_adds_latency() {
    use dst::{ClosureFilter, FilterDecision, UdpSocket};
    use std::net::SocketAddr;
    use std::sync::{Arc, Mutex};

    let extra_delay = Duration::from_millis(50);

    let baseline_arrival: Arc<Mutex<Option<Duration>>> = Arc::new(Mutex::new(None));
    let delayed_arrival: Arc<Mutex<Option<Duration>>> = Arc::new(Mutex::new(None));

    {
        let arrival = Arc::clone(&baseline_arrival);
        let mut sim = Builder::new()
            .rng_seed(300)
            .tick_duration(Duration::from_millis(1))
            .min_message_latency(Duration::from_millis(1))
            .max_message_latency(Duration::from_millis(1))
            .simulation_duration(Duration::from_secs(5))
            .build();

        sim.host("sender", || async {
            tokio::time::sleep(Duration::from_millis(10)).await;
            let sock = UdpSocket::bind("0.0.0.0:9001".parse::<SocketAddr>().unwrap()).await?;
            sock.send_to(b"ping", "192.168.0.2:9002".parse::<SocketAddr>().unwrap())
                .await?;
            Ok(())
        })
        .unwrap();

        sim.host("receiver", move || {
            let a = Arc::clone(&arrival);
            async move {
                let start = tokio::time::Instant::now();
                let sock = UdpSocket::bind("0.0.0.0:9002".parse::<SocketAddr>().unwrap()).await?;
                let mut buf = [0u8; 16];
                sock.recv_from(&mut buf).await?;
                *a.lock().unwrap() = Some(start.elapsed());
                Ok(())
            }
        })
        .unwrap();

        sim.client("done", async {
            tokio::time::sleep(Duration::from_millis(200)).await;
            Ok(())
        })
        .unwrap();

        sim.run().unwrap();
    }

    {
        let arrival = Arc::clone(&delayed_arrival);
        let mut sim = Builder::new()
            .rng_seed(300)
            .tick_duration(Duration::from_millis(1))
            .min_message_latency(Duration::from_millis(1))
            .max_message_latency(Duration::from_millis(1))
            .simulation_duration(Duration::from_secs(5))
            .build();

        sim.host("sender", || async {
            tokio::time::sleep(Duration::from_millis(10)).await;
            let sock = UdpSocket::bind("0.0.0.0:9001".parse::<SocketAddr>().unwrap()).await?;
            sock.send_to(b"ping", "192.168.0.2:9002".parse::<SocketAddr>().unwrap())
                .await?;
            Ok(())
        })
        .unwrap();

        sim.host("receiver", move || {
            let a = Arc::clone(&arrival);
            async move {
                let start = tokio::time::Instant::now();
                let sock = UdpSocket::bind("0.0.0.0:9002".parse::<SocketAddr>().unwrap()).await?;
                let mut buf = [0u8; 16];
                sock.recv_from(&mut buf).await?;
                *a.lock().unwrap() = Some(start.elapsed());
                Ok(())
            }
        })
        .unwrap();

        sim.client("done", async {
            tokio::time::sleep(Duration::from_millis(200)).await;
            Ok(())
        })
        .unwrap();

        sim.add_packet_filter(Box::new(ClosureFilter::new("delay-all", move |_meta| {
            FilterDecision::Delay(extra_delay)
        })));

        sim.run().unwrap();
    }

    let base = baseline_arrival.lock().unwrap().unwrap();
    let delayed = delayed_arrival.lock().unwrap().unwrap();
    assert!(
        delayed >= base + extra_delay,
        "delayed arrival {delayed:?} should be at least {extra_delay:?} after baseline {base:?}"
    );
}

#[test]
fn history_records_packet_delivered() {
    use dst::UdpSocket;
    use dst::sim::history::HistoryEvent;
    use std::net::SocketAddr;

    let mut sim = Builder::new()
        .rng_seed(400)
        .tick_duration(Duration::from_millis(1))
        .min_message_latency(Duration::from_millis(1))
        .max_message_latency(Duration::from_millis(1))
        .simulation_duration(Duration::from_secs(5))
        .build();

    sim.host("sender", || async {
        tokio::time::sleep(Duration::from_millis(5)).await;
        let sock = UdpSocket::bind("0.0.0.0:9001".parse::<SocketAddr>().unwrap()).await?;
        sock.send_to(b"hi", "192.168.0.2:9002".parse::<SocketAddr>().unwrap())
            .await?;
        Ok(())
    })
    .unwrap();

    sim.host("receiver", || async {
        let sock = UdpSocket::bind("0.0.0.0:9002".parse::<SocketAddr>().unwrap()).await?;
        let mut buf = [0u8; 16];
        sock.recv_from(&mut buf).await?;
        Ok(())
    })
    .unwrap();

    sim.client("done", async {
        tokio::time::sleep(Duration::from_millis(100)).await;
        Ok(())
    })
    .unwrap();

    sim.run().unwrap();

    let delivered = sim
        .history()
        .events()
        .iter()
        .any(|e| matches!(e, HistoryEvent::PacketDelivered { .. }));
    assert!(
        delivered,
        "expected at least one PacketDelivered event in history"
    );
}

#[test]
fn history_records_packet_dropped_on_partition() {
    use dst::UdpSocket;
    use dst::sim::history::HistoryEvent;
    use std::net::SocketAddr;

    let mut sim = Builder::new()
        .rng_seed(500)
        .tick_duration(Duration::from_millis(1))
        .min_message_latency(Duration::from_millis(1))
        .max_message_latency(Duration::from_millis(1))
        .simulation_duration(Duration::from_secs(5))
        .build();

    sim.partition("sender", "receiver");

    sim.host("sender", || async {
        tokio::time::sleep(Duration::from_millis(5)).await;
        let sock = UdpSocket::bind("0.0.0.0:9001".parse::<SocketAddr>().unwrap()).await?;
        sock.send_to(b"hi", "192.168.0.2:9002".parse::<SocketAddr>().unwrap())
            .await?;
        Ok(())
    })
    .unwrap();

    sim.host("receiver", || async {
        let sock = UdpSocket::bind("0.0.0.0:9002".parse::<SocketAddr>().unwrap()).await?;
        let mut buf = [0u8; 16];
        let _ = tokio::time::timeout(Duration::from_millis(50), sock.recv_from(&mut buf)).await;
        Ok(())
    })
    .unwrap();

    sim.client("done", async {
        tokio::time::sleep(Duration::from_millis(100)).await;
        Ok(())
    })
    .unwrap();

    sim.run().unwrap();

    let dropped = sim
        .history()
        .events()
        .iter()
        .any(|e| matches!(e, HistoryEvent::PacketDropped { .. }));
    assert!(
        dropped,
        "expected at least one PacketDropped event in history"
    );
}

#[test]
fn history_records_inbox_overflow_drops() {
    use dst::UdpSocket;
    use dst::sim::history::HistoryEvent;
    use std::net::SocketAddr;

    let mut sim = Builder::new()
        .rng_seed(600)
        .tick_duration(Duration::from_millis(1))
        .min_message_latency(Duration::from_millis(1))
        .max_message_latency(Duration::from_millis(1))
        .simulation_duration(Duration::from_secs(5))
        .udp_capacity(2)
        .build();

    sim.host("sender", || async {
        tokio::time::sleep(Duration::from_millis(5)).await;
        let sock = UdpSocket::bind("0.0.0.0:9001".parse::<SocketAddr>().unwrap()).await?;
        let target: SocketAddr = "192.168.0.2:9002".parse().unwrap();
        for _ in 0..10 {
            sock.send_to(b"x", target).await?;
        }
        Ok(())
    })
    .unwrap();

    sim.host("receiver", || async {
        let _sock = UdpSocket::bind("0.0.0.0:9002".parse::<SocketAddr>().unwrap()).await?;
        std::future::pending::<()>().await;
        Ok(())
    })
    .unwrap();

    sim.client("done", async {
        tokio::time::sleep(Duration::from_millis(50)).await;
        Ok(())
    })
    .unwrap();

    sim.run().unwrap();

    let delivered = sim
        .history()
        .events()
        .iter()
        .filter(|e| matches!(e, HistoryEvent::PacketDelivered { .. }))
        .count();
    let inbox_full = sim
        .history()
        .events()
        .iter()
        .filter(|e| matches!(e, HistoryEvent::PacketDroppedInboxFull { .. }))
        .count();

    assert_eq!(delivered, 2, "expected exactly 2 PacketDelivered events");
    assert_eq!(
        inbox_full, 8,
        "expected exactly 8 PacketDroppedInboxFull events"
    );
}

#[test]
fn observer_on_step_end_is_called() {
    use dst::harness::observer::{Observer, StepStats};
    use std::cell::Cell;
    use std::rc::Rc;

    struct Counter {
        ticks: Rc<Cell<u64>>,
    }
    impl Observer for Counter {
        fn on_step_end(&mut self, _digest: &StepStats) -> Result<(), Error> {
            self.ticks.set(self.ticks.get() + 1);
            Ok(())
        }
    }

    let mut sim = Builder::new()
        .rng_seed(700)
        .tick_duration(Duration::from_millis(1))
        .simulation_duration(Duration::from_secs(5))
        .build();

    let ticks = Rc::new(Cell::new(0u64));
    sim.add_observer(Box::new(Counter {
        ticks: Rc::clone(&ticks),
    }));

    sim.client("c", async {
        tokio::time::sleep(Duration::from_millis(10)).await;
        Ok(())
    })
    .unwrap();

    sim.run().unwrap();
    assert!(
        ticks.get() >= 10,
        "observer should fire once per step; got {}",
        ticks.get()
    );
}

#[test]
fn observer_reports_fault_api_events_once_after_observer_error() {
    use dst::harness::observer::{Observer, StepStats};

    struct FailOnceRecorder {
        stats: Arc<Mutex<Vec<StepStats>>>,
        fail_next: bool,
    }

    impl Observer for FailOnceRecorder {
        fn on_step_end(&mut self, digest: &StepStats) -> Result<(), Error> {
            self.stats.lock().unwrap().push(digest.clone());
            if self.fail_next {
                self.fail_next = false;
                return Err(Error::NoProgress { steps: 1, limit: 1 });
            }
            Ok(())
        }
    }

    let mut sim = sharp_fault_sim(910);
    let received = Arc::new(Mutex::new(Vec::new()));
    let sent = Arc::new(AtomicBool::new(false));
    spawn_oneway_udp(
        &mut sim,
        Arc::clone(&received),
        Arc::clone(&sent),
        b"observer-reset",
    );
    step_until(&mut sim, || sent.load(Ordering::SeqCst));

    let stats = Arc::new(Mutex::new(Vec::new()));
    sim.add_observer(Box::new(FailOnceRecorder {
        stats: Arc::clone(&stats),
        fail_next: true,
    }));

    sim.partition("a", "b");
    assert!(matches!(
        sim.step(),
        Err(Error::NoProgress { steps: 1, limit: 1 })
    ));
    sim.step().unwrap();

    let stats = stats.lock().unwrap();
    assert_eq!(stats.len(), 2);
    assert_eq!(stats[0].events_since_last_observer, 2);
    assert_eq!(stats[0].faults_applied, 1);
    assert_eq!(stats[0].packets_dropped, 1);
    assert_eq!(stats[1].events_since_last_observer, 0);
    assert_eq!(stats[1].faults_applied, 0);
    assert_eq!(stats[1].packets_dropped, 0);
}

#[test]
fn progress_watchdog_fires() {
    use dst::harness::observer::ProgressWatchdog;

    let mut sim = Builder::new()
        .rng_seed(800)
        .tick_duration(Duration::from_millis(1))
        .simulation_duration(Duration::from_secs(5))
        .build();

    sim.add_observer(Box::new(ProgressWatchdog::new(5)));

    let mut hit = false;
    for _ in 0..20 {
        match sim.step() {
            Err(Error::NoProgress { .. }) => {
                hit = true;
                break;
            }
            Ok(_) => continue,
            Err(other) => panic!("unexpected error: {other}"),
        }
    }
    assert!(
        hit,
        "ProgressWatchdog should have fired Error::NoProgress within 20 steps"
    );
}

#[test]
fn udp_double_bind_rejected_while_first_alive() {
    use dst::UdpSocket;
    use std::net::SocketAddr;
    use std::sync::{Arc, Mutex};

    let mut sim = Builder::new()
        .rng_seed(101)
        .tick_duration(Duration::from_millis(1))
        .simulation_duration(Duration::from_secs(1))
        .build();

    let result: Arc<Mutex<Option<Result<(), String>>>> = Arc::new(Mutex::new(None));
    let result_node = Arc::clone(&result);

    sim.host("binder", move || {
        let out = Arc::clone(&result_node);
        async move {
            let addr = "0.0.0.0:9100".parse::<SocketAddr>().unwrap();
            let _first = UdpSocket::bind(addr).await?;

            let second = UdpSocket::bind(addr).await;
            let outcome = match second {
                Err(Error::Io(msg)) if msg.contains("address already in use") => Ok(()),
                Err(other) => Err(format!(
                    "expected Io(address already in use), got: {other:?}"
                )),
                Ok(_) => Err("second bind unexpectedly succeeded".to_string()),
            };
            *out.lock().unwrap() = Some(outcome);
            Ok(())
        }
    })
    .unwrap();

    sim.client("done", async {
        tokio::time::sleep(Duration::from_millis(10)).await;
        Ok(())
    })
    .unwrap();

    sim.run().unwrap();
    let outcome = result
        .lock()
        .unwrap()
        .take()
        .expect("binder host did not record an outcome");
    outcome.unwrap();
}

#[test]
fn udp_rebind_succeeds_after_drop() {
    use dst::UdpSocket;
    use std::net::SocketAddr;
    use std::sync::{Arc, Mutex};

    let mut sim = Builder::new()
        .rng_seed(102)
        .tick_duration(Duration::from_millis(1))
        .simulation_duration(Duration::from_secs(1))
        .build();

    let result: Arc<Mutex<Option<Result<(), String>>>> = Arc::new(Mutex::new(None));
    let result_node = Arc::clone(&result);

    sim.host("rebinder", move || {
        let out = Arc::clone(&result_node);
        async move {
            let addr = "0.0.0.0:9200".parse::<SocketAddr>().unwrap();
            {
                let _first = UdpSocket::bind(addr).await?;
            }
            tokio::task::yield_now().await;

            let outcome = match UdpSocket::bind(addr).await {
                Ok(_second) => Ok(()),
                Err(e) => Err(format!("rebind after drop should succeed, got: {e:?}")),
            };
            *out.lock().unwrap() = Some(outcome);
            Ok(())
        }
    })
    .unwrap();

    sim.client("done", async {
        tokio::time::sleep(Duration::from_millis(10)).await;
        Ok(())
    })
    .unwrap();

    sim.run().unwrap();
    let outcome = result
        .lock()
        .unwrap()
        .take()
        .expect("rebinder host did not record an outcome");
    outcome.unwrap();
}

fn sharp_fault_sim(seed: u64) -> Sim {
    Builder::new()
        .rng_seed(seed)
        .tick_duration(Duration::from_millis(1))
        .min_message_latency(Duration::from_millis(50))
        .max_message_latency(Duration::from_millis(50))
        .simulation_duration(Duration::from_secs(1))
        .build()
}

fn step_until(sim: &mut Sim, condition: impl Fn() -> bool) {
    for _ in 0..200 {
        if condition() {
            return;
        }
        sim.step().unwrap();
    }
    panic!("condition was not reached before step limit");
}

fn run_until(sim: &mut Sim, condition: impl Fn() -> bool) {
    for _ in 0..300 {
        if condition() {
            return;
        }
        if sim.step().unwrap() {
            break;
        }
    }
}

fn delivered_seqs(sim: &Sim) -> Vec<u64> {
    sim.history()
        .events()
        .iter()
        .filter_map(|event| match event {
            HistoryEvent::PacketDelivered { seq } => Some(*seq),
            _ => None,
        })
        .collect()
}

fn dropped_seqs(sim: &Sim) -> Vec<u64> {
    sim.history()
        .events()
        .iter()
        .filter_map(|event| match event {
            HistoryEvent::PacketDropped { seq } => Some(*seq),
            _ => None,
        })
        .collect()
}

fn spawn_oneway_udp(
    sim: &mut Sim,
    received: Arc<Mutex<Vec<Vec<u8>>>>,
    sent: Arc<AtomicBool>,
    payload: &'static [u8],
) {
    sim.host("a", move || {
        let sent = Arc::clone(&sent);
        async move {
            tokio::time::sleep(Duration::from_millis(5)).await;
            let sock = UdpSocket::bind("0.0.0.0:9001".parse::<SocketAddr>().unwrap()).await?;
            sock.send_to(payload, "192.168.0.2:9002".parse::<SocketAddr>().unwrap())
                .await?;
            sent.store(true, Ordering::SeqCst);
            Ok(())
        }
    })
    .unwrap();

    sim.host("b", move || {
        let received = Arc::clone(&received);
        async move {
            let sock = UdpSocket::bind("0.0.0.0:9002".parse::<SocketAddr>().unwrap()).await?;
            let mut buf = [0u8; 64];
            if let Ok(Ok((len, _))) =
                tokio::time::timeout(Duration::from_millis(250), sock.recv_from(&mut buf)).await
            {
                received.lock().unwrap().push(buf[..len].to_vec());
            }
            Ok(())
        }
    })
    .unwrap();

    sim.client("done", async {
        tokio::time::sleep(Duration::from_millis(300)).await;
        Ok(())
    })
    .unwrap();
}

#[test]
fn partition_after_send_drops_inflight() {
    let mut sim = sharp_fault_sim(900);
    let received = Arc::new(Mutex::new(Vec::new()));
    let sent = Arc::new(AtomicBool::new(false));
    spawn_oneway_udp(
        &mut sim,
        Arc::clone(&received),
        Arc::clone(&sent),
        b"partition",
    );

    step_until(&mut sim, || sent.load(Ordering::SeqCst));
    sim.partition("a", "b");
    sim.run().unwrap();

    assert!(received.lock().unwrap().is_empty());
    assert_eq!(dropped_seqs(&sim), vec![0]);
    assert!(delivered_seqs(&sim).is_empty());
}

#[test]
fn repair_before_original_delivery_does_not_resurrect_dropped_packet() {
    let mut sim = sharp_fault_sim(901);
    let received = Arc::new(Mutex::new(Vec::new()));
    let sent = Arc::new(AtomicBool::new(false));
    spawn_oneway_udp(
        &mut sim,
        Arc::clone(&received),
        Arc::clone(&sent),
        b"repair",
    );

    step_until(&mut sim, || sent.load(Ordering::SeqCst));
    sim.partition("a", "b");
    for _ in 0..10 {
        sim.step().unwrap();
    }
    sim.repair("a", "b");
    sim.run().unwrap();

    assert!(received.lock().unwrap().is_empty());
    assert_eq!(dropped_seqs(&sim), vec![0]);
}

#[test]
fn oneway_partition_after_send_drops_only_blocked_direction() {
    let mut sim = sharp_fault_sim(902);
    let sent = Arc::new(AtomicUsize::new(0));
    let a_received = Arc::new(Mutex::new(Vec::new()));
    let b_received = Arc::new(Mutex::new(Vec::new()));

    sim.host("a", {
        let sent = Arc::clone(&sent);
        let a_received = Arc::clone(&a_received);
        move || {
            let sent = Arc::clone(&sent);
            let a_received = Arc::clone(&a_received);
            async move {
                let sock = UdpSocket::bind("0.0.0.0:9001".parse::<SocketAddr>().unwrap()).await?;
                tokio::time::sleep(Duration::from_millis(5)).await;
                sock.send_to(b"a-to-b", "192.168.0.2:9002".parse::<SocketAddr>().unwrap())
                    .await?;
                sent.fetch_add(1, Ordering::SeqCst);
                let mut buf = [0u8; 64];
                if let Ok(Ok((len, _))) =
                    tokio::time::timeout(Duration::from_millis(250), sock.recv_from(&mut buf)).await
                {
                    a_received.lock().unwrap().push(buf[..len].to_vec());
                }
                Ok(())
            }
        }
    })
    .unwrap();

    sim.host("b", {
        let sent = Arc::clone(&sent);
        let b_received = Arc::clone(&b_received);
        move || {
            let sent = Arc::clone(&sent);
            let b_received = Arc::clone(&b_received);
            async move {
                let sock = UdpSocket::bind("0.0.0.0:9002".parse::<SocketAddr>().unwrap()).await?;
                tokio::time::sleep(Duration::from_millis(5)).await;
                sock.send_to(b"b-to-a", "192.168.0.1:9001".parse::<SocketAddr>().unwrap())
                    .await?;
                sent.fetch_add(1, Ordering::SeqCst);
                let mut buf = [0u8; 64];
                if let Ok(Ok((len, _))) =
                    tokio::time::timeout(Duration::from_millis(250), sock.recv_from(&mut buf)).await
                {
                    b_received.lock().unwrap().push(buf[..len].to_vec());
                }
                Ok(())
            }
        }
    })
    .unwrap();

    sim.client("done", async {
        tokio::time::sleep(Duration::from_millis(300)).await;
        Ok(())
    })
    .unwrap();

    step_until(&mut sim, || sent.load(Ordering::SeqCst) == 2);
    sim.partition_oneway("a", "b");
    sim.run().unwrap();

    assert_eq!(a_received.lock().unwrap().as_slice(), &[b"b-to-a".to_vec()]);
    assert!(b_received.lock().unwrap().is_empty());
    assert_eq!(dropped_seqs(&sim), vec![0]);
    assert_eq!(delivered_seqs(&sim), vec![1]);
}

#[test]
fn hold_after_send_captures_inflight_until_release() {
    let mut sim = sharp_fault_sim(903);
    let received = Arc::new(Mutex::new(Vec::new()));
    let sent = Arc::new(AtomicBool::new(false));
    spawn_oneway_udp(&mut sim, Arc::clone(&received), Arc::clone(&sent), b"hold");

    step_until(&mut sim, || sent.load(Ordering::SeqCst));
    sim.hold("a", "b");
    for _ in 0..70 {
        sim.step().unwrap();
    }
    assert!(received.lock().unwrap().is_empty());

    sim.release("a", "b");
    run_until(&mut sim, || !received.lock().unwrap().is_empty());

    assert_eq!(received.lock().unwrap().as_slice(), &[b"hold".to_vec()]);
    assert_eq!(delivered_seqs(&sim), vec![0]);
}

#[test]
fn hold_release_preserves_packet_identity_and_does_not_rerun_filter() {
    let mut sim = sharp_fault_sim(904);
    let received = Arc::new(Mutex::new(Vec::new()));
    let sent = Arc::new(AtomicBool::new(false));
    spawn_oneway_udp(
        &mut sim,
        Arc::clone(&received),
        Arc::clone(&sent),
        b"admitted-before-filter",
    );

    step_until(&mut sim, || sent.load(Ordering::SeqCst));
    sim.hold("a", "b");
    sim.add_packet_filter(Box::new(ClosureFilter::new(
        "drop-all-after-hold",
        |_meta| FilterDecision::Drop,
    )));
    sim.release("a", "b");
    sim.run().unwrap();

    assert_eq!(
        received.lock().unwrap().as_slice(),
        &[b"admitted-before-filter".to_vec()]
    );
    assert_eq!(delivered_seqs(&sim), vec![0]);
    assert!(dropped_seqs(&sim).is_empty());
}

#[test]
fn release_before_original_delivery_keeps_original_due_time() {
    let mut sim = sharp_fault_sim(905);
    let received = Arc::new(Mutex::new(Vec::new()));
    let sent = Arc::new(AtomicBool::new(false));
    spawn_oneway_udp(
        &mut sim,
        Arc::clone(&received),
        Arc::clone(&sent),
        b"before-due",
    );

    step_until(&mut sim, || sent.load(Ordering::SeqCst));
    sim.hold("a", "b");
    for _ in 0..10 {
        sim.step().unwrap();
    }
    sim.release("a", "b");
    for _ in 0..20 {
        sim.step().unwrap();
    }
    assert!(received.lock().unwrap().is_empty());

    run_until(&mut sim, || !received.lock().unwrap().is_empty());
    assert_eq!(
        received.lock().unwrap().as_slice(),
        &[b"before-due".to_vec()]
    );
}

#[test]
fn release_after_original_delivery_makes_packet_due() {
    let mut sim = sharp_fault_sim(906);
    let received = Arc::new(Mutex::new(Vec::new()));
    let sent = Arc::new(AtomicBool::new(false));
    spawn_oneway_udp(
        &mut sim,
        Arc::clone(&received),
        Arc::clone(&sent),
        b"after-due",
    );

    step_until(&mut sim, || sent.load(Ordering::SeqCst));
    sim.hold("a", "b");
    for _ in 0..70 {
        sim.step().unwrap();
    }
    assert!(received.lock().unwrap().is_empty());

    sim.release("a", "b");
    sim.step().unwrap();
    assert_eq!(
        received.lock().unwrap().as_slice(),
        &[b"after-due".to_vec()]
    );
}

#[test]
fn release_after_due_preserves_order_deterministically() {
    use dst::harness::determinism::assert_same_seed_twice;

    fn run(seed: u64) -> dst::sim::history::RunSummary {
        let mut sim = sharp_fault_sim(seed);
        let received = Arc::new(Mutex::new(Vec::new()));
        let sent = Arc::new(AtomicBool::new(false));
        spawn_oneway_udp(&mut sim, Arc::clone(&received), Arc::clone(&sent), b"ord");

        step_until(&mut sim, || sent.load(Ordering::SeqCst));
        sim.hold("a", "b");
        for _ in 0..70 {
            sim.step().unwrap();
        }
        // Released well after the original 50ms due time: must deliver on the
        // immediate next pass at its preserved (past) deliver_at, not be
        // rewritten to `now`.
        sim.release("a", "b");
        sim.step().unwrap();
        assert_eq!(received.lock().unwrap().as_slice(), &[b"ord".to_vec()]);
        let ok = sim.run().is_ok();
        sim.run_summary(ok)
    }

    assert_same_seed_twice(&run(951), &run(951));
}

#[test]
fn crash_receiver_after_send_drops_inflight() {
    let mut sim = sharp_fault_sim(907);
    let received = Arc::new(Mutex::new(Vec::new()));
    let sent = Arc::new(AtomicBool::new(false));
    spawn_oneway_udp(
        &mut sim,
        Arc::clone(&received),
        Arc::clone(&sent),
        b"crash-rx",
    );

    step_until(&mut sim, || sent.load(Ordering::SeqCst));
    sim.crash("b");
    sim.run().unwrap();

    assert!(received.lock().unwrap().is_empty());
    assert_eq!(dropped_seqs(&sim), vec![0]);
}

#[test]
fn crash_sender_after_send_drops_inflight() {
    let mut sim = sharp_fault_sim(908);
    let received = Arc::new(Mutex::new(Vec::new()));
    let sent = Arc::new(AtomicBool::new(false));
    spawn_oneway_udp(
        &mut sim,
        Arc::clone(&received),
        Arc::clone(&sent),
        b"crash-tx",
    );

    step_until(&mut sim, || sent.load(Ordering::SeqCst));
    sim.crash("a");
    sim.run().unwrap();

    assert!(received.lock().unwrap().is_empty());
    assert_eq!(dropped_seqs(&sim), vec![0]);
}

#[test]
fn crash_then_bounce_before_original_delivery_does_not_resurrect_packet() {
    let mut sim = sharp_fault_sim(909);
    let received = Arc::new(Mutex::new(Vec::new()));
    let sent = Arc::new(AtomicBool::new(false));
    spawn_oneway_udp(
        &mut sim,
        Arc::clone(&received),
        Arc::clone(&sent),
        b"bounce",
    );

    step_until(&mut sim, || sent.load(Ordering::SeqCst));
    sim.crash("b");
    for _ in 0..10 {
        sim.step().unwrap();
    }
    sim.bounce("b").unwrap();
    sim.run().unwrap();

    assert!(received.lock().unwrap().is_empty());
    assert_eq!(dropped_seqs(&sim), vec![0]);
}

#[test]
fn sut_select_branch_order_deterministic() {
    fn run_once(seed: u64) -> Vec<u8> {
        let winners = Arc::new(Mutex::new(Vec::<u8>::new()));
        let w = Arc::clone(&winners);
        let mut sim = Builder::new()
            .rng_seed(seed)
            .simulation_duration(Duration::from_secs(5))
            .build();
        sim.client("c", async move {
            for _ in 0..64 {
                // Both branches are immediately ready, so the winner is purely
                // the select! branch-poll order (Tokio FastRand).
                tokio::select! {
                    _ = std::future::ready(()) => w.lock().unwrap().push(0),
                    _ = std::future::ready(()) => w.lock().unwrap().push(1),
                }
                tokio::task::yield_now().await;
            }
            Ok(())
        })
        .unwrap();
        sim.run().unwrap();
        winners.lock().unwrap().clone()
    }

    #[cfg(all(feature = "tokio-rng-seed", tokio_unstable))]
    {
        let runs: Vec<Vec<u8>> = (0..5).map(|_| run_once(42)).collect();
        for r in &runs[1..] {
            assert_eq!(
                &runs[0], r,
                "select! branch order diverged across same-seed runs despite tokio-rng-seed"
            );
        }
        // Guard against a vacuous test: both branches must actually occur, else
        // the select! isn't exercising the RNG.
        assert!(
            runs[0].contains(&0) && runs[0].contains(&1),
            "test ineffective: select! never picked both branches ({:?})",
            runs[0]
        );
    }
    #[cfg(not(all(feature = "tokio-rng-seed", tokio_unstable)))]
    {
        let v = run_once(42);
        assert_eq!(v.len(), 64);
    }
}
