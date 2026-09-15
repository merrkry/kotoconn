use anyhow::{Context, Result, ensure};
use std::str::FromStr;

/// Disjoint cyclic client port sets keep the generator out of the proxy's
/// ephemeral allocation budget while bounding the number of retained sessions.
#[derive(Clone, Copy, Debug)]
pub(crate) struct SourcePorts {
    first: u16,
    last: u16,
}

impl FromStr for SourcePorts {
    type Err = anyhow::Error;

    fn from_str(value: &str) -> Result<Self> {
        let (first, last) = value.split_once('-').context("expected START-END ports")?;
        let first = first.parse()?;
        let last = last.parse()?;

        ensure!(first != 0 && first <= last, "invalid UDP source port range");
        Ok(Self { first, last })
    }
}

impl SourcePorts {
    pub(crate) fn validate(self, connections: usize) -> Result<()> {
        // SAFETY: FromStr is the only constructor and rejects zero/reversed ranges.
        debug_assert!(self.first != 0 && self.first <= self.last);
        let count = usize::from(self.last) - usize::from(self.first) + 1;
        ensure!(
            connections > 0 && count / connections >= 2 && count.is_multiple_of(connections),
            "UDP source ports must divide evenly into at least two ports per flow"
        );
        Ok(())
    }

    pub(crate) fn partition(self, connections: usize, flow: u64) -> Result<PortCycle> {
        self.validate(connections)?;
        ensure!(
            flow < connections as u64,
            "UDP flow index exceeds connection count"
        );

        let count = (u64::from(self.last) - u64::from(self.first) + 1) / connections as u64;
        let first = u64::from(self.first) + flow * count;
        debug_assert!(first + count - 1 <= u64::from(self.last));
        Ok(PortCycle {
            first: u16::try_from(first)?,
            count: u16::try_from(count)?,
        })
    }
}

/// A validated flow-local partition. Socket replacement binds the next port
/// before dropping the previous socket, so each cycle needs at least two ports.
#[derive(Clone, Copy, Debug)]
pub(crate) struct PortCycle {
    first: u16,
    count: u16,
}

impl PortCycle {
    pub(crate) fn port(self, sequence: u64) -> u16 {
        // SAFETY: SourcePorts::partition constructs nonempty, in-range partitions.
        // Reduce the sequence before addition so even u64::MAX cannot overflow.
        debug_assert!(self.count >= 2);
        debug_assert!(u32::from(self.first) + u32::from(self.count) - 1 <= u32::from(u16::MAX));
        self.first + (sequence % u64::from(self.count)) as u16
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn source_port_cycles_stay_disjoint_when_flows_advance_independently() {
        let ports: SourcePorts = "50000-50063".parse().unwrap();
        ports.validate(8).unwrap();
        let mut all = std::collections::HashSet::new();
        for flow in 0..8 {
            let cycle = ports.partition(8, flow).unwrap();
            let mut used = std::collections::HashSet::new();
            for sequence in 0..512 {
                let port = cycle.port(sequence);
                assert!((50000..=50063).contains(&port));
                assert_ne!(port, cycle.port(sequence + 1));
                used.insert(port);
            }
            assert_eq!(used.len(), 8);
            for port in used {
                assert!(all.insert(port), "two flows own the same source port");
            }
        }
        assert_eq!(all.len(), 64);

        let edge: SourcePorts = "65520-65535".parse().unwrap();
        edge.validate(8).unwrap();
        assert_eq!(edge.partition(8, 7).unwrap().port(u64::MAX), 65535);
    }

    #[test]
    fn source_port_configuration_rejects_unusable_partitions() {
        for value in ["0-63", "65535-65534", "65536-65537", "50000", "a-b"] {
            assert!(value.parse::<SourcePorts>().is_err(), "{value}");
        }
        let ports: SourcePorts = "50000-50063".parse().unwrap();
        for connections in [0, 3, 33, 65] {
            assert!(ports.validate(connections).is_err());
        }
    }

    #[test]
    fn partitions_validate_before_use() {
        let ports: SourcePorts = "1-65535".parse().unwrap();
        assert!(ports.partition(0, 0).is_err());
        assert!(ports.partition(3, 3).is_err());
        assert!(ports.partition(3, u64::MAX).is_err());
        assert_eq!(ports.partition(3, 0).unwrap().port(0), 1);
        assert_eq!(ports.partition(3, 2).unwrap().port(21844), 65535);
        assert_eq!(ports.partition(3, 2).unwrap().port(21845), 43691);
        assert!(
            "65535-65535"
                .parse::<SourcePorts>()
                .unwrap()
                .partition(1, 0)
                .is_err()
        );
    }
}
