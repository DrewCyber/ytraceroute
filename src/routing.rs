//! Replication of ironwood's per-hop forwarding decision (`_lookup` in
//! ironwood/network/router.go): greedy routing in the spanning-tree metric.
//!
//! At each hop the node picks, among ALL direct peers that are strictly
//! closer (in treespace) to the destination than itself, the link minimizing
//! `cost * dist`, with tie-breaks (in order): lower link priority for the
//! same key, lower `cost*dist`, lower `dist`, lower `cost`, oldest link.
//! We model one link per (key) with no priority/order information, so the
//! last two tie-breaks degenerate to a deterministic lowest-key choice.

use crate::ygg::{tree_dist, Coords, Key};


/// A candidate next hop: a peer of the current node.
#[derive(Debug, Clone)]
pub struct Candidate {
    pub key: Key,
    /// Link cost in whole milliseconds, min 1 (ironwood `_getCost`).
    /// Unknown remote links use the minimum of 1.
    pub cost: u64,
    /// Treespace distance from this peer to the destination.
    pub dist: u64,
}

/// Build candidates for the current hop: all peers strictly closer to `dest`
/// than `cur` is. Mirrors the candidate-collection loop of `_lookup`.
pub fn candidates(
    cur: &Coords,
    dest: &Coords,
    peers: &[(Key, Coords, u64)],
) -> (u64, Vec<Candidate>) {
    let d_self = tree_dist(cur, dest);
    let mut cands: Vec<Candidate> = peers
        .iter()
        .filter(|(k, _c, _)| k.0 != [0u8; 32]) // defensive: skip empty keys
        .map(|(k, c, cost)| Candidate {
            key: *k,
            cost: (*cost).max(1),
            dist: tree_dist(c, dest),
        })
        .filter(|c| c.dist < d_self)
        .collect();
    // Deterministic order stands in for Go's random map iteration; the argmin
    // below is stable so equal candidates resolve to the lowest key.
    cands.sort_by_key(|a| a.key);
    (d_self, cands)
}

/// Pick the next hop among candidates using ironwood's tie-break chain.
pub fn select_next(cands: &[Candidate]) -> Option<&Candidate> {
    let mut best: Option<&Candidate> = None;
    let (mut best_cost, mut best_dist) = (0u64, 0u64);
    for c in cands {
        match best {
            None => {
                best = Some(c);
                best_cost = c.cost;
                best_dist = c.dist;
            }
            Some(_) => {
                // Same-key lower-priority link: we keep a single link per key.
                let cd = c.cost * c.dist;
                let bd = best_cost * best_dist;
                if cd < bd {
                    best = Some(c);
                    best_cost = c.cost;
                    best_dist = c.dist;
                } else if cd > bd {
                    continue;
                } else if c.dist < best_dist {
                    best = Some(c);
                    best_cost = c.cost;
                    best_dist = c.dist;
                } else if c.dist > best_dist {
                    continue;
                } else if c.cost < best_cost {
                    best = Some(c);
                    best_cost = c.cost;
                    best_dist = c.dist;
                } else if c.cost > best_cost {
                    continue;
                }
                // Full tie: keep the earlier (lowest-key) candidate.
            }
        }
    }
    best
}

/// Direction classification of a hop relative to the tree.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
pub enum HopKind {
    /// Next hop is an ancestor (coords of current is a prefix-extension).
    Up,
    /// Next hop is a descendant (we are going down the tree).
    Down,
    /// Next hop is neither ancestor nor descendant: an off-tree shortcut.
    Shortcut,
}

pub fn classify(cur: &[u64], next: &[u64]) -> HopKind {
    let cp = crate::ygg::common_prefix_len(cur, next);
    if cp == next.len() && next.len() < cur.len() {
        HopKind::Up
    } else if cp == cur.len() && cur.len() < next.len() {
        // Also covers cur == root ([]): everything from there is "down".
        HopKind::Down
    } else {
        HopKind::Shortcut
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ygg::Key;

    fn key(n: u8) -> Key {
        let mut k = [0u8; 32];
        k[0] = n;
        Key(k)
    }

    #[test]
    fn picks_strictly_closer_only() {
        let cur = vec![1, 2];
        let dest = vec![1, 2, 3, 4];
        let peers = vec![
            (key(1), vec![1, 2, 3], 1u64), // closer, dist 1
            (key(2), vec![1, 2], 1),       // same dist (parent) - excluded
            (key(3), vec![9], 1),          // farther - excluded
        ];
        let (d_self, cands) = candidates(&cur, &dest, &peers);
        assert_eq!(d_self, 2);
        assert_eq!(cands.len(), 1);
        assert_eq!(cands[0].key, key(1));
    }

    #[test]
    fn cost_times_dist_minimized() {
        let cur = vec![1];
        let dest = vec![1, 2, 3, 4];
        // a: dist 3 cost 1  -> 3
        // b: dist 1 cost 10 -> 10
        // c: dist 2 cost 1  -> 2  (winner)
        let peers = vec![
            (key(1), vec![1, 2], 10u64),
            (key(2), vec![1, 2, 3, 9], 1),
            (key(3), vec![1, 2, 3], 1),
        ];
        let (_, cands) = candidates(&cur, &dest, &peers);
        let best = select_next(&cands).unwrap();
        assert_eq!(best.key, key(3));
    }

    #[test]
    fn tie_break_dist_then_cost_then_key() {
        let cur = vec![1];
        let dest = vec![1, 2, 3, 4];
        // equal cost*dist: dist 1 cost 2 vs dist 2 cost 1 -> dist wins
        let peers = vec![(key(1), vec![1, 2, 3], 2u64), (key(2), vec![1, 2], 1)];
        let (_, cands) = candidates(&cur, &dest, &peers);
        assert_eq!(select_next(&cands).unwrap().key, key(1));

        // equal cost*dist AND equal dist -> lower cost wins
        let peers = vec![(key(5), vec![1, 2, 3], 3u64), (key(6), vec![1, 2, 3], 2u64)];
        let (_, cands) = candidates(&cur, &dest, &peers);
        assert_eq!(select_next(&cands).unwrap().key, key(6));

        // full tie -> lowest key (deterministic stand-in for "oldest link")
        let peers = vec![(key(9), vec![1, 2, 3], 2u64), (key(8), vec![1, 2, 3], 2u64)];
        let (_, cands) = candidates(&cur, &dest, &peers);
        assert_eq!(select_next(&cands).unwrap().key, key(8));
    }

    #[test]
    fn empty_when_delivered() {
        let cur = vec![1, 2];
        let dest = vec![1, 2];
        let peers = vec![(key(1), vec![1], 1u64)];
        let (d_self, cands) = candidates(&cur, &dest, &peers);
        assert_eq!(d_self, 0);
        assert!(cands.is_empty());
        assert!(select_next(&cands).is_none());
    }

    #[test]
    fn classification() {
        assert_eq!(classify(&[1, 2], &[1]), HopKind::Up);
        assert_eq!(classify(&[1], &[1, 2]), HopKind::Down);
        assert_eq!(classify(&[1, 2], &[1, 3]), HopKind::Shortcut);
        assert_eq!(classify(&[], &[1]), HopKind::Down);
    }
}
