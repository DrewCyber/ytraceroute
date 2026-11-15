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

/// Per-hop confidence of the predicted forwarding decision.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, serde::Serialize)]
pub enum TrustLevel {
    /// The decision is effectively forced: delivery, or the unique closest
    /// hop with no plausible alternatives and no blind spots.
    High,
    /// A plausible alternative existed (or peers are unknown): real link
    /// costs could pick a different winner.
    Medium,
    /// The choice is a guess: heavy unknowns on top of an open hop.
    Low,
}

/// Classify how trustworthy the chosen next hop is, given the full
/// candidate list and the number of peers we could not learn coordinates
/// for (unreachable probes plus deliberately skipped bulk in hinted mode).
///
/// Cost model reality: the chosen hop (dist d, assumed cost 1) loses to an
/// alternative at dist da iff its real link is more than da/d times more
/// expensive. A hop already at dist 1 is immune to unknown peers: nothing
/// can be strictly closer except the destination itself, which is always
/// probed first (it is in every hint set).
pub fn hop_trust(
    chosen: &Candidate,
    cands: &[Candidate],
    unknown: usize,
) -> (TrustLevel, Vec<String>) {
    let mut notes = Vec::new();
    let mut level = TrustLevel::High;
    let d = chosen.dist;
    if d == 0 {
        return (level, notes); // next hop is the destination itself
    }
    let d_alt = cands
        .iter()
        .filter(|c| c.key != chosen.key)
        .map(|c| c.dist)
        .min();
    if let Some(da) = d_alt {
        if da == d {
            level = level.max(TrustLevel::Medium);
            notes.push(format!("dist tie: {d_alt_tie} peers at dist {d}; real link costs decide",
                d_alt_tie = cands.iter().filter(|c| c.dist == d).count()));
        } else if da / d < 4 {
            // Flip needs the chosen link to be only da/d times slower:
            // plausible for adjacent distances.
            level = level.max(TrustLevel::Medium);
            notes.push(format!("cost-sensitive: dist {d} vs dist {da} alternatives"));
        }
        // da/d >= 4: the margin survives any plausible cost ratio.
    }
    if d > 1 && unknown > 0 {
        let heavy = unknown > 32;
        let lvl = if heavy { TrustLevel::Low } else { TrustLevel::Medium };
        level = level.max(lvl);
        notes.push(format!(
            "{unknown} peers with unknown coords could be closer"
        ));
    }
    (level, notes)
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
    // A tree edge is exactly ONE step: the direct parent (coords lose the
    // last element) or a direct child (coords gain exactly one). Anything
    // else — including a multi-level ancestor like [12,5] from [12,5,60,238]
    // — must be an off-tree peering link, i.e. a shortcut, because a packet
    // cannot traverse two tree edges in one hop.
    if cp == next.len() && cur.len() == next.len() + 1 {
        HopKind::Up
    } else if cp == cur.len() && next.len() == cur.len() + 1 {
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
    fn trust_levels() {
        use super::hop_trust;
        // Delivery: dist 0 is always High.
        let chosen = Candidate { key: key(1), cost: 1, dist: 0 };
        assert_eq!(hop_trust(&chosen, &[], 99).0, TrustLevel::High);

        // Unique closest hop at dist 1: immune to unknowns.
        let chosen = Candidate { key: key(1), cost: 1, dist: 1 };
        let (lvl, notes) = hop_trust(&chosen, &[], 200);
        assert_eq!(lvl, TrustLevel::High);
        assert!(notes.is_empty());

        // Dist tie: costs decide -> Medium.
        let cands = vec![
            Candidate { key: key(1), cost: 1, dist: 2 },
            Candidate { key: key(2), cost: 1, dist: 2 },
        ];
        let (lvl, notes) = hop_trust(&cands[0], &cands, 0);
        assert_eq!(lvl, TrustLevel::Medium);
        assert!(notes[0].starts_with("dist tie"));

        // Adjacent distances: cost-sensitive -> Medium.
        let cands = vec![
            Candidate { key: key(1), cost: 1, dist: 2 },
            Candidate { key: key(2), cost: 1, dist: 3 },
        ];
        let (lvl, notes) = hop_trust(&cands[0], &cands, 0);
        assert_eq!(lvl, TrustLevel::Medium);
        assert!(notes[0].starts_with("cost-sensitive"));

        // Wide margin: unique-winner semantics -> High.
        let cands = vec![
            Candidate { key: key(1), cost: 1, dist: 1 },
            Candidate { key: key(2), cost: 1, dist: 5 },
        ];
        assert_eq!(hop_trust(&cands[0], &cands, 0).0, TrustLevel::High);

        // Open hop (dist >= 2) with a few unknown peers -> Medium.
        let chosen = Candidate { key: key(1), cost: 1, dist: 2 };
        let (lvl, notes) = hop_trust(&chosen, &[], 5);
        assert_eq!(lvl, TrustLevel::Medium);
        assert!(notes[0].contains("unknown"));

        // Heavy unknowns on an open hop -> Low.
        assert_eq!(hop_trust(&chosen, &[], 64).0, TrustLevel::Low);
    }

    #[test]
    fn classification() {
        // Single tree steps.
        assert_eq!(classify(&[1, 2], &[1]), HopKind::Up);
        assert_eq!(classify(&[1], &[1, 2]), HopKind::Down);
        assert_eq!(classify(&[], &[1]), HopKind::Down);
        // A multi-level ancestor/descendant in one hop is an off-tree link.
        assert_eq!(classify(&[1, 2, 3], &[1]), HopKind::Shortcut);
        assert_eq!(classify(&[1], &[1, 2, 3]), HopKind::Shortcut);
        // Diverging branches.
        assert_eq!(classify(&[1, 2], &[1, 3]), HopKind::Shortcut);
        assert_eq!(classify(&[1, 2, 3, 4], &[1, 2]), HopKind::Shortcut);
    }
}
