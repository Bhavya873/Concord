//! Current-term commit rule (Raft paper, Figure 8 / §5.4.2).

use crate::{LogIndex, Term};

/// Highest index a leader may commit.
///
/// `matched` is one `match_index` per voter (the leader's own value is its persisted index).
/// `quorum` is `floor(n/2)+1`. An index N is committed only when at least `quorum` voters
/// have `match >= N` and `term_at(N) == current_term`.
pub fn highest_committable(
    commit_index: LogIndex,
    current_term: Term,
    mut matched: Vec<LogIndex>,
    quorum: usize,
    mut term_at: impl FnMut(LogIndex) -> Option<Term>,
) -> LogIndex {
    if quorum == 0 || quorum > matched.len() {
        return commit_index;
    }
    matched.sort_by(|left, right| right.cmp(left));
    let Some(mut n) = matched.get(quorum - 1).copied() else {
        return commit_index;
    };
    while n > commit_index {
        if term_at(n) == Some(current_term) {
            return n;
        }
        n -= 1;
    }
    commit_index
}

#[cfg(test)]
mod tests {
    use super::highest_committable;

    fn terms(table: &[u64]) -> impl FnMut(u64) -> Option<u64> + '_ {
        move |index| table.get(index as usize).copied()
    }

    #[test]
    fn previous_term_replicated_to_a_majority_is_not_committed() {
        // Index 5 is term 1, stored on a majority, but the leader's term is 2.
        let table = [0, 1, 1, 1, 1, 1];
        let got = highest_committable(0, 2, vec![5, 5, 3], 2, terms(&table));
        assert_eq!(got, 0);
    }

    #[test]
    fn current_term_entry_on_a_majority_commits_the_prefix() {
        let table = [0, 1, 1, 1, 1, 1, 2];
        let got = highest_committable(0, 2, vec![6, 6, 5], 2, terms(&table));
        assert_eq!(got, 6);
    }

    #[test]
    fn walks_below_an_uncommitted_newer_index() {
        // 2nd-highest match is 3, and index 3 is the current term. Index 4 is older and not on a quorum.
        let table = [0, 1, 1, 2, 1];
        let got = highest_committable(0, 2, vec![4, 3, 3], 2, terms(&table));
        assert_eq!(got, 3);
    }

    #[test]
    fn does_not_move_commit_backward() {
        let table = [0, 2, 2];
        let got = highest_committable(2, 2, vec![1, 1, 1], 2, terms(&table));
        assert_eq!(got, 2);
    }
}
