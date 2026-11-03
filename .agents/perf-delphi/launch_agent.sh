#!/usr/bin/env bash
# Launch one Delphi hypothesis agent headless on another Claude account.
# usage: .agents/perf-delphi/launch_agent.sh <ID> <account>   e.g. D5 ceo, D9 chi
# The agent gets a scoped tool allowlist (edits inside its worktree, the repo's
# interpreter/ruff/ty, git and read-only shell commands); no permission bypass.
set -u
ID="$1"; ACCT="$2"
WT="/tmp/tif1-delphi/$ID"
LOG="/home/harish/Documents/tif1/.agents/perf-delphi/logs/$ID.log"
PROMPT="Implement and measure hypothesis $ID of the Delphi performance round of the tif1 library. Your git worktree is $WT (branch perf/delphi-$ID); your private warm cache copy is /tmp/tif1-delphi-warm-$ID. Read these files first: /home/harish/Documents/tif1/.agents/perf-delphi/BRIEF.md (the shared protocol: environment, commands, measurement and parity rules, deliverable format), the $ID section of /home/harish/Documents/tif1/.agents/perf-delphi/HYPOTHESES.md, /home/harish/Documents/tif1/.agents/perf-delphi/DO_NOT_RETRY.md, and $WT/AGENTS.md. If /home/harish/Documents/tif1/.agents/perf-delphi/${ID}_HANDOFF.md exists, read it too: it describes work already in the worktree and what remains. Edit only files inside $WT. Measure with the interleaved A/B harness and the parity digest described in the brief, keep ruff, ty and the unit suite green, commit on the branch, and write and commit the report $WT/.agents/perf-delphi/$ID.md. End with a short summary of the verdict and the key numbers (control vs candidate medians, pairs won, parity)."
ALLOWED="Read,Edit,Write,Glob,Grep,Bash(/home/harish/Documents/tif1/.venv/bin/python *),Bash(/home/harish/Documents/tif1/.venv/bin/ruff *),Bash(/home/harish/Documents/tif1/.venv/bin/ty *),Bash(git *),Bash(cd *),Bash(ls *),Bash(cat *),Bash(head *),Bash(tail *),Bash(grep *),Bash(wc *),Bash(diff *),Bash(mkdir *),Bash(cp *),Bash(sed -n *),Bash(export *),Bash(PYTHONPATH=*),Bash(TIF1_CACHE_DIR=*),Bash(PY=*)"
cd "$WT" || exit 1
CLAUDE_CONFIG_DIR="$HOME/.claude-$ACCT" nohup claude -p "$PROMPT" --model claude-fable-5-1 --effort xhigh --name "delphi-$ID" --permission-mode acceptEdits --allowedTools "$ALLOWED" --output-format json > "$LOG" 2>&1 &
echo "launched $ID on $ACCT (pid $!), log: $LOG"
