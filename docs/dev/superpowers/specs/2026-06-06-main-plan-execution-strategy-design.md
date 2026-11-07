# Main Plan Execution Strategy Design

## Goal

Execute `docs/superpowers/plans/2026-06-06-date-string-full-migration.md` in the current `main` checkout, with explicit user approval to work on `main`.

## Context

The repository is currently on `main` and has pre-existing uncommitted changes. The date string migration already has a design spec and implementation plan. This spec only defines how to execute that existing plan without mixing unrelated changes into the implementation.

## Strategy

Treat the current dirty worktree as user-owned context. Before implementation, record the current changed file list. During execution, modify only files required by the date string migration plan. Do not revert or rewrite pre-existing changes unless they directly block the plan and the user approves that specific action.

After each major plan task, inspect the changed file list and run the task's verification commands. If verification fails, stop and investigate before continuing. If a failure appears caused by pre-existing changes, report that clearly instead of masking it.

## Commit Discipline

The original plan asks for task-level commits. Because the repository already has uncommitted changes, commits must be selective: stage only files and hunks that belong to the current plan task. Unrelated pre-existing changes remain unstaged.

If selective staging is ambiguous because an existing dirty file and a plan edit overlap, inspect the diff carefully and either stage only the relevant hunks or stop for user direction.

## Success Criteria

- The date string migration plan is executed on `main`.
- Existing unrelated worktree changes are not reverted.
- Verification commands from the plan are run.
- Final static checks confirm that `from datetime import date` appears only where allowed by the migration design.
- Any commits contain only plan-related changes.
