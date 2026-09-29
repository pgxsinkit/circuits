// The wrapper runs the log server built from this workspace and nothing else. Before, it fell back
// to a `durable-streams-server` on PATH, in ~/.cargo/bin, or a `cargo install` from crates.io —
// upstream's build, not ours — so a stale binary could be tested without anyone noticing. These
// tests pin the resolution rules: the workspace build, an explicit DS_RUST_BIN, and a loud failure
// otherwise.

import { chmodSync, existsSync, mkdirSync, mkdtempSync, rmSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { delimiter, dirname, join, resolve } from 'node:path'
import { fileURLToPath } from 'node:url'

import { afterEach, describe, expect, it } from 'vitest'

import { ensureServerBinary, workspaceServerBinary } from './index.js'

const root = join(dirname(fileURLToPath(import.meta.url)), '../../..')
const saved = { ...process.env }
const scratch: string[] = []

function tempDir(prefix: string): string {
  const dir = mkdtempSync(join(tmpdir(), prefix))
  scratch.push(dir)
  return dir
}

afterEach(() => {
  for (const key of ['DS_RUST_BIN', 'CARGO_TARGET_DIR', 'CARGO_HOME', 'PATH'] as const) {
    if (saved[key] === undefined) delete process.env[key]
    else process.env[key] = saved[key]
  }
  for (const dir of scratch.splice(0)) rmSync(dir, { recursive: true, force: true })
})

describe('durable-streams-server resolution', () => {
  it('uses the debug build in the workspace target directory', () => {
    delete process.env.DS_RUST_BIN
    const target = process.env.CARGO_TARGET_DIR ? resolve(root, process.env.CARGO_TARGET_DIR) : join(root, 'target')
    const bin = ensureServerBinary()
    expect(bin).toBe(join(target, 'debug', 'durable-streams-server'))
    expect(existsSync(bin), 'the vitest global setup builds the log server').toBe(true)
  })

  it('honours CARGO_TARGET_DIR', () => {
    delete process.env.DS_RUST_BIN
    const target = tempDir('ds-rust-target-')
    process.env.CARGO_TARGET_DIR = target
    expect(workspaceServerBinary()).toBe(join(target, 'debug', 'durable-streams-server'))
  })

  it('never falls back to a binary on PATH or in CARGO_HOME, and names the build command', () => {
    delete process.env.DS_RUST_BIN
    // A decoy server where the old wrapper looked: on PATH and in $CARGO_HOME/bin.
    const decoyHome = tempDir('ds-rust-decoy-')
    mkdirSync(join(decoyHome, 'bin'))
    const decoy = join(decoyHome, 'bin', 'durable-streams-server')
    writeFileSync(decoy, '#!/bin/sh\nexit 0\n')
    chmodSync(decoy, 0o755)
    process.env.CARGO_HOME = decoyHome
    process.env.PATH = `${join(decoyHome, 'bin')}${delimiter}${saved.PATH ?? ''}`
    // An empty target directory: the workspace binary is missing.
    const target = tempDir('ds-rust-target-')
    process.env.CARGO_TARGET_DIR = target

    expect(() => ensureServerBinary()).toThrow(join(target, 'debug', 'durable-streams-server'))
    expect(() => ensureServerBinary()).toThrow('cargo build -p durable-streams')
  })

  it('uses DS_RUST_BIN when it exists', () => {
    const bin = join(tempDir('ds-rust-bin-'), 'durable-streams-server')
    writeFileSync(bin, '')
    process.env.DS_RUST_BIN = bin
    expect(ensureServerBinary()).toBe(bin)
  })

  it('refuses a DS_RUST_BIN that does not exist, without falling back', () => {
    const missing = join(tempDir('ds-rust-bin-'), 'no-such-binary')
    process.env.DS_RUST_BIN = missing
    expect(() => ensureServerBinary()).toThrow(`DS_RUST_BIN=${missing} does not exist`)
  })
})
