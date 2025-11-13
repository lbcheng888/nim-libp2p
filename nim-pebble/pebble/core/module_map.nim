# Mapping between Go package paths in the original Pebble repository and their
# Nim module counterparts. Used by tooling and doc generation tasks to keep the
# rewrite aligned with the legacy layout.

import std/tables

const packageMap* = {
  "github.com/cockroachdb/pebble": "pebble.core",
  "github.com/cockroachdb/pebble/batch": "pebble.batch",
  "github.com/cockroachdb/pebble/cache": "pebble.cache",
  "github.com/cockroachdb/pebble/compaction": "pebble.compaction",
  "github.com/cockroachdb/pebble/iterator": "pebble.read",
  "github.com/cockroachdb/pebble/internal": "pebble.internal",
  "github.com/cockroachdb/pebble/internal/manifest": "pebble.manifest",
  "github.com/cockroachdb/pebble/objstorage": "pebble.storage",
  "github.com/cockroachdb/pebble/sstable": "pebble.sstable",
  "github.com/cockroachdb/pebble/memtable": "pebble.mem",
  "github.com/cockroachdb/pebble/vfs": "pebble.vfs",
  "github.com/cockroachdb/pebble/rangekey": "pebble.ext",
  "github.com/cockroachdb/pebble/rangedel": "pebble.ext",
  "github.com/cockroachdb/pebble/record": "pebble.wal",
  "github.com/cockroachdb/pebble/merging": "pebble.merge",
  "github.com/cockroachdb/pebble/bloom": "pebble.filter",
  "github.com/cockroachdb/pebble/options": "pebble.config",
  "github.com/cockroachdb/pebble/tool": "pebble.tooling"
}.toTable
