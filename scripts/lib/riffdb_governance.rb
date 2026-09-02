#!/usr/bin/env ruby
# frozen_string_literal: true

# Shared governance helpers for the RiffDB process tooling (governance v2).
#
# One implementation of: repository discovery, governance/tiers.yaml and
# work_packages.yaml loading, the glob matcher, touched-file discovery, the
# path-to-crate mapping, tier computation and path-triggered check computation.
# Every governance script requires this file; nothing here shells out to cargo.
#
# Glob semantics (tiers.yaml rules/checks/governance_paths and work-package
# allowed_paths all use them):
#
#   * every pattern is anchored at the repository root, so a pattern without a
#     slash matches at the root only (`SPEC.md` never matches `docs/SPEC.md`);
#   * `*`, `?` and `[...]` match inside one path segment and never cross `/`;
#   * `**` matches zero or more whole segments, so `a/**` covers `a/b.rs` and
#     `a/b/c.rs`, and `**` alone covers every path;
#   * gitignore-like directory semantics: when a pattern matches a leading run
#     of a path's segments, everything below that point matches too, so
#     `src/catalog*` covers `src/catalog.rs` today and `src/catalog/mod.rs`
#     tomorrow.
#
# Run `ruby scripts/lib/riffdb_governance.rb --self-test` to exercise the
# matcher and the tier computation.

require "open3"
require "set"
require "yaml"

module RiffdbGovernance
  # Ascending ceremony. Index in this array is the tier rank.
  TIER_ORDER = %w[internal surface guarantee].freeze

  class Error < StandardError; end

  # A single classified change.
  Classification = Struct.new(:tier, :files, :deciding_file, :checks, :trailer_tier, keyword_init: true)

  # One classified file.
  FileTier = Struct.new(:path, :tier, :rule, keyword_init: true)

  module_function

  # --- repository ---------------------------------------------------------

  def root
    @root ||= begin
      out, err, status = Open3.capture3("git", "rev-parse", "--show-toplevel", chdir: __dir__)
      raise Error, "not inside a git repository: #{err.strip}" unless status.success?

      File.realpath(out.strip)
    end
  end

  def path_in_root(relative)
    File.join(root, relative)
  end

  def git(*args)
    out, err, status = Open3.capture3("git", *args, chdir: root)
    raise Error, "git #{args.join(' ')} failed: #{err.strip}" unless status.success?

    out
  end

  def git_ok(*args)
    _out, _err, status = Open3.capture3("git", *args, chdir: root)
    status.success?
  end

  def git_lines_z(*args)
    git(*args).split("\0").reject(&:empty?)
  end

  # --- configuration ------------------------------------------------------

  def tiers_path
    path_in_root("governance/tiers.yaml")
  end

  def tiers_config
    @tiers_config ||= begin
      raise Error, "governance/tiers.yaml is missing" unless File.file?(tiers_path)

      config = YAML.safe_load(File.read(tiers_path, encoding: "UTF-8"), aliases: true)
      raise Error, "governance/tiers.yaml is not a mapping" unless config.is_a?(Hash)

      config
    end
  end

  def tier_names
    tiers_config.fetch("tiers", {}).keys
  end

  def ceremony(tier)
    tiers_config.dig("tiers", tier, "ceremony")
  end

  def cutover
    tiers_config["cutover"]
  end

  def rules
    tiers_config.fetch("rules", [])
  end

  def check_rules
    tiers_config.fetch("checks", [])
  end

  def always_commands
    tiers_config.fetch("always", [])
  end

  def governance_paths
    tiers_config.fetch("governance_paths", [])
  end

  def manifest_path
    path_in_root("work_packages.yaml")
  end

  # work_packages.yaml is large; load it only when a caller asks for it.
  def manifest
    @manifest ||= YAML.safe_load(File.read(manifest_path, encoding: "UTF-8"), aliases: true)
  end

  def packages
    manifest.fetch("work_packages", [])
  end

  def package(id)
    packages.find { |entry| entry["id"] == id }
  end

  # --- glob matcher -------------------------------------------------------

  module Glob
    FNM_FLAGS = File::FNM_PATHNAME | File::FNM_DOTMATCH

    module_function

    # True when `pattern` selects `path` (both repository-root relative).
    def match?(pattern, path)
      pattern_segments = normalize(pattern)
      return false if pattern_segments.empty?

      path_segments = normalize(path)
      return false if path_segments.empty?

      walk(pattern_segments, 0, path_segments, 0)
    end

    # The first pattern in `patterns` that selects `path`, or nil.
    def first_match(patterns, path)
      Array(patterns).find { |pattern| match?(pattern, path) }
    end

    def match_any?(patterns, path)
      !first_match(patterns, path).nil?
    end

    def normalize(value)
      value.to_s.strip.sub(%r{\A\./}, "").gsub(%r{/+}, "/").sub(%r{\A/}, "").sub(%r{/\z}, "").split("/")
    end

    def walk(pattern, pattern_index, path, path_index)
      while pattern_index < pattern.length
        segment = pattern[pattern_index]
        if segment == "**"
          # `**` absorbs zero or more whole segments.
          return true if pattern_index + 1 == pattern.length

          (path_index..path.length).each do |candidate|
            return true if walk(pattern, pattern_index + 1, path, candidate)
          end
          return false
        end

        return false if path_index >= path.length
        return false unless File.fnmatch?(segment, path[path_index], FNM_FLAGS)

        pattern_index += 1
        path_index += 1
      end

      # Pattern exhausted: an exact hit, or a directory hit with the path
      # continuing below the matched prefix.
      true
    end
  end

  # --- touched files ------------------------------------------------------

  def current_branch
    git("rev-parse", "--abbrev-ref", "HEAD").strip
  rescue Error
    ""
  end

  # merge-base(HEAD, main)..HEAD on a branch, else HEAD~1..HEAD.
  def default_range
    if current_branch != "main" && git_ok("rev-parse", "--verify", "--quiet", "main")
      base = git("merge-base", "HEAD", "main").strip
      return "#{base}..HEAD" unless base.empty?
    end
    return "HEAD~1..HEAD" if git_ok("rev-parse", "--verify", "--quiet", "HEAD~1")

    nil
  end

  def range_for(range: nil, base: nil)
    return range if range
    return "#{git('merge-base', 'HEAD', base).strip}..HEAD" if base

    default_range
  end

  # Files changed in the range plus staged, unstaged and untracked changes.
  # Explicit paths short-circuit the discovery entirely.
  def touched_files(range: nil, base: nil, paths: [], worktree: true)
    return normalize_paths(paths) if paths && !paths.empty?

    found = Set.new
    effective = range_for(range: range, base: base)
    found.merge(git_lines_z("diff", "--name-only", "-z", effective)) if effective
    if worktree
      found.merge(git_lines_z("diff", "--name-only", "-z", "HEAD")) if git_ok("rev-parse", "--verify", "--quiet", "HEAD")
      found.merge(git_lines_z("ls-files", "--others", "--exclude-standard", "-z"))
    end
    found.to_a.sort
  end

  def normalize_paths(paths)
    prefix = "#{root}/"
    Array(paths).map do |entry|
      absolute = File.expand_path(entry, Dir.pwd)
      relative = absolute.start_with?(prefix) ? absolute.delete_prefix(prefix) : entry.to_s
      relative.sub(%r{\A\./}, "")
    end.reject(&:empty?).uniq.sort
  end

  # Commits in the range may raise, never lower, the computed tier.
  def trailer_tier(range)
    return nil unless range

    body = begin
      git("log", "--format=%B", range)
    rescue Error
      return nil
    end
    claimed = body.scan(/^\s*Governance-Tier:\s*([A-Za-z]+)\s*$/).flatten.map(&:downcase)
    claimed &= TIER_ORDER
    return nil if claimed.empty?

    claimed.max_by { |tier| tier_rank(tier) }
  end

  # --- tiers --------------------------------------------------------------

  def tier_rank(tier)
    TIER_ORDER.index(tier.to_s) || -1
  end

  def valid_tier?(tier)
    TIER_ORDER.include?(tier.to_s)
  end

  # First matching rule wins for a single file.
  def tier_for(path)
    rules.each do |rule|
      pattern = Glob.first_match(rule.fetch("paths", []), path)
      return [rule.fetch("tier"), pattern] if pattern
    end
    ["internal", nil]
  end

  # A change's tier is the maximum over its files.
  def classify(paths, range: nil)
    classified = Array(paths).sort.map do |path|
      tier, rule = tier_for(path)
      FileTier.new(path: path, tier: tier, rule: rule)
    end
    computed = classified.map { |entry| entry.tier }.max_by { |tier| tier_rank(tier) } || "internal"
    claimed = trailer_tier(range)
    tier = claimed && tier_rank(claimed) > tier_rank(computed) ? claimed : computed
    Classification.new(
      tier: tier,
      files: classified,
      deciding_file: classified.find { |entry| entry.tier == computed },
      checks: triggered_checks(paths),
      trailer_tier: claimed
    )
  end

  # Path-triggered checks, in tiers.yaml order, deduplicated.
  def triggered_checks(paths)
    files = Array(paths)
    commands = []
    check_rules.each do |rule|
      patterns = rule.fetch("when", [])
      next unless files.any? { |path| Glob.match_any?(patterns, path) }

      rule.fetch("run", []).each { |command| commands << command unless commands.include?(command) }
    end
    commands
  end

  # --- crates -------------------------------------------------------------

  def crate_names
    @crate_names ||= Dir.children(path_in_root("crates"))
                        .select { |name| File.file?(path_in_root("crates/#{name}/Cargo.toml")) }
                        .sort
  end

  def crate_manifest(name)
    File.read(path_in_root("crates/#{name}/Cargo.toml"), encoding: "UTF-8")
  end

  # Root tests/** files belong to the crate that declares them as a
  # `[[test]] path = "../../tests/..."` target. Files that are not themselves
  # a declared target fall back to the owner(s) of their directory.
  def test_owners
    @test_owners ||= begin
      exact = Hash.new { |hash, key| hash[key] = [] }
      directories = Hash.new { |hash, key| hash[key] = [] }
      crate_names.each do |name|
        crate_manifest(name).scan(/^\s*path\s*=\s*"(\.\.\/\.\.\/tests\/[^"]+)"/).flatten.each do |declared|
          relative = declared.sub(%r{\A\.\./\.\./}, "")
          exact[relative] << name unless exact[relative].include?(name)
          directory = File.dirname(relative)
          # Never map the whole tests/ root onto one crate.
          next if directory == "tests" || directory == "."

          directories[directory] << name unless directories[directory].include?(name)
        end
      end
      { exact: exact, directories: directories }
    end
  end

  def crates_for_test_path(path)
    owners = test_owners
    return owners[:exact][path] if owners[:exact].key?(path)

    best = owners[:directories].keys
                               .select { |directory| path == directory || path.start_with?("#{directory}/") }
                               .max_by(&:length)
    best ? owners[:directories][best] : []
  end

  # Touched crate = any file under crates/<name>/, plus the owners of touched
  # root tests/** files.
  def crates_for(paths)
    known = crate_names.to_set
    found = Set.new
    Array(paths).each do |path|
      segments = path.split("/")
      if segments.first == "crates" && segments.length >= 2 && known.include?(segments[1])
        found << segments[1]
      elsif segments.first == "tests"
        found.merge(crates_for_test_path(path))
      end
    end
    found.to_a.sort
  end

  # Workspace dependency edges, read from the crate manifests so that plan
  # computation never has to shell out to cargo.
  def dependency_edges
    @dependency_edges ||= begin
      edges = {}
      known = crate_names.to_set
      crate_names.each do |name|
        dependencies = Set.new
        in_dependency_table = false
        crate_manifest(name).each_line do |line|
          stripped = line.strip
          if stripped.start_with?("[")
            in_dependency_table = stripped.match?(/\A\[(?:[a-z0-9_."'\-*]+\.)?(?:dev-|build-)?dependencies\]\z/)
            next
          end
          next unless in_dependency_table

          dependency = stripped[/\A([A-Za-z0-9_-]+)\s*=/, 1]
          dependencies << dependency if dependency && known.include?(dependency) && dependency != name
        end
        edges[name] = dependencies.to_a.sort
      end
      edges
    end
  end

  # Transitive reverse dependencies of `names`, excluding `names` themselves.
  def reverse_dependents(names)
    edges = dependency_edges
    frontier = Array(names).to_set
    closure = Set.new
    until frontier.empty?
      dependents = Set.new
      edges.each do |crate, dependencies|
        next if closure.include?(crate) || frontier.include?(crate)

        dependents << crate if dependencies.any? { |dependency| frontier.include?(dependency) }
      end
      closure.merge(dependents)
      frontier = dependents
    end
    (closure - Array(names).to_set).to_a.sort
  end

  # --- self test ----------------------------------------------------------

  # rubocop:disable Metrics/MethodLength
  def self_test
    failures = []
    check = lambda do |label, actual, expected|
      failures << "#{label}: expected #{expected.inspect}, got #{actual.inspect}" unless actual == expected
    end

    matcher_cases = [
      ["crates/riffdb-types/**", "crates/riffdb-types/src/lib.rs", true],
      ["crates/riffdb-types/**", "crates/riffdb-types/Cargo.toml", true],
      ["crates/riffdb-types/**", "crates/riffdb-commit/src/lib.rs", false],
      ["tests/**", "tests/grpc/grpc_end_to_end.rs", true],
      ["tests/**", "crates/riffdb-commit/tests/architecture.rs", false],
      ["**", "README.md", true],
      ["**", "a/b/c/d.rs", true],
      ["SPEC.md", "SPEC.md", true],
      ["SPEC.md", "docs/SPEC.md", false],
      ["adr/0178-a-title.md", "adr/0178-a-title.md", true],
      ["adr/0178-a-title.md", "adr/0179-other.md", false],
      ["adr/**", "adr/README.md", true],
      ["crates/**", "crates/riffdb-cli/src/cli.rs", true],
      [".github/workflows/**", ".github/workflows/ci.yml", true],
      [".config/**", ".config/nextest.toml", true],
      ["Cargo.toml", "Cargo.toml", true],
      ["Cargo.toml", "crates/riffdb-commit/Cargo.toml", false],
      ["crates/*/Cargo.toml", "crates/riffdb-commit/Cargo.toml", true],
      ["crates/*/Cargo.toml", "crates/a/b/Cargo.toml", false],
      ["crates/*/Cargo.toml", "Cargo.toml", false],
      ["scripts/*driver*", "scripts/check-driver-distribution", true],
      ["scripts/*driver*", "scripts/ci-all", false],
      ["adr/0100-*.md", "adr/0100-anything.md", true],
      ["adr/0100-*.md", "adr/0101-anything.md", false],
      ["crates/riffdb-storage-api/src/catalog*", "crates/riffdb-storage-api/src/catalog.rs", true],
      ["crates/riffdb-storage-api/src/catalog*", "crates/riffdb-storage-api/src/catalog/mod.rs", true],
      ["crates/riffdb-storage-api/src/catalog*", "crates/riffdb-storage-api/src/keys.rs", false],
      ["examples/agent-alpha/**/generated/typescript/client.ts",
       "examples/agent-alpha/generated/typescript/client.ts", true],
      ["examples/agent-alpha/**/generated/typescript/client.ts",
       "examples/agent-alpha/a/b/generated/typescript/client.ts", true],
      ["examples/agent-alpha/**/generated/typescript/client.ts",
       "examples/agent-beta/generated/typescript/client.ts", false],
      ["release/container/**", "release/container/Dockerfile", true],
      ["release/container/**", "release/helm/values.yaml", false]
    ]
    matcher_cases.each do |pattern, path, expected|
      check.call("match?(#{pattern.inspect}, #{path.inspect})", Glob.match?(pattern, path), expected)
    end

    check.call("first_match order", Glob.first_match(["docs/**", "adr/**", "adr/README.md"], "adr/README.md"), "adr/**")

    tier_cases = [
      ["crates/riffdb-commit/src/lib.rs", "guarantee"],
      ["crates/riffdb-api-mcp/src/handler.rs", "guarantee"],
      ["crates/riffdb-api-mcp/src/tool.rs", "surface"],
      ["crates/riffdb-storage-redb/src/store.rs", "guarantee"],
      ["crates/riffdb-storage-redb/src/layout.rs", "surface"],
      ["crates/riffdb-storage-redb/src/other.rs", "internal"],
      ["AGENTS.md", "guarantee"],
      ["SPEC.md", "surface"],
      ["proto/riffdb.proto", "surface"],
      ["adr/0001-x.md", "surface"],
      ["README.md", "internal"],
      ["crates/riffdb-testkit/src/lib.rs", "internal"],
      ["crates/riffdb-commit/Cargo.toml", "internal"]
    ]
    tier_cases.each do |path, expected|
      check.call("tier_for(#{path.inspect})", tier_for(path).first, expected)
    end

    check.call(
      "maximum over files",
      classify(["README.md", "crates/riffdb-commit/src/lib.rs", "SPEC.md"], range: nil).tier,
      "guarantee"
    )
    check.call("maximum over files (surface)", classify(%w[README.md SPEC.md], range: nil).tier, "surface")
    check.call("empty change", classify([], range: nil).tier, "internal")
    check.call(
      "deciding file",
      classify(["README.md", "crates/riffdb-commit/src/lib.rs"], range: nil).deciding_file.path,
      "crates/riffdb-commit/src/lib.rs"
    )
    check.call(
      "triggered checks",
      triggered_checks(["crates/riffdb-proto/src/lib.rs"]),
      ["./scripts/check-generated", "./scripts/check-version-topology", "./scripts/check-file-size-guard",
       "./scripts/check-panic-allowances"]
    )
    check.call("triggered checks (document)", triggered_checks(["README.md"]), ["./scripts/handbook check"])
    check.call("triggered checks (none)", triggered_checks(["LICENSE-MIT"]), [])
    check.call("tier rank order", TIER_ORDER.map { |tier| tier_rank(tier) }, [0, 1, 2])

    check.call("crates_for src file", crates_for(["crates/riffdb-commit/src/lib.rs"]), ["riffdb-commit"])
    check.call("crates_for root test", crates_for(["tests/writer_fail_fast.rs"]), ["riffdb-server"])
    check.call("crates_for test directory", crates_for(["tests/service/helpers.rs"]), ["riffdb-service"])
    check.call("crates_for non-crate path", crates_for(["docs/SUMMARY.md"]), [])
    unless reverse_dependents(["riffdb-types"]).include?("riffdb-service")
      failures << "reverse_dependents(riffdb-types) omits riffdb-service"
    end
    if reverse_dependents(["riffdb-server"]).include?("riffdb-types")
      failures << "reverse_dependents(riffdb-server) wrongly includes riffdb-types"
    end

    if failures.empty?
      puts "riffdb_governance self-test passed: #{matcher_cases.length} matcher cases, " \
           "#{tier_cases.length} tier cases, crate mapping and reverse dependencies."
      true
    else
      warn failures.join("\n")
      false
    end
  end
  # rubocop:enable Metrics/MethodLength
end

if $PROGRAM_NAME == __FILE__
  case ARGV[0]
  when "--self-test"
    exit(RiffdbGovernance.self_test ? 0 : 1)
  when "--help", "-h", nil
    puts <<~HELP
      Usage: ruby scripts/lib/riffdb_governance.rb --self-test

      Shared governance library for the RiffDB process tooling. Require it from a
      script; run it directly only to exercise the glob matcher, the tier
      computation and the crate mapping.
    HELP
    exit 0
  else
    warn "unknown argument #{ARGV[0].inspect}; use --self-test or --help"
    exit 2
  end
end
