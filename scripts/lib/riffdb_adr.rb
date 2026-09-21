# frozen_string_literal: true

# Shared ADR, manifest, ledger, and SPEC helpers for the governance scripts.
#
# Agent 1 owns scripts/lib/riffdb_governance.rb (YAML loading, glob matching,
# touched-file discovery, crate mapping). That file did not exist when these
# scripts were written, so the small amount of overlap here -- root resolution
# and manifest access -- is deliberate and private. Nothing in this file
# duplicates the glob matcher or the touched-file discovery; the integrator can
# fold `root`, `manifest`, and `packages_by_id` into riffdb_governance.rb and
# leave the ADR/SPEC parsing here.
#
# The ADR parser accepts both forms at once, because the tree holds both while
# the migration runs:
#
#   v2      a leading `---` YAML front-matter block containing an `adr:` key
#   legacy  a `# ADR-NNNN: Title` heading with `- **Status:** X` (or `- Status: X`)

require "date"
require "yaml"

module RiffdbAdr
  # A record that opens a front-matter block whose YAML does not parse.
  #
  # This used to be swallowed. `split_front_matter` rescued the parse error and
  # returned "no front matter", so the record fell through to the legacy path
  # and read as an untitled legacy one with no obligations. One mistyped colon
  # in ADR-0251 therefore made `check-adr-obligations` report three fewer
  # declared obligations and pass, and `adr-index` render an accepted guarantee
  # record as `(untitled) | (unknown) | legacy`. Both stayed green.
  #
  # A file with no `---` block is still a legacy record, which is legitimate.
  # A file that opens one and then fails to parse is corrupt, and saying so is
  # the whole point.
  class MalformedFrontMatter < StandardError; end

  FRONT_MATTER = /\A---\r?\n(.*?)\r?\n---[ \t]*\r?\n/m
  TITLE_LINE = /\A\#[ \t]*ADR-(?:\d{4}|NNNN):[ \t]*(.*)$/
  LEGACY_STATUS = /^-[ \t]+(?:\*\*)?Status:(?:\*\*)?[ \t]*(.+)$/
  REQUIREMENT_ID = /[A-Z]{2,5}-\d{3}/

  # One ADR file, parsed. `front` is nil for legacy records.
  Record = Struct.new(
    :path, :basename, :number, :title, :status, :tier, :front, :body, :text, :v2,
    keyword_init: true
  ) do
    def v2?
      v2
    end

    def accepted?
      status.to_s.casecmp("accepted").zero?
    end

    def template?
      basename == "0000-template.md"
    end

    def id
      "ADR-#{number}"
    end

    def obligations
      return [] unless v2?

      Array(front["obligations"]).select { |entry| entry.is_a?(Hash) }
    end

    def front_list(key)
      return [] unless v2?

      Array(front[key]).map(&:to_s)
    end
  end

  module_function

  def default_root
    File.expand_path("../..", __dir__)
  end

  # RIFFDB_GOVERNANCE_ROOT exists so the self-tests can point a script at a
  # fabricated tree. Unset, every script resolves the repository it lives in.
  def root
    File.expand_path(ENV.fetch("RIFFDB_GOVERNANCE_ROOT", default_root))
  end

  def adr_dir(root_path)
    File.join(root_path, "adr")
  end

  def adr_paths(root_path)
    Dir.glob(File.join(adr_dir(root_path), "[0-9][0-9][0-9][0-9]-*.md")).sort
  end

  def records(root_path)
    adr_paths(root_path).map { |path| parse(path) }
  end

  def record_for(root_path, number)
    digits = format("%04d", number.to_s[/\d+/].to_i)
    path = Dir.glob(File.join(adr_dir(root_path), "#{digits}-*.md")).sort.first
    path && parse(path)
  end

  def parse(path)
    text = File.read(path, encoding: "UTF-8")
    basename = File.basename(path)
    front, body = split_front_matter(text, path)
    return legacy_record(path, basename, text) unless front

    Record.new(
      path: path,
      basename: basename,
      number: normalize_v2_number(front["adr"], basename),
      title: front["title"].to_s.strip,
      status: normalize_status(front["status"]),
      tier: front["tier"].to_s.strip,
      front: front,
      body: body,
      text: text,
      v2: true
    )
  end

  def split_front_matter(text, path = nil)
    match = FRONT_MATTER.match(text)
    return [nil, text] unless match

    begin
      parsed = YAML.safe_load(match[1], permitted_classes: [Date, Time], aliases: true)
    rescue Psych::Exception => error
      raise MalformedFrontMatter,
            "#{path || '<record>'}: front matter opens with --- but does not parse " \
            "as YAML (#{error.message.lines.first.to_s.strip}). A record that cannot " \
            "be read declares no obligations and renders as legacy, so this fails " \
            "rather than degrading."
    end
    return [nil, text] unless parsed.is_a?(Hash) && parsed.key?("adr")

    [parsed, match.post_match]
  end

  def legacy_record(path, basename, text)
    Record.new(
      path: path,
      basename: basename,
      number: normalize_number(nil, basename),
      title: text[TITLE_LINE, 1].to_s.strip,
      status: normalize_status(text[LEGACY_STATUS, 1].to_s.split(/\s+/).first),
      tier: "legacy",
      front: nil,
      body: text,
      text: text,
      v2: false
    )
  end

  def normalize_number(value, basename)
    digits = value.to_s[/\d+/] || basename[/\A(\d{4})/, 1]
    format("%04d", digits.to_i)
  end

  def normalize_v2_number(value, basename)
    filename_number = basename[/\A(\d{4})-/, 1]
    raise ArgumentError, "#{basename}: filename must begin with a four-digit ADR identifier" unless filename_number

    # The legacy template predates v2's canonical scalar rule. Real records use
    # a quoted string so YAML 1.1 readers cannot reinterpret 0202 as octal 130.
    return "0000" if basename == "0000-template.md" && value == 0

    unless value.is_a?(String) && value.match?(/\A\d{4}\z/)
      raise ArgumentError,
            "#{basename}: front-matter adr must be a quoted four-digit string"
    end
    unless value == filename_number
      raise ArgumentError,
            "#{basename}: front-matter adr #{value.inspect} does not match filename #{filename_number.inspect}"
    end

    value
  end

  def normalize_status(value)
    text = value.to_s.strip
    return "" if text.empty?

    text[0].upcase + text[1..].to_s
  end

  # The body section under `## <name>`, up to the next heading of the same or a
  # higher level, so numbered `### n.` decision clauses stay with their section.
  # Names are matched case-insensitively, first match wins.
  def section(body, names)
    lines = body.lines
    names.each do |name|
      pattern = /\A(\#{1,6})[ \t]+#{Regexp.escape(name)}[ \t]*\r?\n?\z/i
      start = lines.index { |line| line.match?(pattern) }
      next unless start

      level = pattern.match(lines[start])[1].length
      finish = start + 1
      while finish < lines.length
        heading = lines[finish][/\A(\#{1,6})[ \t]/, 1]
        break if heading && heading.length <= level

        finish += 1
      end
      return lines[(start + 1)...finish].join.strip
    end
    nil
  end

  def manifest_path(root_path)
    File.join(root_path, "work_packages.yaml")
  end

  def manifest(root_path)
    @manifests ||= {}
    @manifests[root_path] ||=
      YAML.safe_load(File.read(manifest_path(root_path), encoding: "UTF-8"), aliases: true) || {}
  end

  def packages(root_path)
    manifest(root_path).fetch("work_packages", [])
  end

  def packages_by_id(root_path)
    @packages_by_id ||= {}
    @packages_by_id[root_path] ||= packages(root_path).to_h { |package| [package["id"], package] }
  end

  # "complete", "complete_without_production_activation", and friends all count
  # as complete; "closed_without_release_activation" and "partial" do not.
  def complete?(package)
    package.is_a?(Hash) && package.dig("closure", "status").to_s.start_with?("complete")
  end

  def completed_on(package)
    value = package.is_a?(Hash) ? package.dig("closure", "completed_at") : nil
    to_date(value)
  end

  def to_date(value)
    case value
    when Date then value
    when Time then value.to_date
    when String
      begin
        Date.parse(value)
      rescue ArgumentError
        nil
      end
    end
  end

  def ledger_path(root_path)
    File.join(adr_dir(root_path), "obligations-outstanding.yaml")
  end

  def ledger(root_path)
    path = ledger_path(root_path)
    return {} unless File.exist?(path)

    YAML.safe_load(File.read(path, encoding: "UTF-8")) || {}
  end

  def ledger_entries(root_path)
    Array(ledger(root_path)["outstanding"]).select { |entry| entry.is_a?(Hash) }
  end

  def ledger_adr(entry)
    entry["adr"] || entry["key"].to_s.split("::", 2).first
  end

  def ledger_packages(entry)
    owners = Array(entry["work_packages"]) + Array(entry["owner"])
    owners.map(&:to_s).reject(&:empty?).uniq
  end

  # Requirement text as written in SPEC.md. Three shapes exist.
  #
  # Bullet (most recent requirements):
  #
  #   - `REP-002`: `riffdbd` MUST offer a follower mode that opens the same
  #     database format, ...
  #
  # continuing on more deeply indented lines until the next bullet or a blank
  # line. Paragraph (older sections):
  #
  #   `CON-001` Durable consumer identity MUST cover database, ...
  #   operation name, ...
  #
  # continuing until a blank line. Table row, with the text either in the same
  # cell as the identifier or in the next one:
  #
  #   | Standalone database | `SYS-001` The POC MUST run as ... | reason |
  #   | `DSL-001` | Command bodies MUST terminate ... |
  def spec_requirement_texts(root_path)
    lines = File.read(File.join(root_path, "SPEC.md"), encoding: "UTF-8").lines
    texts = {}
    # Rows whose identifier cell carries no backticks (`| POC-001 | text | ... |`)
    # are a weaker signal, so they only fill identifiers nothing else defined.
    bare = {}
    index = 0
    while index < lines.length
      line = lines[index].chomp

      bullet = line.match(/\A([ \t]*)[-*][ \t]+`(#{REQUIREMENT_ID})`[ \t]*:?[ \t]*(.*)\z/)
      if bullet
        parts = [bullet[3].strip]
        index += 1
        indent = bullet[1].length
        while index < lines.length
          nxt = lines[index]
          break if nxt.strip.empty?
          break if nxt[/\A[ \t]*/].length <= indent
          break if nxt.match?(/\A[ \t]*[-*][ \t]/)

          parts << nxt.strip
          index += 1
        end
        texts[bullet[2]] ||= join_text(parts)
        next
      end

      paragraph = line.match(/\A`(#{REQUIREMENT_ID})`[ \t]+(\S.*)\z/)
      if paragraph
        parts = [paragraph[2].strip]
        index += 1
        while index < lines.length
          nxt = lines[index].chomp
          break if nxt.strip.empty?
          break if nxt.match?(/\A[\#|>]/)
          break if nxt.match?(/\A`#{REQUIREMENT_ID}`[ \t]/)

          parts << nxt.strip
          index += 1
        end
        texts[paragraph[1]] ||= join_text(parts)
        next
      end

      if line.start_with?("|")
        cells = line.split("|").map(&:strip)
        cells.each_with_index do |cell, position|
          following = cells[(position + 1)..].to_a.find { |other| !other.empty? }.to_s
          if (cell_match = cell.match(/\A`(#{REQUIREMENT_ID})`[ \t]*(.*)\z/m))
            rest = cell_match[2].strip
            texts[cell_match[1]] ||= rest.empty? ? following : rest
          elsif (bare_match = cell.match(/\A(#{REQUIREMENT_ID})\z/))
            bare[bare_match[1]] ||= following
          end
        end
      end
      index += 1
    end
    bare.each { |id, text| texts[id] ||= text }
    texts
  end

  def join_text(parts)
    parts.reject(&:empty?).join(" ")
  end
end
