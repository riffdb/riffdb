def package_ref:
  "urn:riffdb:cargo:"
  + (.name | @uri)
  + "@"
  + (.version | @uri)
  + ":"
  + ((.source // "workspace") | @uri);

def package_purl:
  "pkg:cargo/" + (.name | @uri) + "@" + (.version | @uri);

. as $metadata
| ($metadata.packages
   | map({ key: .id, value: package_ref })
   | from_entries) as $refs
| ("pkg:cargo/" + ($component_name | @uri) + "@" + $version) as $component_ref
| {
    bomFormat: "CycloneDX",
    specVersion: "1.5",
    version: 1,
    metadata: {
      component: {
        type: "application",
        "bom-ref": $component_ref,
        name: $component_name,
        version: $version,
        licenses: [
          {
            expression: $component_license
          }
        ],
        properties: [
          {
            name: "riffdb:git-revision",
            value: $revision
          },
          {
            name: "riffdb:lockfile",
            value: $lockfile
          }
        ]
      }
    },
    components: (
      $metadata.packages
      | map({
          type: "library",
          "bom-ref": package_ref,
          name: .name,
          version: .version,
          purl: package_purl,
          licenses: (
            if .license == null
            then []
            else [{ expression: .license }]
            end
          ),
          properties: (
            [
              {
                name: "riffdb:cargo-source",
                value: (.source // "workspace")
              }
            ]
            + (
              if .checksum == null
              then []
              else [{
                name: "riffdb:cargo-checksum",
                value: .checksum
              }]
              end
            )
          )
        })
      | unique_by(."bom-ref")
      | sort_by(."bom-ref")
    ),
    dependencies: (
      [
        {
          ref: $component_ref,
          dependsOn: (
            $metadata.workspace_members
            | map($refs[.])
            | unique
            | sort
          )
        }
      ]
      + (
        $metadata.resolve.nodes
        | map({
            ref: $refs[.id],
            dependsOn: (
              [.deps[].pkg as $id | $refs[$id]]
              | unique
              | sort
            )
          })
      )
      | sort_by(.ref)
    )
  }
