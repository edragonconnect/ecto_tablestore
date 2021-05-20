# Used by "mix format"
locals_without_parens = [
  create: 2,
  drop: 1,
  drop_if_exists: 1,
  add: 2,
  add: 3,
  add_pk: 1,
  add_pk: 2,
  add_pk: 3,
  add_column: 1,
  add_column: 2,
  add_index: 3
]

[
  inputs: ["{mix,.formatter}.exs", "{config,lib,test}/**/*.{ex,exs}"],
  import_deps: [:ecto],
  locals_without_parens: locals_without_parens,
  export: [
    locals_without_parens: locals_without_parens
  ]
]
