# JSON/XML

Any encoder or decoder for JSON/XML can be used, providing they
implement `decode/1` and `encode/1`.

To use [jsx][github-com-jsx], apply the following configuration:

```erlang
 {pgmp, [...
         {codec_json, jsx},
         {codec_jsonb, jsx},
         ...]}
```

[github-com-jsx]: https://github.com/talentdeficit/jsx
