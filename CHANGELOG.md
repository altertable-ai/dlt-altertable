# Changelog

## [0.1.1](https://github.com/altertable-ai/dlt-altertable/compare/dlt-altertable-v0.1.0...dlt-altertable-v0.1.1) (2026-08-20)


### Features

* prepend dbt-altertable to the user agent ([#4](https://github.com/altertable-ai/dlt-altertable/issues/4)) ([6dd25fb](https://github.com/altertable-ai/dlt-altertable/commit/6dd25fb5bb15f36f67715a17b545792a3d8b1333))

## 0.1.0 (2026-08-18)


### Features

* add initial Altertable dlt destination ([5f01ded](https://github.com/altertable-ai/dlt-altertable/commit/5f01ded62b1b2a9249e395970db06d510ca2f047))
* declare one overridable configuration spec for the destination ([469431e](https://github.com/altertable-ai/dlt-altertable/commit/469431eaba9e52969ce5775a6191d664a28118d1))
* evolve target tables, adopt dlt naming, and fail fast on bad credentials ([fe27ade](https://github.com/altertable-ai/dlt-altertable/commit/fe27ade5cb18b6422c5b62529560df3476a27187))
* log schema changes, cap error bodies, and own aligned copies in a context manager ([9c1153d](https://github.com/altertable-ai/dlt-altertable/commit/9c1153da464d54887e2f5dc45b4eb38e6a6d892d))
* make the schema-query compute size configurable ([c12e7d6](https://github.com/altertable-ai/dlt-altertable/commit/c12e7d6355b57f8847fd90d0b3254e49d5baae9c))
* move the write path to the HTTP Lakehouse API ([44cc6d7](https://github.com/altertable-ai/dlt-altertable/commit/44cc6d7c9d4e1a9a60a2818a8a6f7bf0a4db6f26))


### Bug Fixes

* create missing target tables instead of relying on create_append ([e470e25](https://github.com/altertable-ai/dlt-altertable/commit/e470e25cb72e235d2dd958bf600eada0d724daaa))
* pad narrower files to the load schema before uploading ([4e717dd](https://github.com/altertable-ai/dlt-altertable/commit/4e717dd522caca3a6c95b8f68edc9a6046d8acc4))
* post merge files unpadded so omitted columns keep stored values ([08fd04b](https://github.com/altertable-ai/dlt-altertable/commit/08fd04bbde41420c5fa9e098c34c7a7b53245884))
* reject wei columns instead of creating lossy decimals ([9628e52](https://github.com/altertable-ai/dlt-altertable/commit/9628e52dd21265a2d53a453410aa0059c3b9e36e))
* require urllib3 2 for the large-block upload adapter ([f0ceffc](https://github.com/altertable-ai/dlt-altertable/commit/f0ceffc6984ff48f7e159feb0d0c6f178478b6ed))
* uncache created tables, mark the password secret, and reject comma key columns ([0604265](https://github.com/altertable-ai/dlt-altertable/commit/0604265856a82c658a09c692cd1f43a8e76203d4))


### Performance Improvements

* drop the ignored transaction wrapper and gate schema lookups per load ([591988b](https://github.com/altertable-ai/dlt-altertable/commit/591988b53207a2f408c3d60c3de384cfb8b1f7b8))
* stream uploads in 1MiB chunks ([713e274](https://github.com/altertable-ai/dlt-altertable/commit/713e27495fee783d749557865eaad0e7b27ec2ff))


### Documentation

* claim only the system tables that actually load ([80b185f](https://github.com/altertable-ai/dlt-altertable/commit/80b185fb1c21165c989f802395d3405101340634))
* correct retry semantics and harden the bootstrap example ([6c495d5](https://github.com/altertable-ai/dlt-altertable/commit/6c495d5ab5dc95bf0d79dea109705487adcca482))
* correct the transport, dependency, and retry contract prose ([f3cdc6a](https://github.com/altertable-ai/dlt-altertable/commit/f3cdc6a8d8ee6b17ba19ddd011ef76e6bb6f4134))
* count urllib3 among the dependencies ([a276f8b](https://github.com/altertable-ai/dlt-altertable/commit/a276f8b5cdba9a3cf5454c0051d5c8361789a13e))
* describe file alignment from the destination's side ([d446208](https://github.com/altertable-ai/dlt-altertable/commit/d446208f50cabab1017ccb35adffa342c2bf0ec1))
* drop the sandboxed HubSpot example from the repo ([d7d6d70](https://github.com/altertable-ai/dlt-altertable/commit/d7d6d7000a74a503a1a008cb7db6e86a7fe3ec41))
* **examples:** replace the shaped demo with the sandboxed HubSpot task ([7a6d615](https://github.com/altertable-ai/dlt-altertable/commit/7a6d6150b18de2b73740f190569e203f993f54c9))
* explain alignment and transport choices from the destination's side ([5097a31](https://github.com/altertable-ai/dlt-altertable/commit/5097a31a72ebbd76ec005eb38f884133ebda06c5))
* explain file padding from dlt's mid-load schema evolution ([64257bf](https://github.com/altertable-ai/dlt-altertable/commit/64257bfe3c15b15e529334b1e3d2ce139d239d83))
* rewrite the README for the first release ([ec6c507](https://github.com/altertable-ai/dlt-altertable/commit/ec6c50775907b654bfe25ff89fafb4c761e8b89d))
* sharpen the chunked reader docstring ([31ae100](https://github.com/altertable-ai/dlt-altertable/commit/31ae1005636a4a4c1c0a1654f738c17d21929dcb))
* state the blocksize intent without the benchmark number ([6b6cd17](https://github.com/altertable-ai/dlt-altertable/commit/6b6cd179ad74d10ba826949c5a750a5de61f6451))


### Code Refactoring

* adopt dlt's exception vocabulary for config and type errors ([358ae4c](https://github.com/altertable-ai/dlt-altertable/commit/358ae4c2f87cbac51e4a435e87cf2b9c59a62f92))
* apply the naming audit and type the session adapter ([5afdd2b](https://github.com/altertable-ai/dlt-altertable/commit/5afdd2b21e444e2f90e842ea1c76443867fac3d9))
* build aligned files with generic capabilities directly ([5510332](https://github.com/altertable-ai/dlt-altertable/commit/5510332479385319f749bd31529ad65124501a62))
* declare every behavior-shaping decorator default explicitly ([ec2440c](https://github.com/altertable-ai/dlt-altertable/commit/ec2440cfeda9ab0ecb2955d5bc7f1448c2beeaac))
* describe the env fallback without tying it to sandboxes ([2587f32](https://github.com/altertable-ai/dlt-altertable/commit/2587f3281ae0d8b3cc9b4c732f13b2b8c8da57aa))
* inject the resolved configuration and reuse dlt arrow and escape helpers ([b28daea](https://github.com/altertable-ai/dlt-altertable/commit/b28daea2d5e5dc7dbf5e26e3cb739551ba6bb10d))
* inline the merge strategy hint and name the dedup sort translation ([e9b56ca](https://github.com/altertable-ai/dlt-altertable/commit/e9b56ca668b9d4be3f28e9d8c21ae462c8487706))
* inline the session setup ([a2b1744](https://github.com/altertable-ai/dlt-altertable/commit/a2b1744e7dc2b0d495d57970ba01cbe977f3a2a1))
* let TemporaryDirectory own the aligned copy and state the real reason for precreation ([2585a6f](https://github.com/altertable-ai/dlt-altertable/commit/2585a6faf7a979a66bcf9810bee75d0f3214403b))
* name the environment fallback map for what it does ([160376e](https://github.com/altertable-ai/dlt-altertable/commit/160376ed38c8ff85033e60d7ceb478e810b75850))
* name the upload block size for the write it buffers ([0adef91](https://github.com/altertable-ai/dlt-altertable/commit/0adef91a5c6e605be595305d813f8b32c1fa8084))
* own table creation instead of relying on server modes ([8730f86](https://github.com/altertable-ai/dlt-altertable/commit/8730f86ff4230efc92e1918c4245ad612f5b039f))
* split transport and schema concerns and tune uploads via urllib3 blocksize ([314f1fe](https://github.com/altertable-ai/dlt-altertable/commit/314f1feab6cddbeffdfe30e73ee36a2c0b6a216f))
