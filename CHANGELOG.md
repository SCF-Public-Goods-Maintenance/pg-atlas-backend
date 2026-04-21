# Changelog

## [0.6.0](https://github.com/SCF-Public-Goods-Maintenance/pg-atlas-backend/compare/v0.5.0...v0.6.0) (2026-04-21)


### Features

* **api:** add rate limiting and readiness health check ([#61](https://github.com/SCF-Public-Goods-Maintenance/pg-atlas-backend/issues/61)) ([34ff94f](https://github.com/SCF-Public-Goods-Maintenance/pg-atlas-backend/commit/34ff94fd862cd55f59b8a1153d92f7990e77476c))
* **API:** add server-side sorting, category filter, and round filter to Projects and Repos ([#49](https://github.com/SCF-Public-Goods-Maintenance/pg-atlas-backend/issues/49)) ([3d5a866](https://github.com/SCF-Public-Goods-Maintenance/pg-atlas-backend/commit/3d5a866719416e544fa078a933320c7f760397d3))
* **crawlers:** fetch and materialize download counts from package registries ([#62](https://github.com/SCF-Public-Goods-Maintenance/pg-atlas-backend/issues/62)) ([52a316f](https://github.com/SCF-Public-Goods-Maintenance/pg-atlas-backend/commit/52a316f27ecc1f0d22980c008767f3babd6b8048)), closes [#23](https://github.com/SCF-Public-Goods-Maintenance/pg-atlas-backend/issues/23)
* **gitlog:** schedule repo updates with a dormancy-based cadence ([#47](https://github.com/SCF-Public-Goods-Maintenance/pg-atlas-backend/issues/47)) ([3fd48d7](https://github.com/SCF-Public-Goods-Maintenance/pg-atlas-backend/commit/3fd48d7ba78c76dde7a15eaef2a7beecb4fd8dfa))
* **ingestion:** trigger criticality recompute from SBOM and bootstrap ([#56](https://github.com/SCF-Public-Goods-Maintenance/pg-atlas-backend/issues/56)) ([c2ee63a](https://github.com/SCF-Public-Goods-Maintenance/pg-atlas-backend/commit/c2ee63a7f6d5de658b077085d6228652b103f85e))
* **metrics:** materialize A9 repo/project criticality ([#50](https://github.com/SCF-Public-Goods-Maintenance/pg-atlas-backend/issues/50)) ([7ed7751](https://github.com/SCF-Public-Goods-Maintenance/pg-atlas-backend/commit/7ed77519cb674b4b4c284a1f21e1d8a249be1d0c))
* **metrics:** materialize pony factor from gitlog artifacts ([#57](https://github.com/SCF-Public-Goods-Maintenance/pg-atlas-backend/issues/57)) ([2df92b5](https://github.com/SCF-Public-Goods-Maintenance/pg-atlas-backend/commit/2df92b510685bca423369a4ff07bc6a64de8765d))
* **metrics:** materialize project adoption score ([#58](https://github.com/SCF-Public-Goods-Maintenance/pg-atlas-backend/issues/58)) ([9c437c4](https://github.com/SCF-Public-Goods-Maintenance/pg-atlas-backend/commit/9c437c4b09aaf66b993a3115530953e25e0c5603))


### Bug Fixes

* **gitlog:** add Filebase S3 credentials to worker step ([09f587a](https://github.com/SCF-Public-Goods-Maintenance/pg-atlas-backend/commit/09f587aef8bb16cc251c1ed55e760b61bf232e52))
* **procrastinate:** patch logging handlers in tee helper ([e36fe2e](https://github.com/SCF-Public-Goods-Maintenance/pg-atlas-backend/commit/e36fe2e408e7c84692b8506178e8e4b44ba285f5))
* **workers:** always materialize and summarize ([97fbd44](https://github.com/SCF-Public-Goods-Maintenance/pg-atlas-backend/commit/97fbd4488b08ac547d594879a6ebabc807941ec5))


### Performance Improvements

* **metrics:** do bulk updates, no ORM UoW, and keep data columnar ([e25aa7a](https://github.com/SCF-Public-Goods-Maintenance/pg-atlas-backend/commit/e25aa7a2c8392291ad60cc9072629b9e572b013e))
* **metrics:** reduce materialize criticality and pony row locks and WAL churn ([58052d7](https://github.com/SCF-Public-Goods-Maintenance/pg-atlas-backend/commit/58052d7beab864da39ce5f9bb54c8c8388f43037))

## [0.5.0](https://github.com/SCF-Public-Goods-Maintenance/pg-atlas-backend/compare/v0.4.1...v0.5.0) (2026-04-12)


### Features

* **API:** add active contributors 30d + 90d to project and repo detail ([ec935de](https://github.com/SCF-Public-Goods-Maintenance/pg-atlas-backend/commit/ec935de3771f04c7e90038b7223665572ceb4ee1))
* **API:** add active repos 90d metric to metadata ([45938bc](https://github.com/SCF-Public-Goods-Maintenance/pg-atlas-backend/commit/45938bc4d6b72fa22ec15d6f11f939e8cab343dd))
* **gitlog:** integrate with procrastinate ([a6b1955](https://github.com/SCF-Public-Goods-Maintenance/pg-atlas-backend/commit/a6b1955a4b0bbfacf63d3d3e40d15b9dff8339d1))
* **gitlog:** store raw output as .gitlog artifact ([3756cbc](https://github.com/SCF-Public-Goods-Maintenance/pg-atlas-backend/commit/3756cbc009cba51faa548fe92a31d2a359661272))
* **ingestion:** queue SBOM post-validation processing ([8da2177](https://github.com/SCF-Public-Goods-Maintenance/pg-atlas-backend/commit/8da2177138ee6d0591fc67baad822422b7baf673))
* restrict CORS to GET method with allow-all origins ([#29](https://github.com/SCF-Public-Goods-Maintenance/pg-atlas-backend/issues/29)) ([0c68236](https://github.com/SCF-Public-Goods-Maintenance/pg-atlas-backend/commit/0c68236fd680fa19bf9a99f3e81f61bb55b6dcbd))
* **SBOM:** process queue in dedicated workflow ([#32](https://github.com/SCF-Public-Goods-Maintenance/pg-atlas-backend/issues/32)) ([9330dc0](https://github.com/SCF-Public-Goods-Maintenance/pg-atlas-backend/commit/9330dc037f1c47b61bceef157a0e8e9546e9451b))
* **SBOM:** reprocess targeted failed submissions ([#35](https://github.com/SCF-Public-Goods-Maintenance/pg-atlas-backend/issues/35)) ([1b67003](https://github.com/SCF-Public-Goods-Maintenance/pg-atlas-backend/commit/1b67003f6cbc882df72af28875b41de25ab7aae5))


### Bug Fixes

* **persistence:** untangle IPFS reads from Filebase S3 writes ([#39](https://github.com/SCF-Public-Goods-Maintenance/pg-atlas-backend/issues/39)) ([4a0da53](https://github.com/SCF-Public-Goods-Maintenance/pg-atlas-backend/commit/4a0da535d3a7692a729823df0e7fa17f6c891ed8))
* **SBOM:** semantically deduplicate SBOM artifacts before persistence ([#34](https://github.com/SCF-Public-Goods-Maintenance/pg-atlas-backend/issues/34)) ([16ca3cb](https://github.com/SCF-Public-Goods-Maintenance/pg-atlas-backend/commit/16ca3cb945dbdf96597c01c1a89a4609fee7dd3e))
* **storage:** persist SBOM artifacts in Filebase ([eefb4d3](https://github.com/SCF-Public-Goods-Maintenance/pg-atlas-backend/commit/eefb4d32519e5db5c36eb07cdd0c990d38cb5960))
* **storage:** put types_aiobotocore_s3 import behind TYPE_CHECKING guard ([#33](https://github.com/SCF-Public-Goods-Maintenance/pg-atlas-backend/issues/33)) ([8359952](https://github.com/SCF-Public-Goods-Maintenance/pg-atlas-backend/commit/835995273acce78353cdbf9e210f77012870dd8f))


### Performance Improvements

* explore treeless clone with auth [WIP] ([4ba2af1](https://github.com/SCF-Public-Goods-Maintenance/pg-atlas-backend/commit/4ba2af148560f450280978810ace5833119b9e1f))


### Dependencies

* upgrade to procrastinate&gt;=3.8.1 ([812fb91](https://github.com/SCF-Public-Goods-Maintenance/pg-atlas-backend/commit/812fb9175bb58e82e0c8fe6af0cf30170c7a187b))

## [0.4.1](https://github.com/SCF-Public-Goods-Maintenance/pg-atlas-backend/compare/v0.4.0...v0.4.1) (2026-04-05)


### Documentation

* **SDK:** change references to repo and package name ([bb98d33](https://github.com/SCF-Public-Goods-Maintenance/pg-atlas-backend/commit/bb98d3344fb101ac59886894c0e3c0eda58eecba))

## [0.4.0](https://github.com/SCF-Public-Goods-Maintenance/pg-atlas-backend/compare/v0.3.0...v0.4.0) (2026-04-04)


### Features

* **API:** add `project_id` field to `ProjectDetailResponse` ([9021637](https://github.com/SCF-Public-Goods-Maintenance/pg-atlas-backend/commit/90216370a816eebb999688c44d1b2458a57d01b4))
* **API:** create all API endpoints needed to unblock SDK and frontend development ([#22](https://github.com/SCF-Public-Goods-Maintenance/pg-atlas-backend/issues/22)) ([83df2ac](https://github.com/SCF-Public-Goods-Maintenance/pg-atlas-backend/commit/83df2acbc9048eb5ca917ae882fb7c9a65fb1f29))


### Documentation

* **API:** add contribute-data section and generate prettier route IDs ([24e7456](https://github.com/SCF-Public-Goods-Maintenance/pg-atlas-backend/commit/24e74569c0c67b8a92a679c8d9b67bfe5bec34b2))
* **migrations:** document single-db local workflow ([#21](https://github.com/SCF-Public-Goods-Maintenance/pg-atlas-backend/issues/21)) ([ba61dfa](https://github.com/SCF-Public-Goods-Maintenance/pg-atlas-backend/commit/ba61dfa85a8d95c12b5b9de990150c0a174f04bb))
* provide release instructions ([b7660e8](https://github.com/SCF-Public-Goods-Maintenance/pg-atlas-backend/commit/b7660e82e355f041c2d67d9c9f0541d5f6666230))

## [0.3.0](https://github.com/SCF-Public-Goods-Maintenance/pg-atlas-backend/compare/v0.2.0...v0.3.0) (2026-04-02)


### Features

* **metrics:** graph builder, active subgraph, transitive criticality ([#11](https://github.com/SCF-Public-Goods-Maintenance/pg-atlas-backend/issues/11)) ([bf4c866](https://github.com/SCF-Public-Goods-Maintenance/pg-atlas-backend/commit/bf4c866444d5bae02d198acf9e4b28c166c84035))
* **metrics:** implement exact A6 active subgraph projection ([a46d953](https://github.com/SCF-Public-Goods-Maintenance/pg-atlas-backend/commit/a46d953be293de24bdf7829a0c8a55d270417895))
* **metrics:** implement exact A6 active subgraph projection ([ca9c6e8](https://github.com/SCF-Public-Goods-Maintenance/pg-atlas-backend/commit/ca9c6e8ea914b1591a0bb5fb8771ae41a70c143f))


### Bug Fixes

* **QA:** migrate to OpenGrants schema v1.0.11 and harden the bootstrap pipeline ([#20](https://github.com/SCF-Public-Goods-Maintenance/pg-atlas-backend/issues/20)) ([056a55e](https://github.com/SCF-Public-Goods-Maintenance/pg-atlas-backend/commit/056a55e1f9ff5dcfe181d10778b319d479ad4559))


### Performance Improvements

* **metrics:** use single-pass traversal for A6 projection ([8879b39](https://github.com/SCF-Public-Goods-Maintenance/pg-atlas-backend/commit/8879b390a4b443dff16366999d85470c5197f87a))

## [0.2.0](https://github.com/SCF-Public-Goods-Maintenance/pg-atlas-backend/compare/v0.1.0...v0.2.0) (2026-03-17)


### Features

* add git log parser and contributor statistics ([0294005](https://github.com/SCF-Public-Goods-Maintenance/pg-atlas-backend/commit/0294005616ba4dc0d4c0f3fbe6c222bd215aa7f9))
* add pub.dev and Packagist registry crawlers ([98a53f0](https://github.com/SCF-Public-Goods-Maintenance/pg-atlas-backend/commit/98a53f0fc4dd16b89ece21cd49cc53b3b3a20ace))
* add pub.dev and Packagist registry crawlers ([2a96bf3](https://github.com/SCF-Public-Goods-Maintenance/pg-atlas-backend/commit/2a96bf35ec8491045401c1cc426d801f417d1682))
* **alembic:** switch to multiple bases with procrastinate init ([84e1db8](https://github.com/SCF-Public-Goods-Maintenance/pg-atlas-backend/commit/84e1db8b5746c40b5380211c2b1b39ac6beb7430))
* **db:** apply new revisions during container startup ([bae1000](https://github.com/SCF-Public-Goods-Maintenance/pg-atlas-backend/commit/bae1000a5b1267f15504315a2be8e02da4ca3af2))
* **db:** create the initial db schema ([88514f9](https://github.com/SCF-Public-Goods-Maintenance/pg-atlas-backend/commit/88514f930bd9200737ca19df1ac8c02e44f72e89))
* **db:** implement the two-level data model in sqlalchemy ([0f0b751](https://github.com/SCF-Public-Goods-Maintenance/pg-atlas-backend/commit/0f0b7511ff67467c3e50580ad832cf9c2a7e47e0))
* **db:** persist submitted sboms ([c7f618d](https://github.com/SCF-Public-Goods-Maintenance/pg-atlas-backend/commit/c7f618d16e8b23b908ebb733386cc2e794c311a2))
* **deps.dev:** generate gRPC client from proto ([a0e5673](https://github.com/SCF-Public-Goods-Maintenance/pg-atlas-backend/commit/a0e5673fdca61c7b86e897aa8c47546b7baa82cf))
* **gitlog:** add git log parser with bot-aware contributor stats ([66bedb3](https://github.com/SCF-Public-Goods-Maintenance/pg-atlas-backend/commit/66bedb36fd1213a721c59019a9ff5d5ace851c90))
* **procrastinate:** recover stale queue state ([f1f5182](https://github.com/SCF-Public-Goods-Maintenance/pg-atlas-backend/commit/f1f51821bda30c6912f6e821f1807f37c7a1928a))
* **procrastinate:** setup and define bootstrap tasks and workers ([f330ea5](https://github.com/SCF-Public-Goods-Maintenance/pg-atlas-backend/commit/f330ea52b1a1e5b322eac839b1b76ac3d9677636))
* **procrastinate:** worker environment for the bootstrap tasks ([70123eb](https://github.com/SCF-Public-Goods-Maintenance/pg-atlas-backend/commit/70123eb03d72eff2ac5ac0c5fbeb5ad91cd9ac95))
* **SBOM:** add list and detail endpoints ([4b0d824](https://github.com/SCF-Public-Goods-Maintenance/pg-atlas-backend/commit/4b0d82457462274106126f004af046357c18ee69))
* **SBOM:** store raw submissions in filesystem for local dev ([9877a41](https://github.com/SCF-Public-Goods-Maintenance/pg-atlas-backend/commit/9877a41176a029e62a3013e641abd257b84e622f))


### Bug Fixes

* **ci:** align pre-commit ruff to v0.15.2; accept PEP 758 bracketless except ([99069bf](https://github.com/SCF-Public-Goods-Maintenance/pg-atlas-backend/commit/99069bfe4c0a9e5175816c30f3cc754e9f018cdf))
* **config:** strip any query params for sqlalchemy ([ac3bf11](https://github.com/SCF-Public-Goods-Maintenance/pg-atlas-backend/commit/ac3bf1130a95b48317c90ecdd18c1d0ec893be4c))
* **db:** deduplicate multiple pinned versions ([968f7bc](https://github.com/SCF-Public-Goods-Maintenance/pg-atlas-backend/commit/968f7bc580ccf53bc1a72c43454b0ffa6fe68174))
* **db:** remove the reserved pg_ prefix ([244a309](https://github.com/SCF-Public-Goods-Maintenance/pg-atlas-backend/commit/244a309880e8eeb04b85826e669e665dc37f6056))
* **deps.dev:** regenerate a native async client ([de8192c](https://github.com/SCF-Public-Goods-Maintenance/pg-atlas-backend/commit/de8192c45cc1ee80643acbe49b785d8b666dadb5))
* **deps.dev:** replace asyncio wrappers with per-call stub + channel ([67133ea](https://github.com/SCF-Public-Goods-Maintenance/pg-atlas-backend/commit/67133eaab308de84ea68d2067851df2be9a09b1f))
* **gitlog:** filter remaining bots and minor/style improvements ([4a9268b](https://github.com/SCF-Public-Goods-Maintenance/pg-atlas-backend/commit/4a9268bde151e1fb3ae6363ee36cfa9347165d98))
* print details for OIDC audience mismatch ([cf135f0](https://github.com/SCF-Public-Goods-Maintenance/pg-atlas-backend/commit/cf135f04a4cf0452b83cd633de02daac7b7483de))
* **procrastinate:** improve handoff from crawl_github_repo to crawl_package_deps: ([a3cfe84](https://github.com/SCF-Public-Goods-Maintenance/pg-atlas-backend/commit/a3cfe84d5ca810b53a6a08a36ee47b30c07b1e32))
* **test:** cannot assume env db url format; pubdev now gives download count ([76b91f9](https://github.com/SCF-Public-Goods-Maintenance/pg-atlas-backend/commit/76b91f9644f9ddad93804a3c7e951794baeeeff5))
* unwrap github sbom envelope ([626a118](https://github.com/SCF-Public-Goods-Maintenance/pg-atlas-backend/commit/626a118395398b282c178a282c5e2a40188c1b42))
* use last-30-day downloads for adoption_downloads per spec ([bdf98c1](https://github.com/SCF-Public-Goods-Maintenance/pg-atlas-backend/commit/bdf98c1d6280691aee3ce42bb151020cbb65f728))


### Dependencies

* **CI:** update used actions ([9934c42](https://github.com/SCF-Public-Goods-Maintenance/pg-atlas-backend/commit/9934c4216d1b812c9a48fc1b9572f90272df5614))
* **procrastinate:** add dependencies for the bootstrap pipeline ([bd57d3a](https://github.com/SCF-Public-Goods-Maintenance/pg-atlas-backend/commit/bd57d3a76339cd6f7eec79a8e6bbd0e31a26b441))


### Documentation

* show pre-commit.ci status ([7fdd54f](https://github.com/SCF-Public-Goods-Maintenance/pg-atlas-backend/commit/7fdd54f7f402e6984deef892f813beab5442300a))
* update instructions ([de9620f](https://github.com/SCF-Public-Goods-Maintenance/pg-atlas-backend/commit/de9620f8350ed2edc0c7b80085b80c9eb787132d))

## 0.1.0 (2026-02-25)


### Features

* add basic cli arguments to the module ([63088f2](https://github.com/SCF-Public-Goods-Maintenance/pg-atlas-backend/commit/63088f24535b3c8e1258901157382e8d9a45b745))
* initialize alembic ([21b050a](https://github.com/SCF-Public-Goods-Maintenance/pg-atlas-backend/commit/21b050a73101595f072592daca9b9653b28c4ea5))
* scaffold docker deployment ([0911c58](https://github.com/SCF-Public-Goods-Maintenance/pg-atlas-backend/commit/0911c58e8293e362d7ed0e161244fed9d0ce98ae))


### Bug Fixes

* avoid catching base exception ([8875acf](https://github.com/SCF-Public-Goods-Maintenance/pg-atlas-backend/commit/8875acf64ddd17bc683fefc8772c8a141db0e32e))
* dynamically load app version from metadata ([53840c8](https://github.com/SCF-Public-Goods-Maintenance/pg-atlas-backend/commit/53840c8b38d901d3d54e4b8ae1c2109f082d6418))
* fail early when no db connection can be made ([94b7868](https://github.com/SCF-Public-Goods-Maintenance/pg-atlas-backend/commit/94b786821af1f4875fa9496169e2cdef8d5a5797))


### Dependencies

* upgrade all version constraints ([44c73f7](https://github.com/SCF-Public-Goods-Maintenance/pg-atlas-backend/commit/44c73f7fd52c6668ca17cb113b3c9b25c22c5315))


### Documentation

* add pre-commit install to setup ([2540862](https://github.com/SCF-Public-Goods-Maintenance/pg-atlas-backend/commit/2540862958b60a370944d1df6c2125b953f219df))
* add project-specific agent instructions ([7affac1](https://github.com/SCF-Public-Goods-Maintenance/pg-atlas-backend/commit/7affac17248f52da50b196863aaa22ceb4480dd8))
