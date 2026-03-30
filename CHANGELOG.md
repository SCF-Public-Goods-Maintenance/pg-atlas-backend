# Changelog

## [0.3.0](https://github.com/SCF-Public-Goods-Maintenance/pg-atlas-backend/compare/v0.2.0...v0.3.0) (2026-03-30)


### Features

* **metrics:** graph builder, active subgraph, transitive criticality ([#11](https://github.com/SCF-Public-Goods-Maintenance/pg-atlas-backend/issues/11)) ([bf4c866](https://github.com/SCF-Public-Goods-Maintenance/pg-atlas-backend/commit/bf4c866444d5bae02d198acf9e4b28c166c84035))

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
